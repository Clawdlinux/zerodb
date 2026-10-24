use crate::dedup::{DedupKey, Entry, Scope};
use crate::error::{lock, Result};
use crate::facts::{FactKey, Rev, Revs};
use crate::session::{Backing, Key};
use crate::state::State;
use crate::turn::{bytes, Cursor};
use crate::{EngineError, EngineOptions, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use zerodb_tape::crc32c as crc;
use zerodb_tape::{Committed, FileId, Io};

fn corrupt() -> EngineError {
    EngineError::rejected(ErrorKind::CorruptIndex)
}

#[derive(Clone, Debug)]
pub(crate) struct CheckpointFile {
    pub(crate) name: String,
    pub(crate) len: u64,
    pub(crate) digest: [u8; 32],
}

struct File<'a>(&'a dyn Io, FileId);
impl Drop for File<'_> {
    fn drop(&mut self) {
        let _ = self.0.close(self.1);
    }
}

struct PendingFile<'a> {
    io: &'a dyn Io,
    path: PathBuf,
    preserve: bool,
}
impl Drop for PendingFile<'_> {
    fn drop(&mut self) {
        if self.preserve {
            return;
        }
        let dir = self.path.parent().unwrap_or_else(|| Path::new("."));
        for path in [&self.path, &dir.join("CHECKPOINT.next")] {
            if let Err(error) = self.io.remove(path) {
                if !matches!(error, zerodb_tape::Error::Io(std::io::ErrorKind::NotFound, _)) {
                    eprintln!("zerodb: checkpoint cleanup failed: {error}");
                }
            }
        }
        if let Err(error) = self.io.sync_dir(dir) {
            eprintln!("zerodb: checkpoint cleanup sync failed: {error}");
        }
    }
}

fn exact(io: &dyn Io, id: FileId, mut off: u64, mut out: &mut [u8]) -> Result<()> {
    while !out.is_empty() {
        let n = io.read_at(id, off, out)?;
        if n == 0 {
            return Err(corrupt());
        }
        off += n as u64;
        out = &mut out[n..];
    }
    Ok(())
}
fn write(io: &dyn Io, id: FileId, off: &mut u64, payload: &[u8]) -> Result<(u64, u32)> {
    let mut record = Vec::new();
    let (_, len) = buffer_record(&mut record, payload);
    let location = (*off, len);
    io.write_at(id, *off, &record)?;
    *off += u64::from(len);
    Ok(location)
}
/// Appends `crc | len | payload` without a temporary buffer.
fn buffer_record(out: &mut Vec<u8>, payload: &[u8]) -> (u64, u32) {
    let (start, len) = (out.len(), payload.len() + 8);
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&(len as u32).to_le_bytes());
    out.extend_from_slice(payload);
    let sum = crc(&out[start + 4..]);
    out[start..start + 4].copy_from_slice(&sum.to_le_bytes());
    (start as u64, len as u32)
}
fn read(io: &dyn Io, id: FileId, off: u64) -> Result<(Vec<u8>, u32)> {
    read_hashed(io, id, off, None)
}
fn read_hashed(
    io: &dyn Io,
    id: FileId,
    off: u64,
    hash: Option<&mut blake3::Hasher>,
) -> Result<(Vec<u8>, u32)> {
    let mut header = [0; 8];
    exact(io, id, off, &mut header)?;
    let len = u32::from_le_bytes(header[4..].try_into().map_err(|_| corrupt())?);
    if !(9..=131072).contains(&len) {
        return Err(corrupt());
    }
    let mut data = vec![0; len as usize];
    data[..8].copy_from_slice(&header);
    exact(io, id, off + 8, &mut data[8..])?;
    if crc(&data[4..]) != u32::from_le_bytes(header[..4].try_into().map_err(|_| corrupt())?) {
        return Err(corrupt());
    }
    if let Some(hash) = hash {
        hash.update(&data);
    }
    Ok((data[8..].to_vec(), len))
}
fn put_key(out: &mut Vec<u8>, key: &Key) {
    out.extend_from_slice(&key.0.to_le_bytes());
    bytes(out, key.1.as_bytes());
    bytes(out, key.2.as_bytes());
}
fn get_key(cur: &mut Cursor<'_>) -> Result<Key> {
    Ok(Key(cur.u32()?, cur.string(255)?, cur.string(255)?))
}
fn page(data: &[u8]) -> Result<(Key, usize, Vec<u64>)> {
    let mut cur = Cursor(data);
    if cur.byte()? != 2 {
        return Err(corrupt());
    }
    let key = get_key(&mut cur)?;
    let page = cur.u32()? as usize;
    let count = cur.u32()? as usize;
    if count == 0 || count > crate::session::capacity(page) {
        return Err(corrupt());
    }
    let slots = (0..count).map(|_| cur.u64()).collect::<std::result::Result<Vec<_>, _>>()?;
    cur.end()?;
    Ok((key, page, slots))
}
pub(crate) fn read_page(io: &dyn Io, dir: &Path, backing: &Backing) -> Result<Vec<u64>> {
    let file = File(io, io.open(&dir.join(&backing.file))?);
    let (data, len) = read(io, file.1, backing.offset)?;
    if len != backing.len {
        return Err(corrupt());
    }
    Ok(page(&data).map_err(|_| corrupt())?.2)
}

fn load_inner(io: &dyn Io, dir: &Path, opts: &EngineOptions) -> Result<State> {
    let mut state = State::new(opts);
    if !io.exists(&dir.join("CHECKPOINT"))? {
        return Ok(state);
    }
    let file = File(io, io.open(&dir.join("CHECKPOINT"))?);
    let (data, len) = read(io, file.1, 0)?;
    if io.len(file.1)? != u64::from(len) {
        return Err(corrupt());
    }
    let mut cur = Cursor(&data);
    if cur.take(4)? != b"ZCM2" {
        return Err(corrupt());
    }
    let watermark = cur.u64()?;
    let txn = cur.u64()?;
    let count = cur.byte()?;
    if count == 0 || count > 8 {
        return Err(corrupt());
    }
    for _ in 0..count {
        let name = cur.string(128)?;
        if !name.starts_with("ckpt-")
            || !name.ends_with(".zdb")
            || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
        {
            return Err(corrupt());
        }
        let len = cur.u64()?;
        let digest = cur.take(32)?.try_into().map_err(|_| corrupt())?;
        state.chain.push(CheckpointFile { name, len, digest });
    }
    cur.end()?;
    let mut previous = 0;
    for sealed in state.chain.clone() {
        let name = sealed.name;
        let file = File(io, io.open(&dir.join(&name))?);
        let file_len = io.len(file.1)?;
        if file_len != sealed.len {
            return Err(corrupt());
        }
        let mut hash = blake3::Hasher::new();
        let (header, header_len) = read_hashed(io, file.1, 0, Some(&mut hash))?;
        let mut off = u64::from(header_len);
        let mut cur = Cursor(&header);
        if cur.take(4)? != b"ZCK2" {
            return Err(corrupt());
        }
        let boundary = cur.u64()?;
        let boundary_txn = cur.u64()?;
        cur.end()?;
        if boundary <= previous || boundary > watermark || boundary_txn > txn {
            return Err(corrupt());
        }
        previous = boundary;
        while off < file_len {
            let (data, len) = read_hashed(io, file.1, off, Some(&mut hash))?;
            let mut cur = Cursor(&data);
            match cur.byte()? {
                1 => {
                    let id = cur.u32()?;
                    let lsn = cur.u64()?;
                    let name = cur.string(255)?;
                    cur.end()?;
                    if lsn == 0 || lsn > boundary {
                        return Err(corrupt());
                    }
                    state.tenants.insert(&name, id, lsn, opts.max_tenants)?;
                }
                2 => {
                    let (key, index, slots) = page(&data)?;
                    if key.0 == 0
                        || key.0 as usize > state.tenants.entries.len()
                        || slots.iter().any(|&lsn| lsn == 0 || lsn > boundary)
                        || slots.windows(2).any(|w| w[0] >= w[1])
                    {
                        return Err(corrupt());
                    }
                    let revision = *slots.last().ok_or_else(corrupt)?;
                    state.index.restore(
                        key,
                        index,
                        slots.len(),
                        revision,
                        Backing { file: name.clone(), offset: off, len },
                    )?;
                }
                tag @ (3 | 5) => {
                    let tenant = cur.u32()?;
                    let scope = cur.string(if tag == 3 { 255 } else { 1024 })?;
                    let key = cur.string(128)?;
                    let digest = cur.take(32)?.try_into().map_err(|_| corrupt())?;
                    let txn = cur.u64()?;
                    let first_lsn = cur.u64()?;
                    let last_lsn = cur.u64()?;
                    let reply = if tag == 3 { cur.take(8)?.to_vec() } else { Vec::new() };
                    cur.end()?;
                    if first_lsn == 0
                        || first_lsn > last_lsn
                        || last_lsn > boundary
                        || tenant == 0
                        || tenant as usize > state.tenants.entries.len()
                    {
                        return Err(corrupt());
                    }
                    let scope = if tag == 3 { Scope::Session(scope) } else { Scope::Fact(scope) };
                    state.dedup.insert(Entry {
                        key: DedupKey(tenant, scope, key),
                        digest,
                        committed: Committed::Durable { txn, first_lsn, last_lsn, reply },
                    });
                }
                4 => {
                    let tenant = cur.u32()?;
                    let key = cur.string(1024)?;
                    let history = cur.u32()? as usize;
                    let dropped = cur.byte()?;
                    let revs = (0..cur.u32()?)
                        .map(|_| {
                            let lsn = cur.u64()?;
                            match cur.byte()? {
                                deleted @ (0 | 1) => Ok(Rev { lsn, deleted: deleted == 1 }),
                                _ => Err(corrupt()),
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    cur.end()?;
                    // Written under a smaller fact_history, the entry may lack revisions replay keeps.
                    if history < opts.fact_history
                        || dropped > 1
                        || tenant == 0
                        || tenant as usize > state.tenants.entries.len()
                        || revs.is_empty()
                        || revs.iter().any(|rev| rev.lsn == 0 || rev.lsn > boundary)
                        || revs.windows(2).any(|w| w[0].lsn >= w[1].lsn)
                    {
                        return Err(corrupt());
                    }
                    state.facts.restore((tenant, key), revs, dropped == 1)?;
                }
                _ => return Err(corrupt()),
            }
            off = off.checked_add(u64::from(len)).ok_or_else(corrupt)?;
        }
        if off != file_len || hash.finalize().as_bytes() != &sealed.digest {
            return Err(corrupt());
        }
    }
    if previous != watermark {
        return Err(corrupt());
    }
    state.watermark = watermark;
    state.published_lsn = watermark;
    state.txn = txn;
    Ok(state)
}
pub(crate) fn load(io: &dyn Io, dir: &Path, opts: &EngineOptions) -> State {
    match load_inner(io, dir, opts) {
        Ok(state) => state,
        Err(error) => {
            eprintln!("zerodb: checkpoint ignored, replaying tape: {error}");
            let mut state = State::new(opts);
            state.warnings = 1;
            state
        }
    }
}

pub(crate) fn sweep(io: &dyn Io, dir: &Path, chain: &[CheckpointFile]) -> Result<()> {
    for path in io.list(dir)? {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if (name.starts_with("ckpt-")
            && name.ends_with(".zdb")
            && !chain.iter().any(|live| live.name == name))
            || name == "CHECKPOINT.next"
            || (name == "CHECKPOINT" && chain.is_empty())
        {
            io.remove(&path)?;
        }
    }
    io.sync_dir(dir)?;
    Ok(())
}

type PageSnapshot = (Key, usize, u64, Option<Arc<Vec<u64>>>, Option<Backing>);
struct Snapshot {
    lsn: u64,
    txn: u64,
    bytes: u64,
    full: bool,
    chain: Vec<CheckpointFile>,
    tenants: Vec<(u32, String, u64)>,
    pages: Vec<PageSnapshot>,
    dedup: Vec<Arc<Entry>>,
    facts: Vec<(FactKey, Vec<Rev>, bool)>,
    fact_history: usize,
}
fn snapshot(state: &State) -> Snapshot {
    let full = state.chain.len() >= 8;
    let mut pages = Vec::new();
    if full {
        for (key, entry) in &state.index.sessions {
            for (index, page) in entry.pages.iter().enumerate() {
                if page.revision > 0 {
                    pages.push((
                        key.clone(),
                        index,
                        page.revision,
                        page.slots.clone(),
                        page.backing.clone(),
                    ));
                }
            }
        }
        pages.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    } else {
        for (key, index) in &state.dirty {
            let page = &state.index.sessions[key].pages[*index];
            if page.revision > 0 {
                pages.push((
                    key.clone(),
                    *index,
                    page.revision,
                    page.slots.clone(),
                    page.backing.clone(),
                ));
            }
        }
    }
    // Many dirty keys: walking the map in order is cheaper than a lookup per key.
    let all = full || state.facts.dirty.len() * 2 >= state.facts.map.len();
    let facts: Box<dyn Iterator<Item = (&FactKey, &Revs)>> = if all {
        Box::new(state.facts.map.iter())
    } else {
        Box::new(state.facts.dirty.iter().filter_map(|key| state.facts.map.get_key_value(key)))
    };
    Snapshot {
        lsn: state.published_lsn,
        txn: state.txn,
        bytes: state.bytes,
        full,
        chain: state.chain.clone(),
        tenants: state
            .tenants
            .entries
            .iter()
            .enumerate()
            .filter(|(_, (_, lsn))| full || *lsn > state.watermark)
            .map(|(i, (name, lsn))| (i as u32 + 1, name.clone(), *lsn))
            .collect(),
        pages,
        dedup: state
            .dedup
            .entries
            .values()
            .filter(|e| full || e.age() > state.watermark)
            .cloned()
            .collect(),
        facts: facts
            .map(|(key, revs)| {
                let cut = revs.list.len().saturating_sub(state.facts.history + 1);
                (key.clone(), revs.list[cut..].to_vec(), revs.dropped || cut > 0)
            })
            .collect(),
        fact_history: state.facts.history,
    }
}
const BUFFER_LIMIT: usize = 4 * 1024 * 1024;
struct CheckpointBuffer<'a> {
    io: &'a dyn Io,
    file: FileId,
    offset: u64,
    bytes: Vec<u8>,
    peak: usize,
    hash: blake3::Hasher,
}
impl<'a> CheckpointBuffer<'a> {
    fn new(io: &'a dyn Io, file: FileId) -> Self {
        Self {
            io,
            file,
            offset: 0,
            bytes: Vec::with_capacity(BUFFER_LIMIT),
            peak: 0,
            hash: blake3::Hasher::new(),
        }
    }
    fn record(&mut self, payload: &[u8]) -> Result<(u64, u32)> {
        let len = payload.len().checked_add(8).ok_or_else(corrupt)?;
        if len > BUFFER_LIMIT {
            return Err(corrupt());
        }
        if self.bytes.len() + len > BUFFER_LIMIT {
            self.flush()?;
        }
        let (offset, len) = buffer_record(&mut self.bytes, payload);
        self.peak = self.peak.max(self.bytes.len());
        debug_assert!(self.bytes.capacity() <= BUFFER_LIMIT);
        Ok((self.offset + offset, len))
    }
    fn flush(&mut self) -> Result<()> {
        if !self.bytes.is_empty() {
            self.io.write_at(self.file, self.offset, &self.bytes)?;
            self.hash.update(&self.bytes);
            self.offset += self.bytes.len() as u64;
            self.bytes.clear();
        }
        Ok(())
    }
}

fn next_name(shared: &Mutex<State>, lsn: u64) -> Result<String> {
    let mut state = lock(shared)?;
    let attempt = state.checkpoint_attempt;
    state.checkpoint_attempt = attempt.checked_add(1).ok_or_else(|| {
        EngineError::rejected(ErrorKind::ResourceExhausted("checkpoint_attempts"))
    })?;
    Ok(if attempt == 0 { format!("ckpt-{lsn}.zdb") } else { format!("ckpt-{lsn}-{attempt}.zdb") })
}

pub(crate) fn save(io: &dyn Io, dir: &Path, shared: &Mutex<State>) -> Result<u64> {
    let snap = {
        let mut state = lock(shared)?;
        if let Some(error) = &state.failure {
            return Err(error.clone());
        }
        if state.published_lsn == state.watermark {
            return Ok(state.watermark);
        }
        let start = std::time::Instant::now();
        let snap = snapshot(&state);
        state.snapshot_lock_ns =
            state.snapshot_lock_ns.max(start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        state.checkpoint_lock_ns = state.checkpoint_lock_ns.max(state.snapshot_lock_ns);
        snap
    };
    let mut name = next_name(shared, snap.lsn)?;
    while io.exists(&dir.join(&name))? {
        name = next_name(shared, snap.lsn)?;
    }
    let mut pending = PendingFile { io, path: dir.join(&name), preserve: false };
    let file = File(io, io.create(&pending.path)?);
    let mut buffer = CheckpointBuffer::new(io, file.1);
    let mut header = b"ZCK2".to_vec();
    header.extend_from_slice(&snap.lsn.to_le_bytes());
    header.extend_from_slice(&snap.txn.to_le_bytes());
    buffer.record(&header)?;
    for (id, name, lsn) in &snap.tenants {
        let mut data = vec![1];
        data.extend_from_slice(&id.to_le_bytes());
        data.extend_from_slice(&lsn.to_le_bytes());
        bytes(&mut data, name.as_bytes());
        buffer.record(&data)?;
    }
    let mut locations = Vec::new();
    for (key, index, revision, slots, backing) in snap.pages {
        let slots = match slots {
            Some(slots) => slots,
            None => Arc::new(read_page(io, dir, &backing.ok_or_else(corrupt)?)?),
        };
        let mut data = vec![2];
        put_key(&mut data, &key);
        data.extend_from_slice(&(index as u32).to_le_bytes());
        data.extend_from_slice(&(slots.len() as u32).to_le_bytes());
        for slot in slots.iter() {
            data.extend_from_slice(&slot.to_le_bytes());
        }
        let (offset, len) = buffer.record(&data)?;
        locations.push((key, index, revision, Backing { file: name.clone(), offset, len }));
    }
    for entry in snap.dedup {
        let (tag, scope) = match &entry.key.1 {
            Scope::Session(session) => (3, session),
            Scope::Fact(key) => (5, key),
        };
        let mut data = vec![tag];
        data.extend_from_slice(&entry.key.0.to_le_bytes());
        bytes(&mut data, scope.as_bytes());
        bytes(&mut data, entry.key.2.as_bytes());
        data.extend_from_slice(&entry.digest);
        let Committed::Durable { txn, first_lsn, last_lsn, ref reply } = entry.committed;
        for value in [txn, first_lsn, last_lsn] {
            data.extend_from_slice(&value.to_le_bytes());
        }
        // Fact replies are empty.
        data.extend_from_slice(reply);
        buffer.record(&data)?;
    }
    for ((tenant, key), revs, dropped) in &snap.facts {
        let mut data = vec![4];
        data.extend_from_slice(&tenant.to_le_bytes());
        bytes(&mut data, key.as_bytes());
        data.extend_from_slice(&(snap.fact_history as u32).to_le_bytes());
        data.push(u8::from(*dropped));
        data.extend_from_slice(&(revs.len() as u32).to_le_bytes());
        for rev in revs {
            data.extend_from_slice(&rev.lsn.to_le_bytes());
            data.push(u8::from(rev.deleted));
        }
        buffer.record(&data)?;
    }
    buffer.flush()?;
    io.sync_data(file.1)?;
    // A crash can lose this derived filename. Open falls back to tape replay if it is missing.
    let mut chain = if snap.full { Vec::new() } else { snap.chain.clone() };
    chain.push(CheckpointFile {
        name,
        len: buffer.offset,
        digest: *buffer.hash.finalize().as_bytes(),
    });
    let mut manifest = b"ZCM2".to_vec();
    manifest.extend_from_slice(&snap.lsn.to_le_bytes());
    manifest.extend_from_slice(&snap.txn.to_le_bytes());
    manifest.push(chain.len() as u8);
    for sealed in &chain {
        bytes(&mut manifest, sealed.name.as_bytes());
        manifest.extend_from_slice(&sealed.len.to_le_bytes());
        manifest.extend_from_slice(&sealed.digest);
    }
    let next = dir.join("CHECKPOINT.next");
    if io.exists(&next)? {
        io.remove(&next)?;
    }
    let meta = File(io, io.create(&next)?);
    write(io, meta.1, &mut 0, &manifest)?;
    io.sync_data(meta.1)?;
    // A failed rename can have taken effect. Open sweeps any unreferenced complete file.
    pending.preserve = true;
    io.rename(&next, &dir.join("CHECKPOINT"))?;
    io.sync_dir(dir)?;
    // New backing files are already durable. Yield the state lock between bounded installs.
    for chunk in locations.chunks(32) {
        let mut state = lock(shared)?;
        let start = std::time::Instant::now();
        for (key, page, revision, backing) in chunk {
            state.index.clean(key, *page, *revision, backing.clone());
            if state.index.sessions[key].pages[*page].revision == *revision {
                state.dirty.remove(&(key.clone(), *page));
            }
        }
        state.checkpoint_lock_ns = state
            .checkpoint_lock_ns
            .max(start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
    }
    // A write after the snapshot keeps its key dirty for the next checkpoint.
    for chunk in snap.facts.chunks(1024) {
        let mut state = lock(shared)?;
        let start = std::time::Instant::now();
        for (key, revs, _) in chunk {
            if state.facts.map.get(key).and_then(|r| r.list.last()) == revs.last() {
                state.facts.dirty.remove(key);
            }
        }
        state.checkpoint_lock_ns = state
            .checkpoint_lock_ns
            .max(start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
    }
    {
        let mut state = lock(shared)?;
        let start = std::time::Instant::now();
        state.watermark = snap.lsn;
        state.checkpoint_bytes = snap.bytes;
        state.checkpoint_saves += 1;
        state.checkpoint_buffer_peak = state.checkpoint_buffer_peak.max(buffer.peak);
        state.chain = chain;
        state.checkpoint_lock_ns = state
            .checkpoint_lock_ns
            .max(start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
    }
    if snap.full {
        for sealed in snap.chain {
            let _ = io.remove(&dir.join(sealed.name));
        }
        let _ = io.sync_dir(dir);
    }
    Ok(snap.lsn)
}

pub(crate) fn wake(io: &dyn Io, dir: &Path, shared: &Mutex<State>, threshold: u64) -> Result<u64> {
    {
        let state = lock(shared)?;
        state.checkpoint_pending.store(false, std::sync::atomic::Ordering::Release);
        if state.bytes.saturating_sub(state.checkpoint_bytes) < threshold {
            return Ok(state.watermark);
        }
    }
    save(io, dir, shared)
}

#[derive(Debug)]
pub(crate) enum Command {
    Wake,
    Flush(mpsc::SyncSender<Result<u64>>),
    Stop,
}
pub(crate) struct Worker {
    pub(crate) sender: Option<mpsc::SyncSender<Command>>,
    thread: Option<JoinHandle<()>>,
    #[cfg(feature = "sim")]
    inline: Option<Inline>,
}
#[cfg(feature = "sim")]
struct Inline {
    io: Arc<dyn Io>,
    dir: PathBuf,
    state: Arc<Mutex<State>>,
    threshold: u64,
}
impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointWorker").finish_non_exhaustive()
    }
}
impl Worker {
    pub(crate) fn start(
        io: Arc<dyn Io>,
        dir: PathBuf,
        state: Arc<Mutex<State>>,
        opts: &EngineOptions,
    ) -> Result<Self> {
        #[cfg(feature = "sim")]
        if opts.tape.inline_sim {
            return Ok(Self {
                sender: None,
                thread: None,
                inline: Some(Inline { io, dir, state, threshold: opts.checkpoint_bytes }),
            });
        }
        let (sender, receiver) = mpsc::sync_channel(16);
        let interval = opts.checkpoint_interval;
        let threshold = opts.checkpoint_bytes;
        let thread = std::thread::Builder::new()
            .name("zerodb-checkpoint".into())
            .spawn(move || loop {
                let command = receiver.recv_timeout(interval);
                if matches!(command, Ok(Command::Stop) | Err(mpsc::RecvTimeoutError::Disconnected))
                {
                    break;
                }
                let result = if matches!(command, Ok(Command::Wake)) {
                    wake(io.as_ref(), &dir, &state, threshold)
                } else {
                    if let Ok(state) = lock(&state) {
                        state.checkpoint_pending.store(false, std::sync::atomic::Ordering::Release);
                    }
                    save(io.as_ref(), &dir, &state)
                };
                match command {
                    Ok(Command::Flush(reply)) => {
                        let _ = reply.send(result);
                    }
                    _ => {
                        if let Err(error) = result {
                            eprintln!("zerodb: checkpoint failed: {error}");
                        }
                    }
                }
            })
            .map_err(zerodb_tape::Error::from)?;
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
            #[cfg(feature = "sim")]
            inline: None,
        })
    }
    pub(crate) fn flush(&self) -> Result<u64> {
        #[cfg(feature = "sim")]
        if let Some(inline) = &self.inline {
            lock(&inline.state)?
                .checkpoint_pending
                .store(false, std::sync::atomic::Ordering::Release);
            return save(inline.io.as_ref(), &inline.dir, &inline.state);
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .ok_or_else(|| EngineError::from(zerodb_tape::Error::Poisoned))?
            .send(Command::Flush(sender))
            .map_err(|_| EngineError::from(zerodb_tape::Error::Poisoned))?;
        receiver.recv().map_err(|_| EngineError::from(zerodb_tape::Error::Poisoned))?
    }
    #[cfg(feature = "sim")]
    pub(crate) fn wake_sim(&self) -> Result<u64> {
        let inline = self
            .inline
            .as_ref()
            .ok_or_else(|| EngineError::rejected(crate::ErrorKind::InvalidInput))?;
        if !lock(&inline.state)?.checkpoint_pending.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(lock(&inline.state)?.watermark);
        }
        wake(inline.io.as_ref(), &inline.dir, &inline.state, inline.threshold)
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(Command::Stop);
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use zerodb_tape::{HookIo, Op, SimDisk};

    fn fixture(disk: &SimDisk, shapes: &[(usize, usize)], boundary: u64) {
        let path = Path::new("db/ckpt-3.zdb");
        let id = if disk.exists(path).unwrap() {
            disk.open(path).unwrap()
        } else {
            disk.create(path).unwrap()
        };
        disk.set_len(id, 0).unwrap();
        let mut off = 0;
        let mut header = b"ZCK2".to_vec();
        header.extend_from_slice(&boundary.to_le_bytes());
        header.extend_from_slice(&1u64.to_le_bytes());
        write(disk, id, &mut off, &header).unwrap();
        let mut tenant = vec![1];
        tenant.extend_from_slice(&1u32.to_le_bytes());
        tenant.extend_from_slice(&1u64.to_le_bytes());
        bytes(&mut tenant, b"t");
        write(disk, id, &mut off, &tenant).unwrap();
        for &(page, count) in shapes {
            let mut data = vec![2];
            put_key(&mut data, &Key(1, "a".into(), "s".into()));
            data.extend_from_slice(&(page as u32).to_le_bytes());
            data.extend_from_slice(&(count as u32).to_le_bytes());
            for lsn in 2..count as u64 + 2 {
                data.extend_from_slice(&lsn.to_le_bytes());
            }
            write(disk, id, &mut off, &data).unwrap();
        }
        disk.sync_data(id).unwrap();
        let mut sealed_data = vec![0; off as usize];
        assert_eq!(disk.read_at(id, 0, &mut sealed_data).unwrap(), sealed_data.len());
        let digest = blake3::hash(&sealed_data);
        disk.close(id).unwrap();
        let path = Path::new("db/CHECKPOINT");
        let id = if disk.exists(path).unwrap() {
            disk.open(path).unwrap()
        } else {
            disk.create(path).unwrap()
        };
        disk.set_len(id, 0).unwrap();
        let mut data = b"ZCM2".to_vec();
        data.extend_from_slice(&boundary.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes());
        data.push(1);
        bytes(&mut data, b"ckpt-3.zdb");
        data.extend_from_slice(&off.to_le_bytes());
        data.extend_from_slice(digest.as_bytes());
        write(disk, id, &mut 0, &data).unwrap();
        disk.sync_data(id).unwrap();
        disk.close(id).unwrap();
        disk.sync_dir(Path::new("db")).unwrap();
    }

    #[test]
    fn crc_valid_malformed_page_geometry_is_rejected() {
        for shape in [
            vec![(1, 1)],
            vec![(0, 1), (1, 1)],
            vec![(0, 16), (0, 1)],
            vec![(0, 16), (1, 1), (0, 1)],
        ] {
            let disk = SimDisk::new(4);
            fixture(&disk, &shape, 100);
            assert!(
                load_inner(&disk, Path::new("db"), &EngineOptions::default()).is_err(),
                "{shape:?}"
            );
        }
    }

    #[test]
    fn replay_failure_after_a_crc_valid_checkpoint_retries_from_zero() {
        use crate::{Engine, NewTurn, Role, Scope, WindowQuery};
        let disk = Arc::new(SimDisk::new(5));
        let scope = Scope { tenant: "t".into(), agent: "a".into(), session: "s".into() };
        let turn = NewTurn { role: Role::User, content: vec![1], meta: vec![] };
        let open = || {
            let clock = Arc::new(zerodb_tape::sim::SimClock::default());
            Engine::open_with(disk.clone(), clock, "db", EngineOptions::default()).unwrap()
        };
        let engine = open();
        engine.append(&scope, vec![turn.clone(); 2], None).unwrap();
        engine.checkpoint().unwrap();
        engine.append(&scope, vec![turn], None).unwrap();
        drop(engine);
        fixture(&disk, &[(0, 1)], 3);
        let engine = open();
        assert_eq!(engine.metrics().unwrap().checkpoint_warnings, 1);
        assert_eq!(engine.metrics().unwrap().replayed_records, 4);
        assert_eq!(engine.window(&scope, WindowQuery::Last(10)).unwrap().len(), 3);
    }

    #[test]
    fn full_snapshot_shares_pages_and_later_deltas_skip_unchanged_sessions() {
        let disk = SimDisk::new(6);
        let shared = state();
        let key = Key(1, "a".into(), "0".into());
        {
            let mut state = shared.lock().unwrap();
            for i in 0..1000 {
                let key = Key(1, "a".into(), i.to_string());
                state.index.reserve(&key, 1).unwrap();
                state.index.publish(&key, 1, i + 2).unwrap();
                state.dirty.insert((key, 0));
            }
            state.published_lsn = 1001;
            state.txn = 1000;
            state.chain = (0..8)
                .map(|i| CheckpointFile { name: format!("old-{i}"), len: 0, digest: [0; 32] })
                .collect();
        }
        let snap = snapshot(&shared.lock().unwrap());
        let captured = snap.pages.iter().find(|p| p.0 == key).unwrap().3.as_ref().unwrap();
        {
            let mut state = shared.lock().unwrap();
            assert!(Arc::ptr_eq(
                captured,
                state.index.sessions[&key].pages[0].slots.as_ref().unwrap()
            ));
            state.index.reserve(&key, 1).unwrap();
            state.index.publish(&key, 2, 1002).unwrap();
            state.published_lsn = 1002;
            state.txn = 1001;
            assert_eq!(captured.len(), 1);
            assert_eq!(state.index.sessions[&key].pages[0].slots.as_ref().unwrap().len(), 2);
        }
        save(&disk, Path::new("db"), &shared).unwrap();
        let ns = shared.lock().unwrap().snapshot_lock_ns;
        assert!(shared.lock().unwrap().checkpoint_lock_ns >= ns);
        eprintln!("full checkpoint snapshot: 1000 pages, lock_hold_ns={ns}");
        assert!(ns > 0);
        {
            let mut state = shared.lock().unwrap();
            state.index.reserve(&key, 1).unwrap();
            state.index.publish(&key, 3, 1003).unwrap();
            state.dirty.insert((key, 0));
            state.published_lsn = 1003;
            state.txn = 1002;
        }
        save(&disk, Path::new("db"), &shared).unwrap();
        let name = shared.lock().unwrap().chain.last().unwrap().name.clone();
        let file = disk.open(&Path::new("db").join(name)).unwrap();
        assert!(disk.len(file).unwrap() < 128, "a delta rewrites only the changed session");
        disk.close(file).unwrap();
    }

    fn append(state: &Mutex<State>, seq: u64) {
        let mut state = state.lock().unwrap();
        let key = Key(1, "a".into(), "s".into());
        state.index.reserve(&key, 1).unwrap();
        state.index.publish(&key, seq, seq + 1).unwrap();
        state.dirty.insert((key, crate::session::address(seq).0));
        state.published_lsn = seq + 1;
        state.txn = seq;
    }
    fn state() -> Mutex<State> {
        let mut state = State::new(&EngineOptions::default());
        state.tenants.insert("t", 1, 1, 10_000).unwrap();
        Mutex::new(state)
    }

    #[test]
    fn stale_wakes_do_not_save_below_the_next_threshold() {
        let disk = SimDisk::new(17);
        let shared = state();
        for seq in 1..=100 {
            append(&shared, seq);
            shared.lock().unwrap().bytes += 1;
            if seq.is_multiple_of(10) {
                for _ in 0..16 {
                    wake(&disk, Path::new("db"), &shared, 10).unwrap();
                }
            }
        }
        assert_eq!(shared.lock().unwrap().checkpoint_saves, 10);
    }

    #[test]
    fn deltas_compaction_cold_reload_and_corruption_fallback() {
        let disk = SimDisk::new(1);
        let dir = Path::new("db");
        let shared = state();
        for seq in 1..=10 {
            append(&shared, seq);
            save(&disk, dir, &shared).unwrap();
            let loaded = load(&disk, dir, &EngineOptions::default());
            assert_eq!(loaded.watermark, seq + 1);
            assert_eq!(loaded.index.sessions[&Key(1, "a".into(), "s".into())].len, seq);
        }
        let state = shared.lock().unwrap();
        assert_eq!(state.chain.len(), 2);
        let backing = state.index.sessions[&Key(1, "a".into(), "s".into())].pages[0]
            .backing
            .as_ref()
            .unwrap();
        assert_eq!(read_page(&disk, dir, backing).unwrap(), (2..=11).collect::<Vec<_>>());
        let id = disk.open(&dir.join(&state.chain[0].name)).unwrap();
        disk.flip_bit(id, 12).unwrap();
        disk.close(id).unwrap();
        let loaded = load(&disk, dir, &EngineOptions::default());
        assert_eq!(loaded.watermark, 0);
        assert_eq!(loaded.warnings, 1);
    }

    type Image = Arc<Mutex<Option<SimDisk>>>;

    /// Fails the `at`th successful call, except close and space queries, right after it ran.
    /// Returns a crash image taken at that point.
    fn crash_at(disk: SimDisk, at: usize) -> (HookIo, Image) {
        let (image, seen, crash) = (Image::default(), AtomicUsize::new(0), disk.clone());
        let crashed = image.clone();
        let io = HookIo::new(disk).with(move |op, next| {
            next()?;
            let counted = !matches!(op, Op::Close(_) | Op::Available(_) | Op::Total(_));
            if counted && seen.fetch_add(1, Ordering::SeqCst) + 1 == at {
                *crashed.lock().unwrap() = Some(crash.crash()?);
                return Err(zerodb_tape::Error::Io(
                    std::io::ErrorKind::Interrupted,
                    "crash".into(),
                ));
            }
            Ok(())
        });
        (io, image)
    }

    /// Persists only the header and tenant records of the next write, then fails it.
    /// The next directory sync also fails, so cleanup removals stay unsynced.
    fn tear_next_write(io: &HookIo) {
        let (disk, armed, unsynced) =
            (io.inner.clone(), AtomicBool::new(true), AtomicBool::new(false));
        io.add(move |op, next| match op {
            Op::Write(_, file, offset, data) if armed.swap(false, Ordering::SeqCst) => {
                let len = |at: usize| u32::from_le_bytes(data[at + 4..at + 8].try_into().unwrap());
                let header = len(0) as usize;
                disk.write_at(file, offset, &data[..header + len(header) as usize])?;
                disk.sync_data(file)?;
                unsynced.store(true, Ordering::SeqCst);
                Err(zerodb_tape::Error::Io(
                    std::io::ErrorKind::Interrupted,
                    "torn checkpoint".into(),
                ))
            }
            Op::SyncDir(_) if unsynced.swap(false, Ordering::SeqCst) => {
                Err(zerodb_tape::Error::SyncFailed("unsynced cleanup removal".into()))
            }
            _ => next(),
        });
    }

    /// Checkpoint data files created so far, by name.
    fn created(events: &Mutex<Vec<String>>) -> Vec<String> {
        let events = events.lock().unwrap();
        events.iter().filter_map(|e| e.strip_prefix("create db/ckpt-")).map(str::to_owned).collect()
    }

    #[test]
    fn crash_or_failure_at_each_install_operation_keeps_a_chain_or_requests_replay() {
        for seed in 0..16 {
            for at in 1..=20 {
                let disk = SimDisk::new(seed);
                let shared = state();
                append(&shared, 1);
                save(&disk, Path::new("db"), &shared).unwrap();
                append(&shared, 2);
                let (io, image) = crash_at(disk, at);
                let saved = save(&io, Path::new("db"), &shared);
                // Data write and syncs, then metadata write and sync, before the ambiguous rename.
                if [3, 4, 5, 7, 8].contains(&at) {
                    let (disk, case) = (&io.inner, format!("seed {seed}, op {at}"));
                    assert!(saved.is_err(), "{case}");
                    assert!(!disk.exists(Path::new("db/ckpt-3-1.zdb")).unwrap(), "{case}");
                    assert!(!disk.exists(Path::new("db/CHECKPOINT.next")).unwrap(), "{case}");
                    let loaded = load_inner(disk, Path::new("db"), &EngineOptions::default());
                    assert_eq!(loaded.unwrap().watermark, 2, "{case}: partial files removed");
                }
                if let Some(crashed) = image.lock().unwrap().as_ref() {
                    let loaded = load(crashed, Path::new("db"), &EngineOptions::default());
                    if loaded.watermark == 0 {
                        assert_eq!(loaded.warnings, 1, "seed {seed}, op {at}");
                        assert!(loaded.index.sessions.is_empty());
                    } else {
                        assert!(
                            loaded.watermark == 2 || loaded.watermark == 3,
                            "seed {seed}, op {at}"
                        );
                        assert_eq!(loaded.warnings, 0, "seed {seed}, op {at}");
                    }
                };
            }
        }
    }
    #[test]
    fn large_full_snapshot_bounds_encoded_buffer() {
        let shared = state();
        {
            let mut state = shared.lock().unwrap();
            let mut lsn = 1;
            for session in 0..128 {
                let key = Key(1, "a".into(), format!("{session:0128}"));
                state.index.reserve(&key, 5000).unwrap();
                for seq in 1..=5000 {
                    lsn += 1;
                    state.index.publish(&key, seq, lsn).unwrap();
                }
            }
            state.published_lsn = lsn;
            state.txn = 1;
            state.chain = (0..8)
                .map(|i| CheckpointFile { name: format!("old-{i}"), len: 0, digest: [0; 32] })
                .collect();
            for age in 2..10_002 {
                state.dedup.insert(Entry {
                    key: DedupKey(
                        1,
                        Scope::Session(format!("{:0128}", (age - 2) / 5000)),
                        format!("{age:0128}"),
                    ),
                    digest: [1; 32],
                    committed: Committed::Durable {
                        txn: 1,
                        first_lsn: age,
                        last_lsn: age,
                        reply: (((age - 2) % 5000) + 1).to_le_bytes().to_vec(),
                    },
                });
            }
        }
        let (io, writes) = (HookIo::sim(30), Arc::new(Mutex::new(Vec::new())));
        let log = writes.clone();
        io.add(move |op, next| {
            if let Op::Write(_, _, offset, data) = op {
                log.lock().unwrap().push((offset, data.len()));
            }
            next()
        });
        save(&io, Path::new("db"), &shared).unwrap();
        let writes = writes.lock().unwrap();
        assert!(writes.iter().map(|(_, len)| len).sum::<usize>() > 8 * 1024 * 1024);
        assert!(writes.iter().all(|(_, len)| *len <= 4 * 1024 * 1024), "{writes:?}");
        assert!(shared.lock().unwrap().checkpoint_buffer_peak <= BUFFER_LIMIT);
        assert!(shared.lock().unwrap().checkpoint_buffer_peak > BUFFER_LIMIT - 131072);
        assert_eq!(writes[1].0, writes[0].1 as u64);
        let (backing, expected) = {
            let state = shared.lock().unwrap();
            let page = state.index.sessions[&Key(1, "a".into(), format!("{:0128}", 127))]
                .pages
                .last()
                .unwrap();
            (page.backing.clone().unwrap(), page.slots.clone().unwrap())
        };
        assert!(backing.offset > BUFFER_LIMIT as u64);
        assert_eq!(&read_page(&io, Path::new("db"), &backing).unwrap(), expected.as_ref());
        eprintln!(
            "full checkpoint peak_buffer_bytes={} writes={writes:?}",
            shared.lock().unwrap().checkpoint_buffer_peak
        );
        let loaded = load_inner(&io.inner, Path::new("db"), &EngineOptions::default()).unwrap();
        assert_eq!(loaded.index.sessions.len(), 128);
        assert!(loaded.index.sessions.values().all(|entry| entry.len == 5000));
        assert_eq!(loaded.dedup.entries.len(), 10_000);
    }

    #[test]
    fn legacy_manifests_and_altered_chain_files_request_full_replay() {
        // (chain file, truncate it), or None for a manifest in the unsealed legacy format.
        for case in [Some((0, false)), Some((0, true)), Some((1, false)), Some((1, true)), None] {
            let disk = SimDisk::new(38);
            let shared = state();
            for seq in 1..=2 {
                append(&shared, seq);
                save(&disk, Path::new("db"), &shared).unwrap();
            }
            let Some((changed, truncate)) = case else {
                let mut legacy = b"ZCM1".to_vec();
                legacy.extend_from_slice(&2u64.to_le_bytes());
                legacy.extend_from_slice(&1u64.to_le_bytes());
                legacy.push(1);
                bytes(&mut legacy, shared.lock().unwrap().chain[0].name.as_bytes());
                let file = disk.open(Path::new("db/CHECKPOINT")).unwrap();
                disk.set_len(file, 0).unwrap();
                write(&disk, file, &mut 0, &legacy).unwrap();
                disk.sync_data(file).unwrap();
                disk.close(file).unwrap();
                let loaded = load(&disk, Path::new("db"), &EngineOptions::default());
                assert_eq!((loaded.warnings, loaded.watermark), (1, 0), "legacy manifest");
                continue;
            };
            let files = disk
                .list(Path::new("db"))
                .unwrap()
                .into_iter()
                .filter(|p| p.file_name().unwrap().to_str().unwrap().starts_with("ckpt-"))
                .collect::<Vec<_>>();
            let file = disk.open(&files[changed]).unwrap();
            let mut data = vec![0; disk.len(file).unwrap() as usize];
            assert_eq!(disk.read_at(file, 0, &mut data).unwrap(), data.len());
            let len = |at: usize| u32::from_le_bytes(data[at + 4..at + 8].try_into().unwrap());
            let mut offset = 0;
            while data[offset + 8] != 2 {
                offset += len(offset) as usize;
            }
            if truncate {
                disk.set_len(file, offset as u64).unwrap();
            } else {
                let end = offset + len(offset) as usize;
                let slot = end - (changed + 1) * 8;
                data[slot..slot + 8].copy_from_slice(&1u64.to_le_bytes());
                let sum = crc(&data[offset + 4..end]);
                data[offset..offset + 4].copy_from_slice(&sum.to_le_bytes());
                disk.write_at(file, 0, &data).unwrap();
            }
            disk.sync_data(file).unwrap();
            disk.close(file).unwrap();
            let loaded = load(&disk, Path::new("db"), &EngineOptions::default());
            assert_eq!((loaded.warnings, loaded.watermark), (1, 0), "{case:?}");
        }
    }

    #[test]
    fn failed_attempts_consume_names_and_counter_never_wraps() {
        let shared = state();
        append(&shared, 1);
        let io = HookIo::sim(39);
        let events = io.record();
        for _ in 0..2 {
            tear_next_write(&io);
            assert!(save(&io, Path::new("db"), &shared).is_err());
        }
        save(&io, Path::new("db"), &shared).unwrap();
        assert_eq!(created(&events), ["2.zdb", "2-1.zdb", "2-2.zdb"]);
        assert_eq!(shared.lock().unwrap().checkpoint_attempt, 3);
        append(&shared, 2);
        shared.lock().unwrap().checkpoint_attempt = u64::MAX;
        let error = save(&io, Path::new("db"), &shared).unwrap_err();
        assert!(matches!(error.kind, ErrorKind::ResourceExhausted("checkpoint_attempts")));
        assert_eq!(shared.lock().unwrap().checkpoint_attempt, u64::MAX);
    }

    #[test]
    fn failed_save_unsynced_cleanup_retry_crash_never_accepts_stale_prefix() {
        let mut manifests = 0;
        for seed in 0..256 {
            let shared = state();
            append(&shared, 1);
            let io = HookIo::sim(seed);
            let events = io.record();
            tear_next_write(&io);
            assert!(save(&io, Path::new("db"), &shared).is_err());
            assert!(!io.inner.exists(Path::new("db/ckpt-2.zdb")).unwrap());
            let images = io.capture();
            save(&io, Path::new("db"), &shared).unwrap();
            assert_eq!(created(&events), ["2.zdb", "2-1.zdb"]);
            let images = images.lock().unwrap();
            let (_, image) = images.iter().find(|(op, _)| op.starts_with("rename")).unwrap();
            if image.exists(Path::new("db/CHECKPOINT")).unwrap() {
                manifests += 1;
                let loaded = load(image, Path::new("db"), &EngineOptions::default());
                if loaded.watermark == 2 {
                    assert_eq!(
                        loaded.index.sessions.get(&Key(1, "a".into(), "s".into())).map(|e| e.len),
                        Some(1),
                        "stale file accepted at seed {seed}"
                    );
                } else {
                    assert_eq!(loaded.watermark, 0);
                    assert_eq!(loaded.warnings, 1);
                }
            }
        }
        assert!(manifests > 0);
    }
}

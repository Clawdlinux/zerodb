use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use zerodb_tape::sim::{DataDir, PreparedBatch, SimClock, WriteRequest, WriterCore};
use zerodb_tape::{
    Committed, CommittedRecord, Durability, Error, HookIo, Io, Kind, NoopStateMachine, Op, Outcome,
    Prepare, RecordInput, RecoveryReport, Result, SimDisk, StateMachine, TapeOptions, WriteError,
    WriteResult,
};

mod fs {
    const ROOT: &str = env!("CARGO_TARGET_TMPDIR");
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/support.rs"));
}
pub(crate) use fs::TestDir;

pub(crate) fn record(kind: Kind, tenant_id: u32, content: impl AsRef<[u8]>) -> RecordInput {
    RecordInput { kind, tenant_id, content: content.as_ref().to_vec() }
}

/// One turn for tenant 1 with payload `hello`.
pub(crate) fn hello() -> Vec<RecordInput> {
    vec![record(Kind::Turn, 1, b"hello")]
}

pub(crate) fn opts(size: u64) -> TapeOptions {
    TapeOptions { segment_size: size, ..TapeOptions::default() }
}

pub(crate) fn clock() -> Arc<SimClock> {
    Arc::new(SimClock::default())
}

#[track_caller]
pub(crate) fn open(
    io: Arc<dyn Io>,
    opts: TapeOptions,
) -> (WriterCore<NoopStateMachine>, RecoveryReport) {
    WriterCore::open(io, clock(), "db", opts).unwrap()
}

/// A crash image of `disk`.
#[track_caller]
pub(crate) fn image(disk: &SimDisk) -> Arc<SimDisk> {
    Arc::new(disk.crash().unwrap())
}

/// Path of segment `id` in `db`.
pub(crate) fn seg(id: u64) -> PathBuf {
    PathBuf::from(format!("db/seg-{id:016}.zdb"))
}

pub(crate) fn submit<S: StateMachine>(
    core: &mut WriterCore<S>,
    records: Vec<RecordInput>,
) -> mpsc::Receiver<WriteResult> {
    let (request, reply) =
        WriteRequest::new(PreparedBatch::new(records, Durability::Durable).unwrap());
    core.enqueue(request);
    reply
}

/// Submits one batch, runs one writer step and returns the reply that step sent.
#[track_caller]
pub(crate) fn commit<S: StateMachine>(
    core: &mut WriterCore<S>,
    records: Vec<RecordInput>,
) -> WriteResult {
    let reply = submit(core, records);
    core.step();
    reply.try_recv().unwrap()
}

#[track_caller]
pub(crate) fn read(io: &dyn Io, path: impl AsRef<Path>) -> Vec<u8> {
    let file = io.open(path.as_ref()).unwrap();
    let mut bytes = vec![0; io.len(file).unwrap() as usize];
    assert_eq!(io.read_at(file, 0, &mut bytes).unwrap(), bytes.len());
    io.close(file).unwrap();
    bytes
}

/// Every file in `db` with its bytes.
pub(crate) fn files(io: &dyn Io) -> Vec<(PathBuf, Vec<u8>)> {
    io.list(Path::new("db")).unwrap().into_iter().map(|p| (p.clone(), read(io, p))).collect()
}

#[track_caller]
pub(crate) fn superblock(disk: &SimDisk, size: u64) -> u64 {
    DataDir::open(image(disk), "db", None, size).unwrap().superblock()
}

pub(crate) fn superblock_installs(events: &Mutex<Vec<String>>) -> usize {
    let installs = |e: &&String| e.as_str() == "rename db/SUPERBLOCK.tmp db/SUPERBLOCK";
    events.lock().unwrap().iter().filter(installs).count()
}

pub(crate) fn data_sync(op: &Op<'_>) -> bool {
    matches!(op, Op::SyncData(_))
}

pub(crate) fn segment_bytes(op: &Op<'_>) -> u64 {
    match op {
        Op::Write(.., bytes) if op.is_segment() => bytes.len() as u64,
        _ => 0,
    }
}

/// Sets the capacity right after the next free-space check, so the next allocation fails.
pub(crate) fn shrink_after_space_check(io: &HookIo, capacity: u64) {
    let (disk, armed) = (io.inner.clone(), AtomicBool::new(true));
    io.add(move |op, next| {
        next()?;
        if matches!(op, Op::Available(_)) && armed.swap(false, Ordering::SeqCst) {
            disk.set_capacity(capacity)?;
        }
        Ok(())
    });
}

/// Makes segment and spare `allocate` calls only set the length, like a filesystem without
/// preallocation. Index files keep real allocations. They are never synced, so an undone
/// allocation could come back in their crash image.
pub(crate) fn sparse_allocations(io: &HookIo) {
    let disk = io.inner.clone();
    io.add(move |op, next| match op {
        Op::Allocate(path, len) if !op.name().starts_with("LSN-") => {
            let file = disk.open(path)?;
            let old = disk.len(file)?.min(len);
            next()?;
            // Return the blocks the real allocation reserved past the old end.
            disk.set_len(file, old)?;
            disk.set_len(file, len)?;
            disk.close(file)
        }
        _ => next(),
    });
}

#[derive(Debug, Default)]
pub(crate) struct Log {
    pub published: Vec<CommittedRecord>,
    pub replayed: Vec<CommittedRecord>,
    pub prepares: usize,
    pub threads: Vec<std::thread::ThreadId>,
}

#[derive(Default)]
pub(crate) enum Behavior {
    #[default]
    Pass,
    Sequence,
    Dedup,
    Resize {
        len: usize,
        on_prepare: Option<usize>,
    },
    InvalidRewrite,
}

#[derive(Default)]
pub(crate) struct Machine {
    pub expected_segments: Option<(Arc<SimDisk>, usize)>,
    pub behavior: Behavior,
    pub log: Arc<Mutex<Log>>,
    pub replies: Arc<Mutex<Vec<mpsc::Receiver<WriteResult>>>>,
    pub fail_replay: bool,
    pub seq: u64,
    pub cached: bool,
}

impl Machine {
    pub(crate) fn new(behavior: Behavior) -> Self {
        Self { behavior, ..Self::default() }
    }
}

fn reject(reason: &str) -> Prepare {
    let kind = Error::InvalidFrame(reason.into());
    Prepare::Reject(WriteError { kind, outcome: Outcome::NotCommitted })
}

impl StateMachine for Machine {
    fn prepare(&mut self, batch: &mut PreparedBatch) -> Prepare {
        let mut log = self.log.lock().unwrap();
        log.prepares += 1;
        log.threads.push(std::thread::current().id());
        if let Some((disk, expected)) = &self.expected_segments {
            let list = disk.list(Path::new("db")).unwrap();
            let count = list.iter().filter(|p| p.to_string_lossy().contains("/seg-")).count();
            assert_eq!(count, *expected, "segments must be reserved before prepare");
        }
        match self.behavior {
            Behavior::Pass => {}
            Behavior::Resize { len, on_prepare } => {
                if on_prepare.is_none_or(|prepare| log.prepares == prepare) {
                    batch.records_mut().for_each(|record| record.content.resize(len, 7));
                }
            }
            Behavior::InvalidRewrite => {
                let record = batch.records_mut().next().unwrap();
                match record.content.as_slice() {
                    b"footer" => record.kind = Kind::Footer,
                    _ => record.content = vec![0; 4 * 1024 * 1024],
                }
            }
            Behavior::Sequence => {
                match batch.records().next().unwrap().content.as_slice() {
                    _ if batch.application_arg().is_some() => return reject("conditional"),
                    b"reject" => return reject("rejected"),
                    b"reply" => return Prepare::Reply(receipt(77, 88)),
                    _ => {}
                }
                for record in batch.records_mut() {
                    self.seq += 1;
                    record.content = self.seq.to_le_bytes().to_vec();
                }
            }
            Behavior::Dedup => {
                if batch.records().next().unwrap().content != b"trailing" {
                    if self.cached {
                        return Prepare::Reply(receipt(1, 1));
                    }
                    self.cached = true;
                }
            }
        }
        Prepare::Write
    }
    fn publish(&mut self, records: &[CommittedRecord]) {
        assert!(
            !matches!(self.behavior, Behavior::InvalidRewrite),
            "invalid rewrite must not publish"
        );
        for reply in self.replies.lock().unwrap().iter() {
            assert!(matches!(reply.try_recv(), Err(mpsc::TryRecvError::Empty)));
        }
        self.log.lock().unwrap().published.extend_from_slice(records);
    }
    fn replay(&mut self, record: &CommittedRecord) -> Result<()> {
        if self.fail_replay {
            return Err(Error::Configuration("replay failed".into()));
        }
        if matches!(self.behavior, Behavior::Sequence) {
            self.seq = u64::from_le_bytes(record.payload.as_ref().try_into().unwrap());
        }
        self.log.lock().unwrap().replayed.push(record.clone());
        Ok(())
    }
}

pub(crate) fn receipt(txn: u64, lsn: u64) -> Committed {
    Committed::Durable { txn, first_lsn: lsn, last_lsn: lsn, reply: Vec::new() }
}

pub(crate) mod wire {
    use super::*;
    use zerodb_tape::sim::crc32c;
    use zerodb_tape::sim::frame::{self, DecodeError, Footer, Frame, Header, VERSION};

    /// A frame header with fields 11, 22, 33 and 44, so layout tests can find each one.
    pub(crate) fn frame(kind: Kind, content: &[u8]) -> Frame {
        let header = Header {
            kind,
            version: VERSION,
            flags: 0,
            txn: 11,
            lsn: 22,
            commit_time_ns: 33,
            tenant_id: 44,
        };
        Frame { header, payload: content.to_vec() }
    }

    pub(crate) fn encode(frame: &Frame) -> zerodb_tape::Result<Vec<u8>> {
        frame::encode(frame, 7, 128)
    }

    pub(crate) fn decode(bytes: &[u8]) -> std::result::Result<(Frame, usize), DecodeError> {
        frame::decode(bytes, 7, 128)
    }

    pub(crate) fn refresh_crc(bytes: &mut [u8]) {
        let crc = frame::checksum(7, 128, &bytes[4..]);
        bytes[..4].copy_from_slice(&crc.to_le_bytes());
    }

    fn footer(header: &Header, footer: Footer) -> Frame {
        let header = Header { kind: Kind::Footer, tenant_id: 0, ..header.clone() };
        Frame { header, payload: footer.encode().to_vec() }
    }

    /// One `hello` turn and its footer at `location`. Returns the bytes and the next location.
    pub(crate) fn transaction(
        lsn: u64,
        txn: u64,
        (segment, offset): (u64, u64),
    ) -> (Vec<u8>, (u64, u64)) {
        let header = Header {
            kind: Kind::Turn,
            version: VERSION,
            flags: 0,
            txn,
            lsn,
            commit_time_ns: txn,
            tenant_id: 1,
        };
        let turn = Frame { header: header.clone(), payload: b"hello".to_vec() };
        let mut bytes = frame::encode(&turn, segment, offset).unwrap();
        let proof = Footer {
            txn,
            first_lsn: lsn,
            synced_lsn: 0,
            record_count: 1,
            records_crc: crc32c(&bytes[..4]),
        };
        let end = offset + bytes.len() as u64;
        bytes.extend(frame::encode(&footer(&header, proof), segment, end).unwrap());
        let next = (segment, offset + bytes.len() as u64);
        (bytes, next)
    }

    /// A 1024-byte data directory with segment 1 prepared.
    pub(crate) fn setup() -> (Arc<SimDisk>, DataDir) {
        let io = Arc::new(SimDisk::new(1));
        let mut dir = DataDir::open(io.clone(), "db", None, 1024).unwrap();
        dir.prepare_next_segment(1024).unwrap();
        (io, dir)
    }

    /// Writes and syncs `bytes` into segment `id`.
    pub(crate) fn write(io: &SimDisk, id: u64, offset: u64, bytes: &[u8]) {
        let file = io.open(&seg(id)).unwrap();
        io.write_at(file, offset, bytes).unwrap();
        io.sync_data(file).unwrap();
        io.close(file).unwrap();
    }

    #[derive(Clone, Copy, Debug)]
    pub(crate) enum Damage {
        Flip(u64),
        ZeroFooter(u64),
        GarbageFooter,
        MisdirectedFooter(u64, u64),
        InvalidProof(usize),
        EmptyFooter,
    }

    /// Damages segment 1 and syncs it.
    pub(crate) fn damage(disk: &SimDisk, size: u64, fault: Damage, case: &str) {
        let first = disk.open(&seg(1)).unwrap();
        match fault {
            Damage::Flip(offset) => disk.flip_bit(first, offset).unwrap(),
            Damage::ZeroFooter(offset) => disk.write_at(first, offset, &[0; 72]).unwrap(),
            Damage::GarbageFooter => disk.write_at(first, 48, &[0xff; 72]).unwrap(),
            Damage::MisdirectedFooter(segment, offset) => {
                let bytes = read(disk, seg(segment));
                let at = offset as usize;
                disk.write_at(first, 48, &bytes[at..at + 48]).unwrap();
            }
            Damage::InvalidProof(field) => {
                let mut bytes = read(disk, seg(1));
                assert_eq!(bytes.len() as u64, size, "{case}");
                bytes[40] ^= 1;
                let footer = 168;
                bytes[footer + 40 + field] ^= if field == 16 { 2 } else { 1 };
                let crc = frame::checksum(1, footer as u64, &bytes[footer + 4..footer + 72]);
                bytes[footer..footer + 4].copy_from_slice(&crc.to_le_bytes());
                disk.write_at(first, 0, &bytes).unwrap();
            }
            Damage::EmptyFooter => {
                let header =
                    Header { txn: 1, lsn: 1, commit_time_ns: 1, ..frame(Kind::Turn, b"").header };
                let proof =
                    Footer { txn: 1, first_lsn: 1, synced_lsn: 0, record_count: 1, records_crc: 0 };
                disk.write_at(first, 0, &frame::encode(&footer(&header, proof), 1, 0).unwrap())
                    .unwrap();
            }
        }
        disk.sync_data(first).unwrap();
        disk.close(first).unwrap();
    }
}

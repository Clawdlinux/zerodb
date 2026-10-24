//! Seeded, single-threaded crash workloads for the production writer state machine.

#![allow(clippy::unwrap_used)]

pub use crate::crc32c::{crc32c, Crc32c};
pub use crate::dir::{atomic_install, make_spare, DataDir};
pub use crate::io::SimClock;
pub use crate::recovery::{recover, Recovered};
pub use crate::segment::{SegmentWriter, DEFAULT_SEGMENT_SIZE};
pub use crate::writer::{PreparedBatch, WriteRequest, WriterCore};
use crate::{
    Clock, Committed, CommittedRecord, Durability, Error, HookIo, Io, Kind, Op, Outcome, Prepare,
    RecordInput, RecoveryReport, Result, SimDisk, StateMachine, Tape, TapeOptions as Options,
    WriteResult,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};

/// Pauses one publication after application state changes, before the tape watermark advances.
#[cfg(feature = "sim")]
#[derive(Debug, Default)]
pub struct PublishPause {
    gate: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
}

#[cfg(feature = "sim")]
impl PublishPause {
    /// Arms one pause. Receive arrival, then send or drop the release sender to resume.
    pub fn arm(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (arrived, arrival) = mpsc::sync_channel(0);
        let (release, resume) = mpsc::channel();
        let mut gate = self.gate.lock().unwrap();
        assert!(gate.is_none(), "publication pause is already armed");
        *gate = Some((arrived, resume));
        (arrival, release)
    }

    pub(crate) fn pause(&self) {
        let gate = self.gate.lock().unwrap().take();
        if let Some((arrived, resume)) = gate {
            if arrived.send(()).is_ok() {
                let _ = resume.recv();
            }
        }
    }
}

/// Frame codecs and format constants for simulation and storage-format tests.
pub mod frame {
    pub use crate::frame::*;
}

/// SplitMix64 keeps workload generation independent from disk crash randomness.
#[derive(Debug)]
struct Rng(u64);

const MIN_TRANSACTION_BYTES: usize = 2 * frame::HEADER_LEN + 32;

/// Storage error class expected after a prepared capacity fault.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapacityError {
    /// The partial data write failed.
    DiskFull,
    /// The writer rejected a later request without preparing it.
    Poisoned,
    /// A written group failed its data sync.
    SyncFailed,
}

impl CapacityError {
    /// Checks the public tape error without depending on its message.
    pub fn matches(self, error: &Error) -> bool {
        matches!(
            (self, error),
            (Self::DiskFull, Error::DiskFull)
                | (Self::Poisoned, Error::Poisoned)
                | (Self::SyncFailed, Error::SyncFailed(_))
        )
    }
}

/// Capacity changes are relative to the blocks already charged to the database.
#[derive(Clone, Debug)]
pub struct CapacityPlan {
    /// Explicit writer free-space floor.
    pub floor: u64,
    /// Free bytes when the clean-rejection group runs.
    pub free: u64,
    /// Bytes released by another process before retry.
    pub freed: u64,
    /// Successful workload steps before the capacity fault.
    pub delay: usize,
    /// Seed for a budget below the first data-write length, or a failed sync when absent.
    pub write_seed: Option<u64>,
    /// Crash-image I/O boundary during the storage error. Zero disables capture.
    pub cut: usize,
}

impl CapacityPlan {
    /// Chooses below-floor, near-floor, or within-segment headroom.
    pub fn new(seed: u64) -> Self {
        let mut rng = Rng::new(seed ^ 0xd15c_f011);
        let floor = [4096, 8192, 32 * 1024][rng.below(3)];
        let free = match rng.below(3) {
            0 => rng.below(floor as usize) as u64,
            1 => floor - 1 - rng.below(512) as u64,
            _ => floor + rng.below(64) as u64,
        };
        Self {
            floor,
            free,
            freed: (8 + rng.below(9)) as u64 * 64 * 1024,
            delay: rng.below(3),
            write_seed: (rng.below(2) == 0).then(|| rng.next_u64()),
            cut: rng.below(5),
        }
    }

    /// Restricts headroom without removing existing database blocks.
    pub fn limit(&self, disk: &SimDisk) {
        let path = Path::new("db");
        let used = disk.total_bytes(path).unwrap() - disk.available_bytes(path).unwrap();
        disk.set_capacity(used + self.free).unwrap();
    }

    /// Releases the seeded number of bytes.
    pub fn free(&self, disk: &SimDisk) {
        disk.set_capacity(disk.total_bytes(Path::new("db")).unwrap() + self.freed).unwrap();
    }

    /// Returns the error class and allowed outcomes for a request's position in the group.
    pub fn expected(&self, position: usize) -> (CapacityError, &'static [Outcome]) {
        if self.write_seed.is_none() {
            (CapacityError::SyncFailed, &[Outcome::Unknown])
        } else if position == 0 {
            (CapacityError::DiskFull, &[Outcome::NotCommitted, Outcome::Unknown])
        } else {
            (CapacityError::Poisoned, &[Outcome::NotCommitted])
        }
    }
}

/// Targets data-segment I/O, after preflight metadata and application preparation.
#[derive(Debug, Default)]
pub struct StorageFullFault {
    write_seed: AtomicU64,
    write: AtomicBool,
    sync: AtomicBool,
    written: AtomicBool,
}

impl StorageFullFault {
    /// Arms a single data-write or data-sync error.
    pub fn arm(&self, plan: &CapacityPlan) {
        self.written.store(false, Ordering::SeqCst);
        if let Some(seed) = plan.write_seed {
            self.write_seed.store(seed, Ordering::SeqCst);
        }
        self.write.store(plan.write_seed.is_some(), Ordering::SeqCst);
        self.sync.store(plan.write_seed.is_none(), Ordering::SeqCst);
    }

    /// Applies the armed error to a segment write or sync. Call it in a hook before `next`.
    ///
    /// A write error sets the disk's partial-write budget at the first data write.
    /// A sync error waits until data has been written.
    pub fn before(&self, disk: &SimDisk, op: &Op<'_>) {
        match *op {
            Op::Write(.., data) if op.is_segment() => {
                if self.sync.load(Ordering::SeqCst) {
                    self.written.store(true, Ordering::SeqCst);
                }
                if self.write.swap(false, Ordering::SeqCst) {
                    assert!(data.len() >= MIN_TRANSACTION_BYTES);
                    let seed = self.write_seed.load(Ordering::SeqCst);
                    disk.enospc_after_bytes(Rng::new(seed).below(data.len())).unwrap();
                }
            }
            Op::SyncData(_)
                if op.is_segment()
                    && self.written.load(Ordering::SeqCst)
                    && self.sync.swap(false, Ordering::SeqCst) =>
            {
                disk.storage_full_next_sync().unwrap();
            }
            _ => {}
        }
    }
}

fn fault_request<S: StateMachine>(
    core: &mut WriterCore<S>,
    value: u8,
    count: usize,
) -> mpsc::Receiver<WriteResult> {
    let batch = PreparedBatch::new(
        vec![RecordInput { kind: Kind::Turn, tenant_id: 1, content: vec![value; 8] }; count],
        Durability::Durable,
    )
    .unwrap();
    let (request, reply) = WriteRequest::new(batch);
    core.enqueue(request);
    reply
}

/// Checks v2 corruption proofs, stale footers, reordered tails, empty sync steps and operator-cut crashes.
pub fn frame_faults(seed: u64) {
    let mut rng = Rng::new(seed ^ 0xf2f2_95ba);
    for proof in [false, true] {
        let disk = Arc::new(SimDisk::new(seed));
        let size = if seed.is_multiple_of(2) { 128 } else { 1024 };
        let opts = Options { segment_size: size, ..Options::default() };
        let (mut core, _) =
            WriterCore::open(disk.clone(), Arc::new(SimClock::new(1000)), "db", opts.clone())
                .unwrap();
        for _ in 0..if proof { 3 } else { 1 } {
            let reply = fault_request(&mut core, 7, 1);
            core.step();
            reply.recv().unwrap().unwrap();
        }
        drop(core);
        let file = disk.open(Path::new("db/seg-0000000000000001.zdb")).unwrap();
        if seed % 9 == 7 {
            disk.write_at(file, 48, &[0xff; 72]).unwrap();
            disk.sync_data(file).unwrap();
        } else if seed % 9 == 8 {
            let misplaced = frame::encode(
                &frame::Frame {
                    header: frame::Header {
                        kind: Kind::Turn,
                        version: 2,
                        flags: 0,
                        txn: 2,
                        lsn: 2,
                        commit_time_ns: 1001,
                        tenant_id: 1,
                    },
                    payload: vec![7; 8],
                },
                99,
                0,
            )
            .unwrap();
            disk.write_at(file, 48, &misplaced).unwrap();
            disk.sync_data(file).unwrap();
        } else if seed % 9 == 5 {
            disk.write_at(file, 48, &[0; 72]).unwrap();
            disk.sync_data(file).unwrap();
        } else {
            let offset = match seed % 9 {
                0 => 4,
                2 => 48 + 8,
                3 => 48 + 20,
                4 => 48 + 40 + 8,
                _ => 40,
            };
            disk.flip_bit_at(file, offset, rng.below(8) as u8).unwrap();
        }
        disk.close(file).unwrap();
        let result = Tape::open(disk, Arc::new(SimClock::new(0)), "db", opts);
        if proof {
            let reported = if seed % 9 >= 7 { 2 } else { 1 };
            assert!(
                matches!(result, Err(Error::CorruptionDetected { lsn, .. }) if lsn == reported),
                "seed={seed}"
            );
        } else {
            let (_, report) = result.unwrap();
            assert_eq!(report.durable_lsn, 0);
            assert_eq!(report.truncated.unwrap().0, 1..=1);
        }
    }

    // Reuse the exact old location and transaction fields. Only footer-to-record binding can reject this.
    let disk = Arc::new(SimDisk::new(seed));
    let mut dir = DataDir::open(disk.clone(), "stale", None, 1024).unwrap();
    let segment = dir.prepare_next_segment(1024).unwrap();
    let header = frame::Header {
        kind: Kind::Turn,
        version: frame::VERSION,
        flags: 0,
        txn: 1,
        lsn: 1,
        commit_time_ns: 1,
        tenant_id: 1,
    };
    let first =
        frame::encode(&frame::Frame { header: header.clone(), payload: vec![1; 8] }, segment, 0)
            .unwrap();
    let second = frame::encode(
        &frame::Frame { header: frame::Header { lsn: 2, ..header.clone() }, payload: vec![2; 8] },
        segment,
        first.len() as u64,
    )
    .unwrap();
    let footer = frame::Footer {
        txn: 1,
        first_lsn: 1,
        synced_lsn: 0,
        record_count: 2,
        records_crc: crc32c(&[&first[..4], &second[..4]].concat()),
    };
    let end = frame::encode(
        &frame::Frame {
            header: frame::Header { kind: Kind::Footer, lsn: 2, tenant_id: 0, ..header.clone() },
            payload: footer.encode().to_vec(),
        },
        segment,
        (first.len() + second.len()) as u64,
    )
    .unwrap();
    let replacement =
        frame::encode(&frame::Frame { header, payload: vec![3; 8] }, segment, 0).unwrap();
    let file = disk.open(&dir.segment_path(segment)).unwrap();
    disk.write_at(file, 0, &[first, second, end].concat()).unwrap();
    disk.sync_data(file).unwrap();
    disk.write_at(file, 0, &replacement).unwrap();
    disk.sync_data(file).unwrap();
    disk.close(file).unwrap();
    let report = recover(&mut dir, 1024).unwrap().report;
    assert_eq!(report.durable_lsn, 0);
    assert_eq!(report.truncated.unwrap().0, 1..=2);
    drop(dir);

    struct Replies(Arc<std::sync::atomic::AtomicBool>);
    impl StateMachine for Replies {
        fn prepare(&mut self, _: &mut PreparedBatch) -> Prepare {
            if self.0.load(Ordering::SeqCst) {
                Prepare::Reply(Committed::Durable {
                    txn: 1,
                    first_lsn: 1,
                    last_lsn: 1,
                    reply: vec![],
                })
            } else {
                Prepare::Write
            }
        }
        fn publish(&mut self, _: &[CommittedRecord]) {}
        fn replay(&mut self, _: &CommittedRecord) -> Result<()> {
            Ok(())
        }
    }
    let io = CrashIo::new(SimDisk::new(seed));
    let replies_only = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let opts = Options { segment_size: 1024, ..Options::default() };
    let (mut core, _) = WriterCore::open_with(
        Replies(replies_only.clone()),
        io.hooks.clone(),
        Arc::new(SimClock::default()),
        "db",
        opts.clone(),
    )
    .unwrap();
    let reply = fault_request(&mut core, 1, 1);
    core.step();
    reply.recv().unwrap().unwrap();
    replies_only.store(true, Ordering::SeqCst);
    for _ in 0..1 + rng.below(4) {
        let reply = fault_request(&mut core, 1, 1);
        core.step();
        reply.recv().unwrap().unwrap();
        assert_eq!(core.durable_lsn(), 1);
    }
    replies_only.store(false, Ordering::SeqCst);
    let replies: Vec<_> = (2..=4).map(|value| fault_request(&mut core, value, 1)).collect();
    io.faults.cut.store(1 + rng.below(3), Ordering::SeqCst);
    core.step();
    for reply in replies {
        reply.recv().unwrap().unwrap();
    }
    drop(core);
    let image = Arc::new(io.faults.image.lock().unwrap().take().unwrap());
    let (tape, report) =
        Tape::open(image, Arc::new(SimClock::default()), "db", opts.clone()).unwrap();
    assert!(
        (1..=4).contains(&report.durable_lsn),
        "same-group writeback cannot prove its own torn tail"
    );
    drop(tape);
    let file = io.disk.open(Path::new("db/seg-0000000000000001.zdb")).unwrap();
    io.disk.flip_bit_at(file, 120 + 40, rng.below(8) as u8).unwrap();
    io.disk.close(file).unwrap();
    let (_, report) =
        Tape::open(Arc::new(io.disk.clone()), Arc::new(SimClock::default()), "db", opts).unwrap();
    assert_eq!(report.durable_lsn, 1, "later footers in the same sync group are not proof");

    let io = CrashIo::new(SimDisk::new(seed));
    let opts = Options { segment_size: 128, ..Options::default() };
    let (mut core, _) =
        WriterCore::open(io.hooks.clone(), Arc::new(SimClock::default()), "cut", opts.clone())
            .unwrap();
    for _ in 0..3 {
        let reply = fault_request(&mut core, 1, 1);
        core.step();
        reply.recv().unwrap().unwrap();
    }
    core.shutdown().unwrap();
    drop(core);
    io.faults.cut.store(1 + rng.below(24), Ordering::SeqCst);
    Tape::recover_truncate_at_with(io.hooks.clone(), "cut", 1).unwrap();
    let image = io.faults.image.lock().unwrap().take().unwrap_or_else(|| io.disk.crash().unwrap());
    let (_, report) =
        Tape::open(Arc::new(image), Arc::new(SimClock::default()), "cut", opts).unwrap();
    assert!(report.durable_lsn == 1 || report.durable_lsn == 3);
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut bytes = vec![0; len];
        for chunk in bytes.chunks_mut(8) {
            chunk.copy_from_slice(&self.next_u64().to_le_bytes()[..chunk.len()]);
        }
        bytes
    }
}

/// Counters that the [`CrashIo`] hook checks on every call.
#[derive(Debug, Default)]
struct Faults {
    storage_full: StorageFullFault,
    cut: AtomicUsize,
    fail_sync: AtomicUsize,
    image: Mutex<Option<SimDisk>>,
}

/// Captures one crash image at a chosen I/O boundary, without unwinding the writer.
///
/// Replies produced after that boundary are ignored by the oracle.
struct CrashIo {
    hooks: Arc<HookIo>,
    disk: SimDisk,
    faults: Arc<Faults>,
}

impl CrashIo {
    fn new(disk: SimDisk) -> Self {
        let faults = Arc::new(Faults::default());
        let (f, d) = (faults.clone(), disk.clone());
        let hooks = HookIo::new(disk.clone()).with(move |op, next| {
            if op.is_sync() && tick(&f.fail_sync) {
                d.fail_next_sync()?;
            }
            f.storage_full.before(&d, &op);
            let result = next();
            if op.mutates() && tick(&f.cut) {
                *f.image.lock().unwrap() = Some(d.crash()?);
            }
            result
        });
        Self { hooks: Arc::new(hooks), disk, faults }
    }

    fn cut_happened(&self) -> bool {
        self.faults.image.lock().unwrap().is_some()
    }
}

/// Counts down a nonzero counter. True when it reaches zero.
fn tick(counter: &AtomicUsize) -> bool {
    let n = counter.load(Ordering::SeqCst);
    n != 0 && counter.fetch_sub(1, Ordering::SeqCst) == 1
}

#[derive(Clone, Debug)]
struct Batch {
    records: Vec<RecordInput>,
}

#[derive(Clone, Debug, Default)]
struct Oracle {
    batches: Vec<Batch>,
    durable: BTreeMap<u64, Vec<u8>>,
}

#[derive(Debug, Default)]
struct Publications(Mutex<Vec<u64>>);

impl Publications {
    fn record(&self, record: &CommittedRecord) {
        let mut lsns = self.0.lock().unwrap();
        assert_eq!(record.lsn, lsns.last().copied().unwrap_or(0) + 1);
        lsns.push(record.lsn);
    }

    fn verify(&self, durable_lsn: u64) {
        let lsns = self.0.lock().unwrap();
        assert_eq!(lsns.len() as u64, durable_lsn, "durable records publish exactly once");
    }

    fn reply(&self, reply: &WriteResult) {
        if let Ok(Committed::Durable { first_lsn, last_lsn, .. }) = reply {
            let lsns = self.0.lock().unwrap();
            for lsn in *first_lsn..=*last_lsn {
                assert_eq!(lsns.get(lsn as usize - 1), Some(&lsn), "ack precedes publish");
            }
        }
    }
}

#[derive(Debug, Default)]
struct RecordingStateMachine(Arc<Publications>);

impl StateMachine for RecordingStateMachine {
    fn prepare(&mut self, _batch: &mut PreparedBatch) -> Prepare {
        Prepare::Write
    }

    fn publish(&mut self, records: &[CommittedRecord]) {
        for record in records {
            self.0.record(record);
        }
    }

    fn replay(&mut self, record: &CommittedRecord) -> Result<()> {
        self.0.record(record);
        Ok(())
    }
}

fn recording_core(
    io: Arc<dyn Io>,
    clock: Arc<dyn Clock>,
    opts: Options,
) -> (WriterCore<RecordingStateMachine>, Arc<Publications>) {
    let machine = RecordingStateMachine::default();
    let published = machine.0.clone();
    let (core, _) = WriterCore::open_with(machine, io, clock, "db", opts).unwrap();
    published.verify(core.durable_lsn());
    (core, published)
}

fn recording_tape(
    io: Arc<dyn Io>,
    clock: Arc<dyn Clock>,
    opts: Options,
) -> (Tape<RecordingStateMachine>, RecoveryReport) {
    let machine = RecordingStateMachine::default();
    let published = machine.0.clone();
    let (tape, report) = Tape::open_with(machine, io, clock, "db", opts).unwrap();
    published.verify(tape.durable_lsn());
    (tape, report)
}

impl Oracle {
    fn reply(&mut self, batch: Batch, reply: Option<WriteResult>) {
        match reply {
            Some(Ok(Committed::Durable { first_lsn, last_lsn, .. })) => {
                assert_eq!(last_lsn - first_lsn + 1, batch.records.len() as u64);
                for (i, record) in batch.records.iter().enumerate() {
                    assert!(self
                        .durable
                        .insert(first_lsn + i as u64, record.content.clone())
                        .is_none());
                }
            }
            Some(Err(error)) if error.outcome == Outcome::NotCommitted => return,
            _ => {}
        }
        self.batches.push(batch);
    }

    fn verify<S: StateMachine>(&mut self, tape: &Tape<S>) {
        let candidates: BTreeMap<_, _> = self
            .batches
            .iter()
            .enumerate()
            .flat_map(|(b, batch)| {
                batch
                    .records
                    .iter()
                    .enumerate()
                    .map(move |(r, record)| (record.content.clone(), (b, r)))
            })
            .collect();
        let mut seen = vec![Vec::new(); self.batches.len()];
        let mut observed = BTreeMap::new();
        let mut last_txn = 0;
        for lsn in 1..=tape.durable_lsn() {
            let record = tape.read(lsn).unwrap().unwrap();
            let h = &record.header;
            observed.insert(lsn, record.payload.clone());
            assert_eq!(h.lsn, lsn, "LSNs must strictly increase");
            let &(b, r) = candidates.get(&record.payload).unwrap();
            let expected = &self.batches[b].records[r];
            assert_eq!(h.kind, expected.kind);
            assert_eq!(h.tenant_id, expected.tenant_id);
            assert_eq!(record.payload, expected.content);
            if r == 0 {
                assert_eq!(h.txn, last_txn + 1, "transactions must be dense");
                last_txn = h.txn;
            } else {
                assert_eq!(seen[b].last(), Some(&(lsn - 1, h.txn, r - 1)));
                assert_eq!(h.txn, last_txn);
            }
            seen[b].push((lsn, h.txn, r));
        }
        for (batch, seen) in self.batches.iter().zip(seen) {
            assert!(
                seen.is_empty() || seen.len() == batch.records.len(),
                "partial transaction became visible"
            );
        }
        for (lsn, digest) in &self.durable {
            assert_eq!(observed.get(lsn), Some(digest));
        }
        // Recovery syncs complete survivors. They must survive every later generation too.
        self.durable.extend(observed);
    }
}

fn enqueue<S: StateMachine>(
    core: &mut WriterCore<S>,
    records: Vec<RecordInput>,
    mode: Durability,
    _rng: &mut Rng,
) -> (Batch, mpsc::Receiver<WriteResult>) {
    let prepared = PreparedBatch::new(records.clone(), mode).unwrap();
    let (request, receiver) = WriteRequest::new(prepared);
    core.enqueue(request);
    (Batch { records }, receiver)
}

fn probe<S: StateMachine>(core: &mut WriterCore<S>, oracle: &mut Oracle, rng: &mut Rng) {
    let records = vec![RecordInput { tenant_id: 1, kind: Kind::Turn, content: rng.bytes(32) }];
    let (batch, receiver) = enqueue(core, records, Durability::Durable, rng);
    let before = core.durable_lsn();
    core.step();
    let reply = receiver.recv().unwrap();
    assert!(matches!(reply, Ok(Committed::Durable { .. })), "{reply:?}");
    assert!(core.durable_lsn() > before, "reopened writer must accept writes");
    oracle.reply(batch, Some(reply));
}

fn workload(rng: &mut Rng, tenants: usize) -> (Vec<RecordInput>, Durability) {
    let mode = Durability::Durable;
    let count = 1 + rng.below(64);
    let small = rng.below(2) == 0;
    let large = rng.below(80) == 0;
    let records = (0..count)
        .map(|i| {
            let len = if large && i == 0 {
                1024 * 1024 + rng.below(1024)
            } else if small {
                rng.below(257)
            } else {
                rng.below(8193)
            };
            let kind = [Kind::Turn, Kind::Fact, Kind::Forget][rng.below(3)];
            RecordInput {
                kind,
                tenant_id: 1 + rng.below(tenants) as u32,
                content: rng.bytes(len + 16),
            }
        })
        .collect();
    (records, mode)
}

fn damage_durable(disk: &SimDisk, dir: &DataDir, rng: &mut Rng) -> (u64, u64, u8) {
    let mut regions = Vec::new();
    let mut boundary = false;
    for &segment in dir.segments() {
        let file = disk.open(&dir.segment_path(segment)).unwrap();
        let mut bytes = vec![0; disk.len(file).unwrap() as usize];
        disk.read_at(file, 0, &mut bytes).unwrap();
        disk.close(file).unwrap();
        let mut offset = 0;
        while let Ok((record, used)) = frame::decode(&bytes[offset..], segment, offset as u64) {
            offset += used;
            if record.header.kind == Kind::Footer && record.header.lsn == dir.superblock() {
                boundary = true;
                break;
            }
        }
        regions.push((segment, if boundary { offset as u64 } else { bytes.len() as u64 }));
        if boundary {
            break;
        }
    }
    assert!(boundary && dir.superblock() > 0, "superblock position must be reachable");
    let mut offset = rng.next_u64() % regions.iter().map(|(_, len)| len).sum::<u64>();
    let bit = rng.below(8) as u8;
    for (segment, len) in regions {
        if offset < len {
            let file = disk.open(&dir.segment_path(segment)).unwrap();
            disk.flip_bit_at(file, offset, bit).unwrap();
            disk.close(file).unwrap();
            return (segment, offset, bit);
        }
        offset -= len;
    }
    unreachable!("selected offset belongs to a durable region")
}

fn capacity_faults(
    mut core: WriterCore<RecordingStateMachine>,
    io: &mut CrashIo,
    clock: &Arc<SimClock>,
    opts: &Options,
    oracle: &mut Oracle,
    rng: &mut Rng,
    plan: &CapacityPlan,
) -> (WriterCore<RecordingStateMachine>, Arc<Publications>) {
    for _ in 0..plan.delay {
        probe(&mut core, oracle, rng);
    }
    let boundary = core.durable_lsn();
    let last_txn = core.read(boundary).unwrap().map_or(0, |r| r.header.txn);
    let mut txn = last_txn;
    let records: Vec<_> = (0..1 + rng.below(4))
        .map(|_| {
            (0..1 + rng.below(4))
                .map(|_| RecordInput {
                    tenant_id: 1,
                    kind: Kind::Turn,
                    content: {
                        let len = 16 + rng.below(1024);
                        rng.bytes(len)
                    },
                })
                .collect::<Vec<_>>()
        })
        .collect();
    plan.limit(&io.disk);
    let pending: Vec<_> = records
        .iter()
        .map(|records| enqueue(&mut core, records.clone(), Durability::Durable, rng))
        .collect();
    let writes = io.hooks.tally(|op| u64::from(matches!(op, Op::Write(..)) && op.is_segment()));
    core.step();
    assert_eq!(writes.load(Ordering::SeqCst), 0, "a floor rejection wrote to a segment");
    for (_, reply) in pending {
        let error = reply.recv().unwrap().unwrap_err();
        assert!(matches!(error.kind, Error::DiskFull));
        assert_eq!(error.outcome, Outcome::NotCommitted);
    }
    assert!(!core.is_poisoned(), "a floor hit must not poison");
    assert!(core.read_only(), "a floor hit must set read-only");
    let (oldest, expected) = oracle.durable.first_key_value().unwrap();
    let capacity = io.disk.total_bytes(Path::new("db")).unwrap();
    let free = io.disk.available_bytes(Path::new("db")).unwrap();
    io.disk.set_capacity(capacity - free).unwrap();
    let reloads = core.index_stats().unwrap().page_reloads;
    assert_eq!(
        core.read(*oldest)
            .unwrap_or_else(|error| panic!("read-only cold read must not allocate: {error:?}"))
            .unwrap()
            .payload,
        *expected,
        "acknowledged record must remain readable with no free space"
    );
    if boundary > 256 && opts.lsn_index_bytes == 12 * 1024 {
        assert!(core.index_stats().unwrap().page_reloads > reloads, "sampled a cold index page");
    }
    assert_eq!(io.disk.available_bytes(Path::new("db")).unwrap(), 0);
    assert!(core.read_only(), "cold reads must not clear read-only");
    io.disk.set_capacity(capacity).unwrap();
    assert_eq!(core.durable_lsn(), boundary);
    assert_eq!(core.read(boundary).unwrap().map_or(0, |r| r.header.txn), last_txn);
    assert!(core.scan(boundary + 1).next().is_none(), "a rejected group published records");

    plan.free(&io.disk);
    let pending: Vec<_> = records
        .into_iter()
        .map(|records| enqueue(&mut core, records, Durability::Durable, rng))
        .collect();
    core.step();
    let mut lsn = boundary;
    for (batch, reply) in pending {
        let reply = reply.recv().unwrap();
        let Ok(Committed::Durable { txn: actual, first_lsn, last_lsn, .. }) = &reply else {
            panic!("freed space must admit the next group: {reply:?}");
        };
        txn += 1;
        assert_eq!(*actual, txn, "a floor rejection consumed a transaction");
        assert_eq!(*first_lsn, lsn + 1, "a floor rejection consumed an LSN");
        lsn = *last_lsn;
        oracle.reply(batch, Some(reply));
    }
    assert!(!core.read_only(), "freed space must clear read-only");
    assert!(!core.is_poisoned());

    let pending: Vec<_> = (0..2 + rng.below(3))
        .map(|_| {
            let len = 512 + rng.below(2048);
            let records =
                vec![RecordInput { tenant_id: 1, kind: Kind::Fact, content: rng.bytes(len) }];
            enqueue(&mut core, records, Durability::Durable, rng)
        })
        .collect();
    io.faults.storage_full.arm(plan);
    io.faults.cut.store(plan.cut, Ordering::SeqCst);
    let full_before = io.disk.storage_full_returns().unwrap();
    core.step();
    assert!(core.is_poisoned(), "post-prepare StorageFull must poison");
    assert_eq!(io.disk.storage_full_returns().unwrap() - full_before, 1);
    let cut = io.cut_happened();
    let mut after_reply = cut.then(|| oracle.clone());
    for (index, (batch, receiver)) in pending.into_iter().enumerate() {
        let reply = receiver.recv().unwrap();
        let error = reply.as_ref().unwrap_err();
        let (class, outcomes) = plan.expected(index);
        assert!(class.matches(&error.kind), "{error:?}");
        assert!(outcomes.contains(&error.outcome), "{error:?}");
        if let Some(after_reply) = &mut after_reply {
            after_reply.reply(batch.clone(), Some(reply.clone()));
        }
        oracle.reply(batch, if cut { None } else { Some(reply) });
    }
    io.faults.cut.store(0, Ordering::SeqCst);
    // Recover the actual post-return crash as well as any earlier captured cut.
    let crashed = io.disk.crash().unwrap();
    let image = io.faults.image.lock().unwrap().take();
    drop(core);
    if image.is_some() {
        let (tape, _) = recording_tape(Arc::new(crashed.clone()), clock.clone(), opts.clone());
        // Do not promote later survivors into acknowledgements for the earlier image.
        after_reply.unwrap().verify(&tape);
        drop(tape);
    }
    let next = image.unwrap_or(crashed);
    let (tape, _) = recording_tape(Arc::new(next.clone()), clock.clone(), opts.clone());
    oracle.verify(&tape);
    drop(tape);
    next.set_capacity(SimDisk::DEFAULT_CAPACITY).unwrap();
    *io = CrashIo::new(next);
    recording_core(io.hooks.clone(), clock.clone(), opts.clone())
}

/// Runs 1 capacity fault phase and 2 or 3 crash generations, retaining acknowledged and uncertain transactions.
pub fn run_seed(seed: u64) {
    let plan = CapacityPlan::new(seed);
    if let Err(error) = std::panic::catch_unwind(|| run_seed_with_capacity(seed, &plan)) {
        eprintln!("simulation seed={seed} fault_plan={plan:?}");
        std::panic::resume_unwind(error);
    }
}

fn run_seed_with_capacity(seed: u64, plan: &CapacityPlan) {
    frame_faults(seed);
    if seed % 97 == 1 {
        large_record(seed);
    }
    let mut rng = Rng::new(seed);
    let tenants = 1 + rng.below(8);
    let generations = 2 + rng.below(2);
    let capacity_generation = rng.below(generations);
    let opts = Options {
        segment_size: 64 * 1024,
        free_space_floor: Some(plan.floor),
        superblock_interval: std::time::Duration::from_secs(1),
        lsn_index_bytes: if seed.is_multiple_of(3) { 12 * 1024 } else { 8 * 1024 * 1024 },
        ..Options::default()
    };
    let clock = Arc::new(SimClock::default());
    let mut disk = SimDisk::new(seed);
    let mut oracle = Oracle::default();
    for generation in 0..generations {
        let mut io = CrashIo::new(disk);
        let (mut core, mut published) =
            recording_core(io.hooks.clone(), clock.clone(), opts.clone());
        probe(&mut core, &mut oracle, &mut rng);
        published.verify(core.durable_lsn());
        clock.advance(1_000_000_000);
        core.maintain().unwrap();
        if rng.below(3) == 0 {
            let records =
                vec![RecordInput { tenant_id: 1, kind: Kind::Fact, content: rng.bytes(128) }];
            let (batch, reply) = enqueue(&mut core, records, Durability::Durable, &mut rng);
            let before = core.durable_lsn();
            io.disk.fail_next_sync().unwrap();
            core.step();
            assert_eq!(core.durable_lsn(), before);
            published.verify(before);
            let error = reply.recv().unwrap().unwrap_err();
            assert!(matches!(error.kind, Error::SyncFailed(_)));
            oracle.reply(batch, Some(Err(error)));
            assert!(core.is_poisoned());
            drop(core);
            // Process restart preserves failed page-cache bytes. No crash image is taken here.
            let (tape, _) = recording_tape(io.hooks.clone(), clock.clone(), opts.clone());
            oracle.verify(&tape);
            drop(tape);
            (core, published) = recording_core(io.hooks.clone(), clock.clone(), opts.clone());
            probe(&mut core, &mut oracle, &mut rng);
            published.verify(core.durable_lsn());
            if rng.below(2) == 0 {
                clock.advance(1_000_000_000);
                core.update_superblock().unwrap();
            }
        }
        if generation == capacity_generation {
            (core, published) =
                capacity_faults(core, &mut io, &clock, &opts, &mut oracle, &mut rng, plan);
        }
        let steps = 4 + rng.below(9);
        let cut_step = rng.below(steps);
        for step in 0..steps {
            let mut pending = Vec::new();
            for _ in 0..1 + rng.below(4) {
                let (records, mode) = workload(&mut rng, tenants);
                pending.push(enqueue(&mut core, records, mode, &mut rng));
            }
            if step == cut_step {
                io.faults.cut.store(1 + rng.below(35), Ordering::SeqCst);
            }
            match rng.below(10) {
                0 => io.faults.fail_sync.store(1 + rng.below(6), Ordering::SeqCst),
                1 => io.disk.enospc_after_bytes(rng.below(80 * 1024)).unwrap(),
                _ => {}
            }
            let skip_step = step == cut_step && rng.below(8) == 0;
            if !skip_step {
                core.step();
            }
            published.verify(core.durable_lsn());
            let cut = io.cut_happened() || skip_step;
            for (batch, receiver) in pending {
                let reply = if cut { None } else { Some(receiver.recv().unwrap()) };
                if let Some(reply) = &reply {
                    published.reply(reply);
                }
                oracle.reply(batch, reply);
            }
            if cut {
                break;
            }
            clock.advance(rng.next_u64() % 1_500_000_000);
            // Cuts here land in the spare fill as well as the superblock install.
            let _ = core.maintain();
            if io.cut_happened() || core.is_poisoned() {
                break;
            }
        }
        disk = io.faults.image.lock().unwrap().take().unwrap_or_else(|| io.disk.crash().unwrap());
        drop(core);
        if seed.is_multiple_of(11) && generation + 1 == generations {
            let dir = DataDir::open(Arc::new(disk.clone()), "db", None, opts.segment_size).unwrap();
            assert!(dir.superblock() > 0);
            damage_durable(&disk, &dir, &mut rng);
            drop(dir);
            assert!(matches!(
                Tape::open(Arc::new(disk), clock, "db", opts),
                Err(Error::CorruptionDetected { .. })
            ));
            return;
        }
        if rng.below(4) == 0 {
            let recovery_io = CrashIo::new(disk.clone());
            recovery_io.faults.cut.store(1 + rng.below(25), Ordering::SeqCst);
            let (tape, _) = recording_tape(recovery_io.hooks.clone(), clock.clone(), opts.clone());
            drop(tape);
            let image = recovery_io.faults.image.lock().unwrap().take();
            if let Some(image) = image {
                disk = image;
            }
        }
        let (tape, report) = recording_tape(Arc::new(disk.clone()), clock.clone(), opts.clone());
        oracle.verify(&tape);
        assert_eq!(report.records as u64, tape.durable_lsn());
        drop(tape);
    }
    let (mut core, published) = recording_core(Arc::new(disk.clone()), clock.clone(), opts.clone());
    probe(&mut core, &mut oracle, &mut rng);
    published.verify(core.durable_lsn());
    drop(core);
    let (tape, _) = recording_tape(Arc::new(disk), clock, opts);
    oracle.verify(&tape);
}

fn large_record(seed: u64) {
    // A 64 KiB segment rejects 1 MiB batches. Also exercise accepted large records separately.
    let mut rng = Rng::new(seed);
    let opts = Options { segment_size: 2 * 1024 * 1024, ..Options::default() };
    let disk = Arc::new(SimDisk::new(seed));
    let clock = Arc::new(SimClock::default());
    let (mut core, _) =
        WriterCore::open(disk.clone(), clock.clone(), "large", opts.clone()).unwrap();
    let mut oracle = Oracle::default();
    for _ in 0..2 {
        let records =
            vec![RecordInput { kind: Kind::Fact, tenant_id: 1, content: rng.bytes(1024 * 1024) }];
        let (batch, reply) = enqueue(&mut core, records, Durability::Durable, &mut rng);
        core.step();
        let reply = reply.recv().unwrap();
        assert!(matches!(reply, Ok(Committed::Durable { .. })));
        oracle.reply(batch, Some(reply));
    }
    drop(core);
    let disk = Arc::new(disk.crash().unwrap());
    let (tape, _) = Tape::open(disk.clone(), clock.clone(), "large", opts.clone()).unwrap();
    oracle.verify(&tape);
    drop(tape);
    let (mut core, _) =
        WriterCore::open(disk.clone(), clock.clone(), "large", opts.clone()).unwrap();
    probe(&mut core, &mut oracle, &mut rng);
    drop(core);
    let (tape, _) = Tape::open(Arc::new(disk.crash().unwrap()), clock, "large", opts).unwrap();
    oracle.verify(&tape);
}

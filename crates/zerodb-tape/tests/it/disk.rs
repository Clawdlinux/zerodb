use crate::support::*;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use zerodb_tape::sim::{frame::HEADER_LEN, make_spare, DataDir, SegmentWriter, WriterCore};
use zerodb_tape::{Committed, Error, HookIo, Io, Kind, Op, Outcome, SimDisk, TapeOptions};

#[derive(Clone, Copy)]
enum Fault {
    Floor,
    IndexFloor,
    IndexOffsets,
    IndexRoutes,
    IndexStat,
    DefaultFloor(u64),
    Create,
    Allocate,
    LaterAllocate,
    Stat,
    Sync,
    DirSync,
    Manifest,
    Spare,
    SpareRename,
}

#[test]
fn space_faults_set_group_outcome() {
    use Fault::*;
    for (case, fault, expect_read_only, expect_poisoned) in [
        ("whole group floor", Floor, true, false),
        // Capacity covers the 2 MiB index reservation at open. Five percent stays under the cap.
        ("five percent floor", DefaultFloor(64 << 20), true, false),
        ("capped default floor", DefaultFloor(1 << 40), true, false),
        ("create full", Create, true, false),
        ("allocate full", Allocate, true, false),
        ("later allocation full", LaterAllocate, true, false),
        ("space query error", Stat, true, false),
        ("segment sync error", Sync, false, true),
        ("directory sync error", DirSync, false, true),
        ("manifest write full", Manifest, false, true),
        ("spare is one prepared roll", Spare, true, false),
        ("spare rename full", SpareRename, true, false),
    ] {
        space_fault(case, fault, expect_read_only, expect_poisoned);
    }
}

// Index rows prime 131072 LSNs to cross a 1 MiB backing chunk.
#[test]
#[cfg_attr(debug_assertions, ignore = "slow in debug")]
fn index_growth_space_faults_set_group_outcome() {
    use Fault::*;
    for (case, fault, expect_read_only, expect_poisoned) in [
        ("index growth floor", IndexFloor, true, false),
        ("index offsets allocation full", IndexOffsets, true, false),
        ("index routes allocation full", IndexRoutes, true, false),
        ("index growth space query error", IndexStat, true, false),
    ] {
        space_fault(case, fault, expect_read_only, expect_poisoned);
    }
}

fn turns(core: &mut WriterCore<Machine>, count: u64, size: usize) -> Vec<Committed> {
    let batch = || vec![record(Kind::Turn, 1, vec![7; size])];
    let replies: Vec<_> = (0..count).map(|_| submit(core, batch())).collect();
    core.step();
    replies.into_iter().map(|reply| reply.recv().unwrap().unwrap()).collect()
}

fn space_fault(case: &str, fault: Fault, expect_read_only: bool, expect_poisoned: bool) {
    use Fault::*;
    let capacity = if let DefaultFloor(capacity) = fault { capacity } else { 1 << 30 };
    let floor = if matches!(fault, DefaultFloor(_)) { None } else { Some(100) };
    let indexed = matches!(fault, IndexFloor | IndexOffsets | IndexRoutes | IndexStat);
    // Index backing grows in 1 MiB chunks. 131072 LSNs fill one offsets chunk and two routes
    // chunks exactly, so the next LSN grows both files. Large segments keep priming fast.
    let boundary = if indexed { 131072 } else { 1 };
    let segment = if indexed { 1 << 20 } else { 4096 };
    let opts = TapeOptions { free_space_floor: floor, ..opts(segment) };
    let index_lens = |disk: &SimDisk| {
        ["LSN-OFFSETS", "LSN-ROUTES"].map(|name| read(disk, Path::new("db").join(name)).len())
    };
    let (io, db) = (Arc::new(HookIo::sim(8)), Path::new("db"));
    let (disk, events) = (&io.inner, io.record());
    disk.set_capacity(capacity).unwrap();
    let machine = Machine::default();
    let log = machine.log.clone();
    let (mut core, _) =
        WriterCore::open_with(machine, io.clone(), clock(), "db", opts.clone()).unwrap();
    disk.set_capacity(capacity.max(16384)).unwrap();
    // Leaves 184 bytes in the first segment. Later 120-byte frames fill whole segments.
    turns(&mut core, 1, segment as usize - 296);
    while core.durable_lsn() < boundary {
        let count = (boundary - core.durable_lsn()).min(1024);
        turns(&mut core, count, 8);
    }
    disk.set_capacity(capacity).unwrap();
    let rename_full =
        |io: &HookIo| io.fail_once(ErrorKind::StorageFull, |op| matches!(op, Op::Rename(..)));
    if matches!(fault, Spare) {
        // A failed rename frees the filled space at once.
        rename_full(&io);
        assert!(make_spare(io.clone(), db, segment, 0, &Default::default()).is_err());
        assert!(!disk.exists(Path::new("db/spare.tmp")).unwrap(), "{case}: spare.tmp removed");
    }
    if matches!(fault, Spare | SpareRename) {
        make_spare(io.clone(), db, segment, 0, &Default::default()).unwrap();
    }
    let before_prepares = log.lock().unwrap().prepares;
    let before_manifest = read(disk, "db/MANIFEST");
    let before_files = disk.list(db).unwrap();
    let segments = before_files.iter().filter(|p| p.to_string_lossy().contains("/seg-")).count();
    let next_segment = segments as u64 + 1;
    let before_events = events.lock().unwrap().clone();
    if indexed {
        let allocations = before_events.iter().filter(|e| e.starts_with("allocate db/LSN-"));
        // Page-wise growth made 258 calls here, one per 1024-LSN group per file.
        assert_eq!(allocations.count(), 3, "{case}: index allocate calls while priming");
        assert_eq!(index_lens(disk), [1 << 20, 2 << 20], "{case}: primed index backing");
    }
    let written = io.tally(segment_bytes);
    let used = capacity - disk.available_bytes(db).unwrap();
    let chunk = 1 << 20;
    // A ready spare already holds the roll's blocks.
    let roll = if matches!(fault, Spare) { 0 } else { segment };
    let reserve = 240 + roll + if indexed { 2 * chunk } else { 0 };
    let floor_bytes = floor.unwrap_or((capacity / 20).min(1 << 30));
    match fault {
        Floor | IndexFloor | DefaultFloor(_) | Spare => {
            disk.set_capacity(used + floor_bytes + reserve - 1).unwrap()
        }
        Create => io.fail_once(ErrorKind::StorageFull, |op| matches!(op, Op::Create(_))),
        // Each row leaves one byte less than its failing allocation needs.
        Allocate => shrink_after_space_check(&io, used + 4095),
        LaterAllocate => shrink_after_space_check(&io, used + 8191),
        IndexOffsets => shrink_after_space_check(&io, used + chunk - 1),
        IndexRoutes => shrink_after_space_check(&io, used + 2 * chunk - 1),
        Stat | IndexStat => io.fail_once(ErrorKind::Other, |op| matches!(op, Op::Available(_))),
        Sync => io.fail_nth_sync(1, data_sync),
        DirSync => io.fail_nth_sync(1, |op| matches!(op, Op::SyncDir(_))),
        Manifest => io.fail_once(ErrorKind::StorageFull, |op| {
            op.name() == "MANIFEST.tmp" && matches!(op, Op::Write(..))
        }),
        SpareRename => rename_full(&io),
    }
    let size = if matches!(fault, LaterAllocate) { 3800 } else { 8 };
    let replies = [0; 2].map(|_| submit(&mut core, vec![record(Kind::Turn, 1, vec![7; size])]));
    core.step();
    for reply in replies {
        let error = reply.recv().unwrap().unwrap_err();
        assert_eq!(error.outcome, Outcome::NotCommitted, "{case}");
        let expected = match fault {
            Stat | IndexStat => matches!(error.kind, Error::Io(ErrorKind::Other, _)),
            Sync | DirSync => matches!(error.kind, Error::SyncFailed(_)),
            _ => matches!(error.kind, Error::DiskFull),
        };
        assert!(expected, "{case}: {error:?}");
    }
    assert_eq!(core.read_only(), expect_read_only, "{case}");
    assert_eq!(core.is_poisoned(), expect_poisoned, "{case}");
    assert_eq!(log.lock().unwrap().prepares, before_prepares, "{case}");
    assert_eq!(core.durable_lsn(), boundary, "{case}");
    assert!(core.read(1).unwrap().is_some(), "{case}");
    assert_eq!(written.load(Ordering::SeqCst), 0, "{case}: no segment writes");
    let manifest_changed = read(disk, "db/MANIFEST") != before_manifest;
    assert_eq!(manifest_changed, matches!(fault, LaterAllocate), "{case}");
    let rolled = matches!(fault, LaterAllocate | Sync | DirSync | Manifest);
    assert_eq!(disk.exists(&seg(next_segment)).unwrap(), rolled, "{case}");
    assert!(!disk.exists(&seg(next_segment + 1)).unwrap(), "{case}");
    if !rolled {
        assert_eq!(disk.list(db).unwrap(), before_files, "{case}");
    }
    if matches!(fault, Floor | IndexFloor | DefaultFloor(_) | Stat | IndexStat | Spare) {
        assert_eq!(*events.lock().unwrap(), before_events, "{case}");
    }
    let retry_size = match fault {
        DefaultFloor(_) => {
            disk.set_capacity(used + floor_bytes + 120).unwrap();
            8
        }
        // The roll fits only by taking the spare. Then a refill would cross the floor by 1 byte.
        Spare => {
            disk.set_capacity(used + floor_bytes + segment - 1).unwrap();
            3800
        }
        _ => {
            disk.set_capacity(1 << 42).unwrap();
            3800
        }
    };
    if expect_poisoned {
        let retry = commit(&mut core, vec![record(Kind::Turn, 1, [7; 8])]);
        assert!(matches!(retry.unwrap_err().kind, Error::Poisoned), "{case}");
        drop(core);
        let machine = Machine { log: log.clone(), ..Machine::default() };
        core = WriterCore::open_with(machine, io.clone(), clock(), "db", opts.clone()).unwrap().0;
        assert!(!disk.exists(&seg(2)).unwrap(), "{case}");
    }
    let Committed::Durable { txn, last_lsn, .. } = turns(&mut core, 1, retry_size)[0].clone();
    assert_eq!((txn, last_lsn), (boundary + 1, boundary + 1), "{case}");
    if indexed {
        assert_eq!(index_lens(disk), [2 << 20, 3 << 20], "{case}: one chunk per file");
    }
    assert!(!core.read_only() && !core.is_poisoned(), "{case}");
    assert_eq!(log.lock().unwrap().prepares, before_prepares + 1, "{case}");
    if matches!(fault, Spare) {
        core.maintain().unwrap();
        assert!(!disk.exists(Path::new("db/spare.zdb")).unwrap(), "{case}: refill skipped");
        // A refill would push the tail write below the floor. The next roll hits it.
        for floor_hit in [false, true] {
            let reply = commit(&mut core, vec![record(Kind::Turn, 1, [7; 8])]);
            let full = reply.map_err(|e| matches!(e.kind, Error::DiskFull));
            assert_eq!(full.err(), floor_hit.then_some(true), "{case}");
        }
    }
    let durable = core.durable_lsn();
    drop(core);
    let image = image(disk);
    image.set_capacity(1 << 42).unwrap();
    let (core, report) =
        WriterCore::open_with(Machine::default(), image, clock(), "db", opts).unwrap();
    assert_eq!(report.durable_lsn, durable, "{case}");
    // One read per 4096-byte routes page keeps the 131072-LSN rows fast in debug builds.
    let stride = if indexed { 256 } else { 1 };
    for lsn in (1..=durable).step_by(stride).chain([durable]) {
        assert!(core.read(lsn).unwrap().is_some(), "{case}, lsn={lsn}");
    }
}

#[test]
fn a_group_spanning_segments_is_reserved_before_any_prepare() {
    for (case, size, count, bytes, segments, behavior, durable_lsn) in [
        ("written rolls", 200, 3, 8, 3, Behavior::Pass, 3),
        ("unused small rolls", 200, 3, 8, 3, Behavior::Dedup, 1),
        ("unused large roll", 8 * 1024 * 1024, 3, 3 * 1024 * 1024, 2, Behavior::Dedup, 1),
        ("unused large rolls", 8 * 1024 * 1024, 7, 3 * 1024 * 1024, 4, Behavior::Dedup, 1),
    ] {
        let disk = Arc::new(SimDisk::new(5));
        let opts = TapeOptions { free_space_floor: Some(0), ..opts(size) };
        let expected_segments = Some((disk.clone(), segments));
        let machine = Machine { expected_segments, behavior, ..Machine::default() };
        let (mut core, _) =
            WriterCore::open_with(machine, disk.clone(), clock(), "db", opts.clone()).unwrap();
        turns(&mut core, count, bytes);
        assert_eq!(core.durable_lsn(), durable_lsn, "{case}");
        core.shutdown().unwrap();
        drop(core);
        for reopen in 0..3 {
            let label = format!("{case}, reopen={reopen}");
            let (mut core, report) = WriterCore::open_with(
                Machine::default(),
                disk.clone(),
                clock(),
                "db",
                opts.clone(),
            )
            .unwrap();
            let expected_lsn = durable_lsn + u64::from(reopen == 2);
            assert_eq!(report.durable_lsn, expected_lsn, "{label}");
            assert_eq!((report.truncated, report.dropped_bytes), (None, 0), "{label}");
            for lsn in 1..=expected_lsn {
                assert!(core.read(lsn).unwrap().is_some(), "{label}, lsn={lsn}");
            }
            if reopen == 1 {
                let Committed::Durable { last_lsn, .. } = turns(&mut core, 1, 8)[0].clone();
                assert_eq!(last_lsn, durable_lsn + 1, "{case}: reservations consumed no LSNs");
            }
            core.shutdown().unwrap();
        }
    }
}

#[test]
fn rewrites_cannot_exceed_the_submitted_space_budget() {
    for (case, segment_size, free, floor, submitted, rewritten, count, sparse, on_prepare) in [
        ("sparse floor growth", 8192, 8192, 4096, 120, 6112, 1, true, None),
        ("one byte growth", 8192, 8192, 4096, 120, 121, 1, true, None),
        ("growth across reserved segments", 256, 8192, 0, 120, 192, 3, false, Some(3)),
        ("same size", 256, 8192, 0, 120, 120, 3, false, None),
        ("shrinking", 256, 8192, 0, 192, 120, 3, false, None),
    ] {
        let io = Arc::new(HookIo::sim(6));
        if sparse {
            sparse_allocations(&io);
        }
        let opts = TapeOptions { free_space_floor: Some(floor), ..opts(segment_size) };
        let overhead = 2 * HEADER_LEN + 32;
        let machine = Machine::new(Behavior::Resize { len: rewritten - overhead, on_prepare });
        let log = machine.log.clone();
        let (mut core, _) =
            WriterCore::open_with(machine, io.clone(), clock(), "db", opts.clone()).unwrap();
        let (disk, dir) = (&io.inner, Path::new("db"));
        let used = disk.total_bytes(dir).unwrap() - disk.available_bytes(dir).unwrap();
        disk.set_capacity(used + free).unwrap();
        assert_eq!(disk.available_bytes(dir).unwrap(), free, "{case}: free before prepare");
        let written = io.tally(segment_bytes);
        let batch = || vec![record(Kind::Turn, 1, vec![7; submitted - overhead])];
        let replies: Vec<_> = (0..count).map(|_| submit(&mut core, batch())).collect();
        core.step();
        let available = disk.available_bytes(dir).unwrap();
        assert!(available >= floor, "{case}: free={available} consumed protected floor={floor}");
        let growth = rewritten > submitted;
        let durable = if growth { on_prepare.unwrap_or(1) - 1 } else { count };
        let durable_size = if growth { submitted } else { rewritten };
        for (index, reply) in replies.into_iter().enumerate() {
            let label = format!("{case}: request={index}");
            match reply.recv().unwrap() {
                Ok(Committed::Durable { txn, first_lsn, last_lsn, .. }) => {
                    assert!(index < durable, "{label}");
                    assert_eq!([txn, first_lsn, last_lsn], [index as u64 + 1; 3], "{label}");
                }
                Err(error) if index == durable => {
                    assert_eq!(error.outcome, Outcome::NotCommitted, "{label}");
                    let message = format!(
                        "prepared batch grew from {submitted} to {rewritten} bytes (1 records)"
                    );
                    assert!(
                        matches!(&error.kind, Error::InvalidFrame(m) if *m == message),
                        "{label}"
                    );
                }
                Err(error) => {
                    assert!(index > durable && error.outcome == Outcome::NotCommitted, "{label}");
                    assert!(matches!(error.kind, Error::Poisoned), "{label}: {error:?}");
                }
            }
        }
        assert_eq!(core.is_poisoned(), growth, "{case}");
        assert_eq!(core.durable_lsn(), durable as u64, "{case}");
        let records = core.scan(0).collect::<zerodb_tape::Result<Vec<_>>>().unwrap();
        assert_eq!(records.len(), durable, "{case}");
        assert_eq!(log.lock().unwrap().published, records, "{case}");
        let content = vec![7; durable_size - overhead];
        assert!(records.iter().all(|r| r.payload.as_ref() == content), "{case}: durable content");
        let prepares = if growth { durable + 1 } else { count };
        assert_eq!(log.lock().unwrap().prepares, prepares, "{case}");
        let bytes = (durable * durable_size) as u64;
        assert_eq!(written.load(Ordering::SeqCst), bytes, "{case}: only durable writes");
        if growth {
            if sparse {
                assert_eq!(available, free, "{case}: invalid growth consumed sparse blocks");
            }
            let beyond = disk.exists(&seg(3)).unwrap();
            assert!(!beyond, "{case}: no allocation beyond the preflight reservation");
            let next = commit(&mut core, vec![record(Kind::Turn, 1, [7; 8])]);
            assert!(matches!(next.unwrap_err().kind, Error::Poisoned), "{case}");
            let prepared = log.lock().unwrap().prepares;
            assert_eq!(prepared, prepares, "{case}: later append did not prepare");
            assert_eq!(written.load(Ordering::SeqCst), bytes, "{case}: no later write");
        }
        drop(core);
        let reopened = WriterCore::open(image(disk), clock(), "db", opts);
        let (reopened, report) = reopened.unwrap_or_else(|error| panic!("{case}: {error}"));
        assert_eq!(report.durable_lsn, durable as u64, "{case}: acknowledged prefix survives");
        let survivors = reopened.scan(0).collect::<zerodb_tape::Result<Vec<_>>>().unwrap();
        assert_eq!(survivors, records, "{case}: only acknowledged records survive reopen");
    }
}

#[test]
fn roll_prepares_names_and_syncs_every_dirty_file() {
    // A sparse fallback reserves nothing, so no spare is made.
    let sparse = Arc::new(HookIo::sim(0));
    sparse_allocations(&sparse);
    DataDir::open(sparse.clone(), "db", None, 512).unwrap();
    make_spare(sparse.clone(), Path::new("db"), 512, 0, &Mutex::new(())).unwrap();
    for name in ["db/spare.zdb", "db/spare.tmp"] {
        assert!(!sparse.inner.exists(Path::new(name)).unwrap(), "sparse: {name}");
    }
    for (case, spare) in [("preallocated roll", false), ("spare roll", true)] {
        for seed in 0..100 {
            let io = Arc::new(SimDisk::new(seed));
            let dir = DataDir::open(io.clone(), "db", None, 512).unwrap();
            let mut writer = SegmentWriter::new(dir, 512, 0).unwrap();
            if spare {
                let used = SimDisk::DEFAULT_CAPACITY - io.available_bytes("db".as_ref()).unwrap();
                for (room, made) in [(2 * 512 - 1, false), (2 * 512, true)] {
                    io.set_capacity(used + 100 + room).unwrap();
                    make_spare(io.clone(), Path::new("db"), 512, 100, &Mutex::new(())).unwrap();
                    assert_eq!(io.exists("db/spare.zdb".as_ref()).unwrap(), made, "{case}");
                }
                io.set_capacity(SimDisk::DEFAULT_CAPACITY).unwrap();
            }
            assert_eq!(writer.write_batch(&[7; 300]).unwrap(), (1, 0), "{case}");
            assert_eq!(writer.write_batch(&[8; 300]).unwrap(), (2, 0), "{case}");
            assert!(!io.exists(Path::new("db/spare.zdb")).unwrap(), "{case}: spare taken");
            let oversized = writer.write_batch(&[0; 513]);
            assert!(matches!(oversized, Err(Error::BatchTooLarge)), "{case}");
            assert!(!io.exists(&seg(3)).unwrap(), "{case}: an oversized batch never rolls");
            writer.sync().unwrap();
            drop(writer);
            let crash = image(&io);
            assert_eq!(
                DataDir::open(crash.clone(), "db", None, 512).unwrap().segments(),
                &[1, 2],
                "{case}"
            );
            for (id, byte) in [(1, 7), (2, 8)] {
                let bytes = read(&*crash, seg(id));
                assert_eq!(bytes.len(), 512, "{case}, id={id}");
                assert_eq!(
                    (&bytes[..300], &bytes[300..]),
                    (&[byte; 300][..], &[0; 212][..]),
                    "{case}"
                );
            }
        }
    }
}

#[test]
fn failed_segment_preparation_keeps_the_manifest() {
    for (case, sync_fails) in [("allocation full", false), ("sync failure", true)] {
        let (disk, db) = (Arc::new(SimDisk::new(2)), Path::new("db"));
        let mut dir = DataDir::open(disk.clone(), "db", None, 4096).unwrap();
        let free = disk.available_bytes(db).unwrap();
        dir.prepare_next_segment(4096).unwrap();
        assert_eq!(free - disk.available_bytes(db).unwrap(), 4096, "{case}: preallocated");
        let before = dir.segments().to_vec();
        let total = disk.total_bytes(db).unwrap();
        if sync_fails {
            disk.fail_next_sync().unwrap();
        } else {
            disk.set_capacity(total - disk.available_bytes(db).unwrap() + 4095).unwrap();
        }
        let error = dir.prepare_next_segment(4096).unwrap_err();
        let kind_ok = match error {
            Error::Io(ErrorKind::StorageFull, _) => !sync_fails,
            Error::SyncFailed(_) => sync_fails,
            _ => false,
        };
        assert!(kind_ok, "{case}: {error:?}");
        assert_eq!(dir.segments(), before, "{case}");
        drop(dir);
        disk.set_capacity(total).unwrap();
        let reopened = DataDir::open(disk, "db", None, 4096).unwrap();
        assert_eq!(reopened.segments(), before, "{case}: manifest unchanged");
    }
}

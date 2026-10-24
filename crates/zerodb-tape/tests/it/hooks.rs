use crate::support::*;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::time::Duration;
use zerodb_tape::sim::WriterCore;
use zerodb_tape::{
    Committed, Durability, Error, HookIo, Io, Kind, Outcome, Result, SimDisk, Tape, TapeOptions,
};

fn turn(content: &[u8]) -> Vec<zerodb_tape::RecordInput> {
    vec![record(Kind::Turn, 7, content)]
}

#[test]
fn concurrent_appends_are_dense_published_before_reply_and_share_syncs() {
    // (case, segment size, threads, appends per thread, records per append, sync delay ms)
    for (case, size, threads, appends, records, delay) in
        [("rolling segments", 512, 8, 16, 2, 0), ("slow group sync", 65536, 16, 1, 1, 20)]
    {
        let io = Arc::new(HookIo::sim(1));
        let machine = Machine::new(Behavior::Sequence);
        let log = machine.log.clone();
        let (tape, _) = Tape::open_with(machine, io.clone(), clock(), "db", opts(size)).unwrap();
        let syncs = io.tally(|op| data_sync(op) as u64);
        io.add(move |op, next| {
            if data_sync(&op) {
                std::thread::sleep(Duration::from_millis(delay));
            }
            next()
        });
        let barrier = Barrier::new(threads);
        let append = || {
            let batch = vec![record(Kind::Turn, 7, [0; 8]); records];
            let Committed::Durable { txn, first_lsn, last_lsn, .. } =
                tape.append(batch, Durability::Durable).unwrap();
            assert_eq!(last_lsn - first_lsn + 1, records as u64, "{case}");
            let published = log.lock().unwrap().published.clone();
            for lsn in first_lsn..=last_lsn {
                let count = published.iter().filter(|r| r.lsn == lsn).count();
                assert_eq!(count, 1, "{case}: lsn={lsn} published once before its reply");
            }
            let read = tape.read(last_lsn).unwrap().unwrap().payload;
            assert_eq!(read, last_lsn.to_le_bytes(), "{case}: read after ack");
            txn
        };
        let mut txns: Vec<_> = std::thread::scope(|scope| {
            let worker = || {
                barrier.wait();
                (0..appends).map(|_| append()).collect::<Vec<_>>()
            };
            let workers: Vec<_> = (0..threads).map(|_| scope.spawn(worker)).collect();
            workers.into_iter().flat_map(|worker| worker.join().unwrap()).collect()
        });
        txns.sort();
        let total = threads * appends;
        assert_eq!(txns, (1..=total as u64).collect::<Vec<_>>(), "{case}: dense transactions");
        let scanned = tape.scan(0).collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(scanned.len(), total * records, "{case}");
        for (record, lsn) in scanned.iter().zip(1u64..) {
            assert_eq!((record.lsn, record.tenant_id, record.kind), (lsn, 7, Kind::Turn), "{case}");
            assert_eq!(record.payload.as_ref(), &lsn.to_le_bytes(), "{case}");
        }
        let log = log.lock().unwrap();
        assert_eq!(log.published, scanned, "{case}");
        assert_eq!(log.prepares, total, "{case}");
        assert!(log.threads.iter().all(|id| *id == log.threads[0]), "{case}: one writer thread");
        assert_ne!(log.threads[0], std::thread::current().id(), "{case}");
        let syncs = syncs.load(Ordering::SeqCst);
        assert!(delay == 0 || syncs < threads as u64, "{case}: callers share syncs={syncs}");
    }
}

#[test]
fn reject_and_cached_reply_write_nothing() {
    let machine = Machine::new(Behavior::Sequence);
    let log = machine.log.clone();
    let disk = Arc::new(SimDisk::new(2));
    let (tape, _) = Tape::open_with(machine, disk, clock(), "db", opts(512)).unwrap();
    let error = tape.append(turn(b"reject"), Durability::Durable).unwrap_err();
    assert_eq!(error.outcome, Outcome::NotCommitted);
    let error =
        tape.append_with_digest(turn(&[0; 8]), Durability::Durable, None, Some(1)).unwrap_err();
    assert_eq!(error.outcome, Outcome::NotCommitted, "the machine sees the unframed argument");
    assert_eq!(tape.append(turn(b"reply"), Durability::Durable).unwrap(), receipt(77, 88));
    assert_eq!((tape.durable_lsn(), tape.scan(0).count()), (0, 0));
    assert!(log.lock().unwrap().published.is_empty());
    let committed = tape.append(turn(&[0; 8]), Durability::Durable).unwrap();
    assert!(matches!(committed, Committed::Durable { txn: 1, first_lsn: 1, last_lsn: 1, .. }));
    let images: Vec<_> = [None, Some(7)]
        .into_iter()
        .map(|arg| {
            let disk = Arc::new(SimDisk::new(2));
            let (tape, _) = Tape::open(disk.clone(), clock(), "db", opts(512)).unwrap();
            tape.append_with_digest(turn(b"same"), Durability::Durable, None, arg).unwrap();
            drop(tape);
            files(&*disk)
        })
        .collect();
    assert!(!images[0].is_empty());
    assert_eq!(images[0], images[1], "the argument is never framed");
}

#[test]
fn durable_records_publish_each_step_and_shutdown_is_idempotent() {
    let machine = Machine::new(Behavior::Sequence);
    let log = machine.log.clone();
    let disk = Arc::new(SimDisk::new(3));
    let (mut core, _) = WriterCore::open_with(machine, disk, clock(), "db", opts(2048)).unwrap();
    for lsn in 1..=5 {
        commit(&mut core, turn(&[0; 8])).unwrap();
        let published: Vec<_> = log.lock().unwrap().published.iter().map(|r| r.lsn).collect();
        assert_eq!(published, (1..=lsn).collect::<Vec<_>>());
        assert_eq!(core.scan(0).count() as u64, lsn);
    }
    for _ in 0..2 {
        core.shutdown().unwrap();
        assert_eq!(log.lock().unwrap().published.len(), 5);
    }
}

#[test]
fn replay_is_ordered_exclusive_of_watermark_and_finishes_before_writes() {
    let (disk, clock) = (Arc::new(SimDisk::new(4)), clock());
    let open = |machine, from| {
        let opts = TapeOptions { replay_from_lsn: from, ..opts(512) };
        Tape::open_with(machine, disk.clone(), clock.clone(), "db", opts)
    };
    let (tape, _) = open(Machine::new(Behavior::Sequence), 0).unwrap();
    for _ in 0..5 {
        tape.append(turn(&[0; 8]), Durability::Durable).unwrap();
    }
    let all = tape.scan(0).collect::<Result<Vec<_>>>().unwrap();
    assert_eq!(all.iter().map(|r| r.lsn).collect::<Vec<_>>(), [1, 2, 3, 4, 5], "no footers");
    assert_eq!(tape.scan(3).collect::<Result<Vec<_>>>().unwrap(), all[2..]);
    assert_eq!(tape.scan(6).count(), 0);
    let mut end = tape.scan(u64::MAX);
    assert!(end.next().is_none() && end.next().is_none());
    drop(end);
    let snapshot = tape.scan(0);
    tape.append(turn(&[0; 8]), Durability::Durable).unwrap();
    assert_eq!(snapshot.collect::<Result<Vec<_>>>().unwrap(), all);
    drop(tape);
    for watermark in [0, 3, 6, u64::MAX] {
        let machine = Machine { seq: watermark.min(6), ..Machine::new(Behavior::Sequence) };
        let log = machine.log.clone();
        drop(open(machine, watermark).unwrap());
        let log = log.lock().unwrap();
        let replayed: Vec<_> = log.replayed.iter().map(|r| r.lsn).collect();
        let expected: Vec<_> = (1..=6).filter(|lsn| *lsn > watermark).collect();
        assert_eq!(replayed, expected, "watermark={watermark}");
        assert!(log.published.is_empty(), "watermark={watermark}");
    }
    // A failed replay aborts open and releases the lock.
    let failing = Machine { fail_replay: true, ..Machine::new(Behavior::Sequence) };
    assert!(matches!(open(failing, 0), Err(Error::Configuration(_))));
    let (tape, _) = open(Machine { seq: 3, ..Machine::new(Behavior::Sequence) }, 3).unwrap();
    tape.append(turn(&[0; 8]), Durability::Durable).unwrap();
    assert_eq!(tape.scan(7).next().unwrap().unwrap().payload.as_ref(), &7u64.to_le_bytes());
}

#[test]
fn rewritten_batches_are_revalidated_without_exposing_reserved_state() {
    for case in ["footer", "too large"] {
        let machine = Machine::new(Behavior::InvalidRewrite);
        let disk = Arc::new(SimDisk::new(9));
        let (tape, _) = Tape::open_with(machine, disk, clock(), "db", opts(512)).unwrap();
        let error = tape.append(turn(case.as_bytes()), Durability::Durable).unwrap_err();
        assert_eq!(error.outcome, Outcome::NotCommitted, "{case}");
        assert_eq!((tape.durable_lsn(), tape.scan(0).count()), (0, 0), "{case}");
        let next = tape.append(turn(b"next"), Durability::Durable).unwrap_err();
        assert!(matches!(next.kind, Error::Poisoned), "{case}");
    }
}

#[test]
fn scan_reports_corruption_and_stops_after_the_bad_frame() {
    let disk = Arc::new(SimDisk::new(7));
    let (tape, _) = Tape::open(disk.clone(), clock(), "db", opts(160)).unwrap();
    for _ in 0..3 {
        tape.append(turn(&[0; 8]), Durability::Durable).unwrap();
    }
    let file = disk.open(&seg(2)).unwrap();
    disk.flip_bit(file, 0).unwrap();
    disk.close(file).unwrap();
    let mut scan = tape.scan(0);
    assert_eq!(scan.next().unwrap().unwrap().lsn, 1);
    assert!(matches!(scan.next().unwrap(), Err(Error::CorruptionDetected { lsn: 2, .. })));
    assert!(scan.next().is_none());
}

#[test]
fn grouped_retries_share_sync_publication_and_partial_write_outcomes() {
    for (case, partial, fail_sync) in [
        ("complete group", false, false),
        ("complete group sync failure", false, true),
        ("partial tail", true, false),
        ("partial tail sync failure", true, true),
    ] {
        let disk = Arc::new(SimDisk::new(1));
        let machine = Machine::new(Behavior::Dedup);
        let (replies, log) = (machine.replies.clone(), machine.log.clone());
        let (mut core, _) =
            WriterCore::open_with(machine, disk.clone(), clock(), "db", opts(2048)).unwrap();
        for _ in 0..2 {
            let reply = submit(&mut core, vec![record(Kind::Turn, 1, b"original")]);
            replies.lock().unwrap().push(reply);
        }
        let trailing = partial.then(|| submit(&mut core, vec![record(Kind::Turn, 1, b"trailing")]));
        if partial {
            disk.enospc_after_bytes(125).unwrap();
        }
        if fail_sync {
            disk.fail_next_sync().unwrap();
        }
        core.step();
        for reply in replies.lock().unwrap().iter() {
            match reply.recv().unwrap() {
                Ok(committed) => assert!(!fail_sync && committed == receipt(1, 1), "{case}"),
                Err(error) => {
                    assert!(fail_sync && error.outcome == Outcome::Unknown, "{case}");
                    assert!(partial || matches!(error.kind, Error::SyncFailed(_)), "{case}");
                }
            }
        }
        if let Some(reply) = trailing {
            assert!(reply.recv().unwrap().is_err(), "{case}");
        }
        assert_eq!(core.is_poisoned(), partial || fail_sync, "{case}");
        let published: Vec<_> = log.lock().unwrap().published.iter().map(|r| r.lsn).collect();
        assert_eq!(published, if fail_sync { vec![] } else { vec![1] }, "{case}");
    }
}

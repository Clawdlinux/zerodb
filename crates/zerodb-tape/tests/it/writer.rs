use crate::support::*;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use zerodb_tape::sim::{PreparedBatch, SimClock, WriterCore};
use zerodb_tape::{
    Clock, Committed, Digest, Durability, Error, FileId, Frame, Header, HookIo, Io, Kind,
    LockGuard, Op, Outcome, RecoveryReport, SimDisk, StdIo, SystemClock, Tape,
    TapeOptions as Options, WriteError, WriteResult,
};

#[test]
fn byte_threshold_and_segment_roll_install_the_superblock() {
    for (case, size, budget, count) in
        [("byte threshold", 4096, 300, 3), ("segment roll", 200, 256 * 1024 * 1024, 2)]
    {
        let io = Arc::new(HookIo::sim(35));
        let events = io.record();
        let (mut core, _) = open(io.clone(), Options { superblock_bytes: budget, ..opts(size) });
        let opened = std::mem::take(&mut *events.lock().unwrap());
        let first = "rename db/spare.zdb db/seg-0000000000000001.zdb".to_owned();
        assert!(opened.contains(&first), "{case}: first segment zero-filled: {opened:?}");
        for lsn in 1..=count {
            commit(&mut core, hello()).unwrap();
            assert!(core.read(lsn).unwrap().is_some(), "{case}, ack={lsn}");
            core.update_superblock().unwrap();
            let boundary = if lsn == count { count } else { 0 };
            assert_eq!(superblock(&io.inner, size), boundary, "{case}, lsn={lsn}");
        }
        assert_eq!(superblock_installs(&events), 1, "{case}");
        drop(core);
        assert_eq!(io.open_files(), 0, "{case}");
    }
}

#[test]
fn clock_threshold_rate_limits_superblock_installs_off_the_reply_path() {
    let (io, clock) = (Arc::new(HookIo::sim(35)), clock());
    let (mut core, _) = WriterCore::open(io.clone(), clock.clone(), "db", opts(4096)).unwrap();
    let (syncs, events) = (io.tally(|op| data_sync(op) as u64), io.record());
    for (case, lsn, advance, boundary, syncs_now) in [
        ("before threshold", 1, 29_999_999_999, 0, 1),
        ("at threshold", 2, 1, 2, 2),
        ("rate limited", 3, 0, 2, 4),
    ] {
        commit(&mut core, hello()).unwrap();
        assert!(core.read(lsn).unwrap().is_some(), "{case}");
        assert_eq!(syncs.load(Ordering::SeqCst), syncs_now, "{case}: reply path");
        clock.advance(advance);
        core.update_superblock().unwrap();
        assert_eq!(superblock(&io.inner, 4096), boundary, "{case}");
    }
    assert_eq!(syncs.load(Ordering::SeqCst), 4);
    assert_eq!(superblock_installs(&events), 1);
    drop(core);
    assert_eq!(io.open_files(), 0);
}

#[test]
fn superblock_sync_failures_preserve_acknowledgements_and_poison_writes() {
    for (case, at_shutdown, durable_lsn) in
        [("metadata sync failure", false, 1), ("shutdown sync failure", true, 0)]
    {
        let (io, clock) = (Arc::new(HookIo::sim(35)), clock());
        let (mut core, _) = WriterCore::open(io.clone(), clock.clone(), "db", opts(4096)).unwrap();
        let (syncs, events) = (io.tally(|op| data_sync(op) as u64), io.record());
        let reply = submit(&mut core, hello());
        if at_shutdown {
            io.inner.fail_next_sync().unwrap();
            assert!(matches!(core.shutdown(), Err(Error::Poisoned)), "{case}");
            assert_eq!(reply.recv().unwrap().unwrap_err().outcome, Outcome::Unknown, "{case}");
            let before = syncs.load(Ordering::SeqCst);
            assert!(matches!(core.shutdown(), Err(Error::Poisoned)), "{case}");
            assert_eq!(syncs.load(Ordering::SeqCst), before, "{case}: no sync after poison");
        } else {
            core.step();
            reply.try_recv().unwrap().unwrap();
            clock.advance(30_000_000_000);
            io.inner.fail_next_sync().unwrap();
            assert!(core.update_superblock().is_err(), "{case}");
            assert!(core.read(1).unwrap().is_some(), "{case}: acknowledgement survives");
        }
        assert!(core.is_poisoned(), "{case}");
        assert_eq!(core.durable_lsn(), durable_lsn, "{case}");
        assert_eq!(superblock_installs(&events), 0, "{case}");
        drop(core);
        assert_eq!(superblock(&io.inner, 4096), 0, "{case}");
        assert_eq!(io.open_files(), 0, "{case}");
    }
}

#[test]
fn open_and_shutdown_install_superblocks_with_bounded_rewrites() {
    for (case, payload_len, sessions, bounded) in
        [("preallocated tails", 5, 8, false), ("full segments", 912, 16, true)]
    {
        let (io, clock) = (Arc::new(HookIo::sim(0)), clock());
        let written = io.tally(segment_bytes);
        let segment_syncs = io.tally(|op| (op.is_segment() && data_sync(op)) as u64);
        let events = io.record();
        let mut previous = 0;
        for session in 0..sessions {
            let label = format!("{case}, session={session}");
            segment_syncs.store(0, Ordering::SeqCst);
            let (tape, report) = Tape::open(io.clone(), clock.clone(), "db", opts(1024)).unwrap();
            assert_eq!(report.durable_lsn, session, "{label}");
            let rewritten = written.swap(0, Ordering::SeqCst);
            if bounded {
                assert!(rewritten <= previous, "{label}, rewritten={rewritten}");
                let syncs = segment_syncs.load(Ordering::SeqCst);
                assert!(session == 0 || syncs == 0, "{label}, syncs={syncs}");
            }
            let content = vec![session as u8; payload_len];
            tape.append(vec![record(Kind::Turn, 1, &content)], Durability::Durable).unwrap();
            assert_eq!(tape.durable_lsn(), session + 1, "{label}");
            previous = written.load(Ordering::SeqCst);
            assert_eq!(previous, (payload_len + 112) as u64, "{label}");
            events.lock().unwrap().clear();
            drop(tape);
            assert!(superblock_installs(&events) > 0, "{label}");
            // Open fills a spare. Shutdown joins the worker, which refills after each roll.
            assert!(io.inner.exists(Path::new("db/spare.zdb")).unwrap(), "{label}");
            assert_eq!(superblock(&io.inner, 1024), session + 1, "{label}");
            let image = image(&io.inner);
            let (tape, report) =
                Tape::open(image.clone(), clock.clone(), "db", opts(1024)).unwrap();
            assert_eq!(report.durable_lsn, session + 1, "{label}");
            assert_eq!(tape.read(session + 1).unwrap().unwrap().payload, content, "{label}");
            drop(tape);
            let file = image.open(&seg(1)).unwrap();
            image.flip_bit(file, 0).unwrap();
            image.close(file).unwrap();
            let damaged = Tape::open(image, clock.clone(), "db", opts(1024));
            let protected = matches!(damaged, Err(Error::CorruptionDetected { .. }));
            assert!(protected, "{label}: clean-session data stays protected");
            clock.advance(500_000_000);
        }
        let (mut core, _) = WriterCore::open(io.clone(), clock.clone(), "db", opts(1024)).unwrap();
        commit(&mut core, hello()).unwrap();
        drop(core);
        events.lock().unwrap().clear();
        let (core, report) = WriterCore::open(io.clone(), clock, "db", opts(1024)).unwrap();
        assert_eq!(report.durable_lsn, sessions + 1, "{case}: recovered survivor");
        assert_eq!(superblock(&io.inner, 1024), sessions + 1, "{case}");
        assert_eq!(&core.read(sessions + 1).unwrap().unwrap().payload, b"hello", "{case}");
        let events = events.lock().unwrap();
        let first = |prefix: &str| events.iter().position(|e| e.starts_with(prefix)).unwrap();
        let (rewritten, synced) = (first("write db/seg-"), first("sync db/seg-"));
        let installed = first("rename db/SUPERBLOCK.tmp db/SUPERBLOCK");
        let recovered_dir = events[..installed].iter().rposition(|e| e == "dir-sync db").unwrap();
        let final_dir = events.iter().rposition(|e| e == "dir-sync db").unwrap();
        assert!(rewritten < synced && synced < recovered_dir, "{case}");
        assert!(recovered_dir < installed && installed < final_dir, "{case}");
    }
}

#[test]
fn live_restart_after_a_failed_sync_rewrites_survivors_before_acknowledging() {
    // (case, seeds, fail a recovery sync once, damage the tail behind an acknowledged write)
    for (case, seeds, failed_recovery, tail) in [
        ("failed write sync", 200, false, false),
        ("failed write sync and recovery sync", 32, true, false),
        ("failed tail repair sync", 32, true, true),
    ] {
        for (seed, advance_superblock) in (0..seeds).flat_map(|s| [(s, false), (s, true)]) {
            let label = format!("{case}, seed={seed}, superblock={advance_superblock}");
            let (io, clock) = (Arc::new(HookIo::sim(seed)), clock());
            let open = || {
                let machine = Machine::default();
                let log = machine.log.clone();
                (WriterCore::open_with(machine, io.clone(), clock.clone(), "db", opts(1024)), log)
            };
            let (core, log) = open();
            let mut core = core.unwrap().0;
            if tail {
                commit(&mut core, hello()).unwrap();
                clock.advance(30_000_000_000);
                core.update_superblock().unwrap();
                drop(core);
                wire::write(&io.inner, 1, 900, &[1; 100]);
            } else {
                io.inner.fail_next_sync().unwrap();
                let error = commit(&mut core, hello()).unwrap_err();
                assert!(matches!(error.kind, Error::SyncFailed(_)), "{label}");
                assert_eq!(error.outcome, Outcome::Unknown, "{label}");
                assert!(core.is_poisoned(), "{label}");
                assert!(log.lock().unwrap().published.is_empty(), "{label}");
                drop(core);
            }
            // Process restarts only. Failed bytes stay readable through the live page cache.
            if failed_recovery {
                io.fail_nth_sync(1, data_sync);
                let (core, log) = open();
                assert!(core.is_err(), "{label}: failed recovery sync aborts open");
                assert!(log.lock().unwrap().replayed.is_empty(), "{label}: replay after sync");
            }
            let (core, log) = open();
            let (mut core, report) = core.unwrap();
            assert_eq!(report.durable_lsn, 1, "{label}");
            let survivors = core.scan(0).collect::<zerodb_tape::Result<Vec<_>>>().unwrap();
            assert_eq!(log.lock().unwrap().replayed, survivors, "{label}");
            for lsn in 2..=4 {
                let Committed::Durable { first_lsn, .. } = commit(&mut core, hello()).unwrap();
                assert_eq!(first_lsn, lsn, "{label}");
            }
            if advance_superblock {
                clock.advance(30_000_000_000);
                core.update_superblock().unwrap();
            }
            drop(core);
            let (tape, report) = Tape::open(image(&io.inner), clock, "db", opts(1024)).unwrap();
            assert_eq!(report.durable_lsn, 4, "{label}");
            for lsn in 1..=4 {
                assert_eq!(&tape.read(lsn).unwrap().unwrap().payload, b"hello", "{label}");
            }
        }
    }
}

#[test]
fn live_restart_rewrites_across_rolls_and_before_a_trimmed_tail() {
    let (io, clock) = (Arc::new(HookIo::sim(0)), clock());
    let (mut core, _) = WriterCore::open(io.clone(), clock.clone(), "db", opts(435)).unwrap();
    commit(&mut core, hello()).unwrap();
    clock.advance(30_000_000_000);
    core.update_superblock().unwrap();
    let replies: Vec<_> = (0..3).map(|_| submit(&mut core, hello())).collect();
    // The roll performs 4 preparation syncs before the first dirty-segment sync.
    io.fail_nth_sync(3, data_sync);
    core.step();
    for reply in replies {
        assert_eq!(reply.recv().unwrap().unwrap_err().outcome, Outcome::Unknown);
    }
    drop(core);
    let file = io.open(&seg(2)).unwrap();
    io.write_at(file, 117, &[1; 8]).unwrap();
    io.close(file).unwrap();
    let events = io.record();
    let (mut core, report) = WriterCore::open(io.clone(), clock.clone(), "db", opts(435)).unwrap();
    assert_eq!(report.durable_lsn, 4);
    assert!(report.truncated.is_some());
    let wrote = |event: &str| events.lock().unwrap().iter().any(|e| e == event);
    assert!(wrote("write db/seg-0000000000000001.zdb 117"));
    assert!(wrote("write db/seg-0000000000000002.zdb 0"));
    assert!(!wrote("write db/seg-0000000000000001.zdb 0"));
    commit(&mut core, hello()).unwrap();
    drop(core);
    let (tape, report) = Tape::open(image(&io.inner), clock, "db", opts(435)).unwrap();
    assert_eq!(report.durable_lsn, 5);
    for lsn in 1..=5 {
        assert_eq!(&tape.read(lsn).unwrap().unwrap().payload, b"hello");
    }
}

#[test]
fn failed_group_sync_is_unknown_unpublished_and_poisons() {
    // (case, segment size, acknowledged first, failing data sync)
    for (case, size, acked, nth) in [("one segment", 4096, 2, 1), ("group spans rolls", 128, 0, 6)]
    {
        for seed in 0..64 {
            let eight = || vec![record(Kind::Turn, 1, [0; 8])];
            let label = format!("{case}, seed={seed}");
            let io = Arc::new(HookIo::sim(seed));
            let machine = Machine::new(Behavior::Sequence);
            let log = machine.log.clone();
            let (mut core, _) =
                WriterCore::open_with(machine, io.clone(), clock(), "db", opts(size)).unwrap();
            for lsn in 1..=acked {
                commit(&mut core, eight()).unwrap();
                assert!((1..=lsn).all(|l| core.read(l).unwrap().is_some()), "{label}, ack={lsn}");
            }
            io.fail_nth_sync(nth, data_sync);
            let replies: Vec<_> = (0..3).map(|_| submit(&mut core, eight())).collect();
            core.step();
            for reply in replies {
                let error = reply.recv().unwrap().unwrap_err();
                assert!(matches!(error.kind, Error::SyncFailed(_)), "{label}: {error:?}");
                assert_eq!(error.outcome, Outcome::Unknown, "{label}");
            }
            assert!(core.is_poisoned(), "{label}");
            assert_eq!(core.durable_lsn(), acked, "{label}");
            assert_eq!(core.scan(0).count() as u64, acked, "{label}");
            for lsn in 1..=acked + 3 {
                assert_eq!(core.read(lsn).unwrap().is_some(), lsn <= acked, "{label}, lsn={lsn}");
            }
            let error = commit(&mut core, eight()).unwrap_err();
            assert!(matches!(error.kind, Error::Poisoned), "{label}");
            assert_eq!(error.outcome, Outcome::NotCommitted, "{label}");
            assert!(core.shutdown().is_err(), "{label}");
            let log = log.lock().unwrap();
            assert_eq!(log.prepares as u64, acked + 3, "{label}: poisoned writes never prepare");
            assert_eq!(log.published.len() as u64, acked, "{label}");
            drop(core);
            assert_eq!(io.open_files(), 0, "{label}");
        }
    }
}

#[test]
fn invalid_and_oversized_batches_are_rejected_without_poisoning() {
    let sized = |len| vec![record(Kind::Turn, 1, vec![0; len])];
    for (case, batch, oversized) in [
        ("too many records", vec![record(Kind::Turn, 1, b"hello"); 1025], true),
        ("frame exceeds byte limit", sized(4 * 1024 * 1024), true),
        ("caller supplied footer", vec![record(Kind::Footer, 1, b"hello")], false),
        ("empty batch", vec![], false),
    ] {
        let error = PreparedBatch::new(batch, Durability::Durable).unwrap_err();
        assert_eq!(error.outcome, Outcome::NotCommitted, "{case}");
        assert_eq!(matches!(error.kind, Error::BatchTooLarge), oversized, "{case}");
    }
    let disk = Arc::new(SimDisk::new(0));
    let (mut core, _) = open(disk.clone(), opts(512));
    let error = commit(&mut core, sized(1000)).unwrap_err();
    assert!(matches!(error.kind, Error::BatchTooLarge), "{error:?}");
    assert_eq!(error.outcome, Outcome::NotCommitted);
    assert!(!core.is_poisoned());
    assert!(!disk.exists(&seg(2)).unwrap(), "a batch larger than a segment never rolls");
    commit(&mut core, hello()).unwrap();
}

#[test]
fn one_step_group_shares_one_sync_and_closes_all_handles() {
    let io = Arc::new(HookIo::sim(0));
    let (mut core, _) = open(io.clone(), opts(65536));
    let syncs = io.tally(|op| data_sync(op) as u64);
    let replies: Vec<_> = (0..20).map(|_| submit(&mut core, hello())).collect();
    assert_eq!(core.durable_lsn(), 0);
    core.step();
    assert_eq!(syncs.load(Ordering::SeqCst), 1);
    for reply in replies {
        reply.recv().unwrap().unwrap();
    }
    drop(core);
    assert_eq!(io.open_files(), 0);
}

#[test]
fn partial_write_commits_only_the_complete_prefix() {
    // (case, write budget, records per request, tail reservation fails, sparse resize fails)
    for (case, budget, requests, reserve_fails, resize_fails) in [
        ("partial batch", 200, &[3][..], false, false),
        ("partial second request", 217, &[1, 1], false, false),
        ("tail reservation fails", 217, &[1, 1], true, false),
        ("sparse resize also fails", 217, &[1, 1], true, true),
    ] {
        let io = Arc::new(HookIo::sim(19));
        let opts = Options { free_space_floor: Some(0), ..opts(4096) };
        let (mut core, _) = open(io.clone(), opts.clone());
        let db = Path::new("db");
        let used = io.inner.total_bytes(db).unwrap() - io.inner.available_bytes(db).unwrap();
        if reserve_fails {
            shrink_after_space_check(&io, used - 512);
        }
        if resize_fails {
            io.fail_once(ErrorKind::Other, |op| matches!(op, Op::SetLen(_, 4096)));
        }
        io.inner.enospc_after_bytes(budget).unwrap();
        let replies: Vec<_> =
            requests.iter().map(|&n| submit(&mut core, vec![hello()[0].clone(); n])).collect();
        core.step();
        let mut results: Vec<_> = replies.into_iter().map(|r| r.recv().unwrap()).collect();
        let error = results.pop().unwrap().unwrap_err();
        let outcome = if resize_fails { Outcome::Unknown } else { Outcome::NotCommitted };
        assert!(matches!(error.kind, Error::DiskFull), "{case}: {error:?}");
        assert_eq!(error.outcome, outcome, "{case}");
        let durable = results.len() as u64 * u64::from(!resize_fails);
        for result in results {
            match result {
                Ok(done) => assert!(matches!(done, Committed::Durable { last_lsn: 1, .. })),
                Err(error) => assert_eq!(error.outcome, outcome, "{case}: prefix shares the sync"),
            }
        }
        assert!(core.is_poisoned(), "{case}");
        assert_eq!(core.durable_lsn(), durable, "{case}");
        assert!(core.read(durable + 1).unwrap().is_none(), "{case}");
        if reserve_fails && !resize_fails {
            assert_eq!(io.inner.available_bytes(db).unwrap(), 3072, "{case}: sparse tail");
        }
        if resize_fails {
            continue;
        }
        let image = image(&io.inner);
        image.set_capacity(1 << 30).unwrap();
        drop(core);
        let (reopened, report) = open(image, opts);
        assert_eq!(report.records as u64, durable, "{case}");
        assert!((1..=durable).all(|lsn| reopened.read(lsn).unwrap().unwrap().payload == b"hello"));
        assert!(reopened.read(durable + 1).unwrap().is_none(), "{case}");
    }
}

#[test]
fn real_files_reopen_with_multiple_tenants_and_rebuilt_chain_heads() {
    let dir = TestDir::new();
    let (io, clock) = (Arc::new(StdIo::new()), clock());
    let reopen = || Tape::open(io.clone(), clock.clone(), &dir.0, opts(65536)).unwrap();
    let (tape, _) = reopen();
    tape.append(hello(), Durability::Durable).unwrap();
    let batch = vec![
        record(Kind::Turn, 1, b"hello"),
        record(Kind::Fact, 2, b"hello"),
        record(Kind::Forget, 1, b"hello"),
    ];
    let committed = tape.append(batch, Durability::Durable).unwrap();
    assert!(matches!(committed, Committed::Durable { first_lsn: 2, last_lsn: 4, .. }));
    drop(tape);
    let (tape, report) = reopen();
    assert_eq!(report.records, 4);
    for lsn in 1..=4 {
        assert_eq!(&tape.read(lsn).unwrap().unwrap().payload, b"hello");
    }
    tape.append(hello(), Durability::Durable).unwrap();
    drop(tape);
    assert_eq!(reopen().1.records, 5);
}

#[test]
fn segment_size_mismatch_is_rejected_without_mutation() {
    for wrong in [512, 2048] {
        let io = Arc::new(HookIo::sim(0));
        let (mut core, _) = open(io.clone(), opts(1024));
        commit(&mut core, hello()).unwrap();
        drop(core);
        let (before, events) = (files(&*io), io.record());
        let result = WriterCore::open(io.clone(), clock(), "db", opts(wrong));
        assert!(matches!(result, Err(Error::Configuration(_))), "size={wrong}");
        assert!(events.lock().unwrap().is_empty(), "size={wrong}: mismatch must not write or sync");
        assert_eq!(files(&*io), before, "size={wrong}");
        assert_eq!(io.open_files(), 0, "size={wrong}");
        assert_eq!(open(io, opts(1024)).1.records, 1, "size={wrong}");
    }
}

#[test]
fn commit_time_survives_a_restart_with_a_backwards_clock() {
    let disk = Arc::new(SimDisk::new(1));
    let mut previous = 0;
    for now in [1000, 1, 0] {
        let clock = Arc::new(SimClock::new(now));
        let (mut core, _) = WriterCore::open(disk.clone(), clock, "db", opts(1024)).unwrap();
        commit(&mut core, vec![record(Kind::Turn, 1, b"")]).unwrap();
        let time = core.read(core.durable_lsn()).unwrap().unwrap().header.commit_time_ns;
        assert!(time > previous, "clock={now}");
        previous = time;
    }
}

#[test]
fn root_api_appends_reads_and_reopens_atomic_tenant_creates() {
    let _: (StdIo, FileId, Digest, Option<Header>, Option<Frame>, Option<RecoveryReport>) =
        (StdIo::new(), FileId(1), [0; 32], None, None, None);
    let _: WriteResult = Err(WriteError { kind: Error::Poisoned, outcome: Outcome::NotCommitted });
    let _guard = LockGuard::new(|| {});
    let (io, clock): (Arc<dyn Io>, Arc<dyn Clock>) =
        (Arc::new(SimDisk::new(5)), Arc::new(SystemClock));
    let (tape, _) = Tape::open(io.clone(), clock.clone(), "db", opts(4096)).unwrap();
    for _ in 0..2 {
        let batch = vec![record(Kind::TenantCreate, 1, b"tenant"), record(Kind::Turn, 1, b"turn")];
        tape.append(batch, Durability::Durable).unwrap();
    }
    let Committed::Durable { first_lsn, .. } =
        tape.append(vec![record(Kind::Turn, 1, b"public")], Durability::Durable).unwrap();
    assert_eq!(&tape.read(first_lsn).unwrap().unwrap().payload, b"public");
    assert_eq!(tape.read(3).unwrap().unwrap().header.kind, Kind::TenantCreate);
    drop(tape);
    let (tape, _) = Tape::open(io, clock, "db", opts(4096)).unwrap();
    assert_eq!(tape.scan(0).count(), 5);
}

#[test]
fn crash_during_a_group_spanning_roll_retains_every_prior_ack() {
    for seed in 0..100 {
        let io = Arc::new(HookIo::sim(seed));
        // Odd seeds tear unsynced writes at 4-byte sectors instead of 512.
        io.inner.set_sector_size(if seed % 2 == 1 { 4 } else { 512 }).unwrap();
        let (mut core, _) = open(io.clone(), opts(512));
        commit(&mut core, hello()).unwrap();
        let images = io.capture();
        let replies: Vec<_> = (0..3).map(|_| submit(&mut core, hello())).collect();
        core.step();
        for reply in replies {
            reply.recv().unwrap().unwrap();
        }
        for (event, image) in images.lock().unwrap().iter() {
            let (reopened, _) = open(Arc::new(image.clone()), opts(512));
            let payload = reopened.read(1).unwrap().unwrap().payload;
            assert_eq!(payload, b"hello", "seed={seed}, crash after {event}");
        }
        let (mut reopened, report) = open(image(&io.inner), opts(512));
        assert_eq!(report.records, 4, "seed={seed}");
        for lsn in 1..=4 {
            assert_eq!(&reopened.read(lsn).unwrap().unwrap().payload, b"hello", "seed={seed}");
        }
        let committed = commit(&mut reopened, hello()).unwrap();
        assert!(matches!(committed, Committed::Durable { first_lsn: 5, .. }), "seed={seed}");
    }
}

#[test]
#[cfg_attr(debug_assertions, ignore = "slow in debug")]
fn concurrent_durable_appends_survive_reopen() {
    let dir = TestDir::new();
    let (io, clock) = (Arc::new(StdIo::new()), Arc::new(SystemClock));
    let (tape, _) = Tape::open(io.clone(), clock.clone(), &dir.0, Options::default()).unwrap();
    let barrier = Barrier::new(16);
    let append = |tenant: u32| {
        let mut rng = u64::from(tenant) + 1;
        barrier.wait();
        (0..1250u32)
            .map(|sequence| {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let mut content = vec![rng as u8; (rng % 8193) as usize];
                let count = content.len().min(4);
                content[..count].copy_from_slice(&sequence.to_le_bytes()[..count]);
                let batch = vec![record(Kind::Turn, tenant, &content)];
                let Committed::Durable { first_lsn, last_lsn, .. } =
                    tape.append(batch, Durability::Durable).unwrap();
                assert_eq!(first_lsn, last_lsn);
                (first_lsn, tenant, content)
            })
            .collect::<Vec<_>>()
    };
    let acknowledged: Vec<_> = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..16).map(|tenant| scope.spawn(move || append(tenant))).collect();
        threads.into_iter().flat_map(|thread| thread.join().unwrap()).collect()
    });
    assert_eq!(acknowledged.len(), 20_000);
    assert_eq!(tape.durable_lsn(), 20_000);
    drop(tape);
    let (tape, report) = Tape::open(io, clock, &dir.0, Options::default()).unwrap();
    assert_eq!(report.records, 20_000);
    let mut lsns = std::collections::BTreeSet::new();
    for (lsn, tenant, content) in acknowledged {
        assert!(lsns.insert(lsn), "lsn={lsn} acknowledged twice");
        let frame = tape.read(lsn).unwrap().unwrap();
        let header = (frame.header.lsn, frame.header.tenant_id, frame.header.kind);
        assert_eq!(header, (lsn, tenant, Kind::Turn));
        assert_eq!(frame.payload, content);
    }
}

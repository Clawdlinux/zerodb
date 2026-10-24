use crate::support::*;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;
use zerodb_core::{
    Appended, Engine, EngineOptions, ErrorKind, Health, NewTurn, Outcome, Scope, WindowQuery,
};
use zerodb_tape::sim::SimClock;
use zerodb_tape::{HookIo, Io, Kind, Op, SimDisk, Tape, TapeOptions};

#[test]
fn windows_are_dense_and_read_cost_depends_only_on_returned_turns() {
    let disk = Arc::new(SimDisk::new(1));
    let engine = open(disk.clone(), options());
    let scope = scope("s");
    let mut written = 0;
    for boundary in [1, 16, 17, 5000] {
        eprintln!("case: boundary={boundary:?}");
        while written < boundary {
            let count = (boundary - written).min(128);
            let receipt = engine
                .append(&scope, (written..written + count).map(turn).collect(), None)
                .unwrap();
            assert_eq!(receipt.seqs, (written + 1..=written + count).collect::<Vec<_>>());
            written += count;
        }
        for n in [0, 1, 7, 32] {
            let before = engine.metrics().unwrap().window_reads;
            let window = engine.window(&scope, WindowQuery::Last(n)).unwrap();
            assert_eq!(window.len(), n.min(boundary as usize));
            assert_eq!(engine.metrics().unwrap().window_reads - before, window.len() as u64);
            for pair in window.windows(2) {
                assert_eq!(pair[1].seq, pair[0].seq + 1);
            }
            for value in window {
                assert_eq!(value.content, (value.seq - 1).to_le_bytes());
            }
        }
        let before = engine.metrics().unwrap().window_reads;
        let window = engine.window(&scope, WindowQuery::Since { seq: 1, limit: 3 }).unwrap();
        assert_eq!(window.len(), boundary.min(3) as usize);
        assert_eq!(engine.metrics().unwrap().window_reads - before, window.len() as u64);
        assert_eq!(
            engine.window(&scope, WindowQuery::Since { seq: boundary + 1, limit: 10 }).unwrap(),
            vec![]
        );
    }
    // Byte budgets count content plus meta. Every turn in "s" costs 8 bytes, in "meta" 16.
    let meta = crate::support::scope("meta");
    let turns = (1..=3).map(|value| NewTurn { meta: vec![0; 8], ..turn(value) }).collect();
    engine.append(&meta, turns, None).unwrap();
    for (case, scope, query, max_bytes, seqs, truncated) in [
        ("fits all", &scope, WindowQuery::Last(4), 32, &[4997, 4998, 4999, 5000][..], false),
        ("fits some", &scope, WindowQuery::Last(4), 20, &[4999, 5000][..], true),
        ("newest alone too large", &scope, WindowQuery::Last(4), 7, &[][..], true),
        (
            "since with a budget",
            &scope,
            WindowQuery::Since { seq: 10, limit: 5 },
            24,
            &[10, 11, 12][..],
            true,
        ),
        ("meta counts", &meta, WindowQuery::Last(3), 40, &[2, 3][..], true),
    ] {
        let before = engine.metrics().unwrap().window_reads;
        let window = engine.window_budget(scope, query, max_bytes).unwrap();
        let got: Vec<_> = window.turns.iter().map(|turn| turn.seq).collect();
        assert_eq!(got, seqs, "case: {case}");
        assert_eq!(window.truncated, truncated, "case: {case}");
        let reads = engine.metrics().unwrap().window_reads - before;
        assert_eq!(reads, got.len() as u64 + u64::from(truncated), "case: {case}");
    }
    // A huge `last` with no budget after a reopen reads one turn and loads only the
    // newest index page, not one slot per requested turn.
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = open(disk, options());
    let before = engine.metrics().unwrap();
    let window = engine.window_budget(&scope, WindowQuery::Last(1_000_000), 0).unwrap();
    assert!(window.turns.is_empty() && window.truncated);
    let after = engine.metrics().unwrap();
    assert_eq!(after.window_reads - before.window_reads, 1, "one tape read");
    assert!(after.hot_page_bytes - before.hot_page_bytes <= 4096 * 8, "{after:?}");
}

#[test]
fn retries_conflicts_scope_isolation_and_replay() {
    let disk = Arc::new(SimDisk::new(2));
    let engine = open(disk.clone(), options());
    let s = scope("s");
    let first = engine.append(&s, vec![turn(1), turn(2)], Some("key")).unwrap();
    assert!(!first.deduplicated, "a fresh commit is not deduplicated");
    let durable = engine.metrics().unwrap().durable_lsn;
    let retried = |engine: &Engine, case: &str| {
        let receipt = engine.append(&s, vec![turn(1), turn(2)], Some("key")).unwrap();
        assert_deduplicated(&first, receipt, case);
    };
    retried(&engine, "fast-path retry");
    assert_eq!(engine.metrics().unwrap().durable_lsn, durable);
    let error = engine.append(&s, vec![turn(9)], Some("key")).unwrap_err();
    assert!(matches!(error.kind, ErrorKind::IdempotencyConflict));
    assert_eq!(error.outcome, Outcome::NotCommitted);
    let mut other = s.clone();
    other.agent = "another-agent".into();
    assert!(matches!(
        engine.append(&other, vec![turn(1), turn(2)], Some("key")).unwrap_err().kind,
        ErrorKind::IdempotencyConflict
    ));
    engine.append(&scope("other"), vec![turn(9)], Some("key")).unwrap();
    other.tenant = "another-tenant".into();
    engine.append(&other, vec![turn(9)], Some("key")).unwrap();
    drop(engine);
    let engine = open(disk.clone(), options());
    retried(&engine, "retry after reopen with replay");
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = open(disk, options());
    assert_eq!(engine.metrics().unwrap().replayed_records, 0);
    retried(&engine, "retry after reopen from a checkpoint");
}

#[test]
fn tenant_ids_survive_reopen_and_limit_is_typed() {
    let (disk, limit) = (Arc::new(SimDisk::new(3)), 2);
    let opts = EngineOptions { max_tenants: limit, ..options() };
    let engine = open(disk.clone(), opts.clone());
    let tenant = |id: usize| Scope { tenant: format!("tenant-{id}"), ..scope("s") };
    let scopes: Vec<_> =
        (1..=limit).map(|id| if id == 1 { scope("s") } else { tenant(id) }).collect();
    for (index, scope) in scopes.iter().enumerate() {
        let receipt = engine.append(scope, vec![turn(index as u64 + 1)], None).unwrap();
        assert_eq!(receipt.seqs, vec![1], "tenant={index}");
    }
    for (index, scope) in scopes.iter().enumerate() {
        let window = engine.window(scope, WindowQuery::Last(1)).unwrap();
        assert_eq!(window[0].content, (index as u64 + 1).to_le_bytes(), "tenant={index}");
    }
    let error = engine.append(&tenant(limit + 1), vec![turn(1)], None).unwrap_err();
    assert!(matches!(error.kind, ErrorKind::ResourceExhausted("tenants")), "{error}");
    assert_eq!(error.outcome, Outcome::NotCommitted);
    drop(engine);
    let (tape, _) =
        Tape::open(disk.clone(), Arc::new(SimClock::default()), "db", opts.tape.clone()).unwrap();
    let creates: Vec<_> =
        tape.scan(0).map(Result::unwrap).filter(|r| r.kind == Kind::TenantCreate).collect();
    let ids: Vec<_> = creates.iter().map(|record| record.tenant_id as usize).collect();
    assert_eq!(ids, (1..=limit).collect::<Vec<_>>(), "ids are dense and logged in order");
    assert_eq!(creates[0].payload.as_ref(), b"tenant");
    drop(tape);
    let lowered = EngineOptions { max_tenants: limit - 1, ..opts.clone() };
    let clock = Arc::new(SimClock::default());
    let error = Engine::open_with(disk.clone(), clock, "db", lowered).unwrap_err();
    assert!(matches!(error.kind, ErrorKind::ResourceExhausted("tenants")), "lowered: {error}");
    let engine = open(disk, opts);
    engine.append(&scope("s"), vec![turn(2)], None).unwrap();
    assert_eq!(engine.window(&scope("s"), WindowQuery::Last(2)).unwrap().len(), 2);
    assert_eq!(engine.metrics().unwrap().tenants, limit);
}

#[test]
fn dedup_scope_bounds_and_oldest_eviction_survive_the_public_api() {
    let disk = Arc::new(SimDisk::new(40));
    let opts = EngineOptions { dedup_per_tenant: 2, dedup_global: 3, ..options() };
    let engine = open(disk.clone(), opts.clone());
    enum Write {
        New,
        Cached,
        ProbeEviction,
    }
    for (case, tenant, session, key, expected_seq, expected_entries, window, write) in [
        ("first tenant", "first", "s", "k", 1, 1, None, Write::New),
        ("session scope", "first", "other", "k", 1, 2, None, Write::New),
        ("tenant scope", "second", "s", "k", 1, 3, Some(1), Write::New),
        ("per-tenant oldest eviction", "first", "s", "new", 2, 3, Some(1), Write::ProbeEviction),
        ("global oldest eviction", "third", "s", "k", 1, 3, Some(1), Write::New),
        ("other tenant key survives", "second", "s", "k", 1, 3, Some(1), Write::Cached),
        ("oldest session key was evicted", "first", "other", "k", 2, 3, Some(1), Write::New),
    ] {
        eprintln!("case: {case}");
        let mut scope = scope(session);
        scope.tenant = tenant.into();
        let before = engine.metrics().unwrap().durable_lsn;
        let receipt = engine.append(&scope, vec![turn(1)], Some(key)).unwrap();
        assert_eq!(receipt.seqs, vec![expected_seq], "{case}");
        assert_eq!(engine.metrics().unwrap().dedup_entries, expected_entries, "{case}");
        if matches!(write, Write::Cached) {
            assert_eq!(engine.metrics().unwrap().durable_lsn, before, "{case}");
        } else {
            assert!(engine.metrics().unwrap().durable_lsn > before, "{case}");
        }
        if let Some(window) = window {
            assert_eq!(engine.metrics().unwrap().dedup_window_effective, window, "{case}");
        }
        if matches!(write, Write::ProbeEviction) {
            engine.checkpoint().unwrap();
            let image = Arc::new(disk.crash().unwrap());
            let fork = open(image, opts.clone());
            let retry = fork.append(&scope, vec![turn(1)], Some("k")).unwrap();
            assert_eq!(retry.seqs, vec![3], "{case}: the evicted key must write again");
            assert!(fork.metrics().unwrap().durable_lsn > receipt.last_lsn, "{case}");
        }
    }

    let mut checkpoint_scope = scope("checkpoint");
    checkpoint_scope.tenant = "first".into();
    for (case, range) in [("first checkpoint", 0..3), ("second checkpoint", 3..6)] {
        eprintln!("case: {case}");
        for i in range {
            engine.append(&checkpoint_scope, vec![turn(i)], Some(&i.to_string())).unwrap();
        }
        assert_eq!(engine.metrics().unwrap().dedup_entries, 3, "{case}");
        engine.checkpoint().unwrap();
    }
    drop(engine);
    let engine = open(disk, opts);
    assert_eq!(engine.metrics().unwrap().replayed_records, 0);
    assert_eq!(engine.metrics().unwrap().dedup_entries, 3);
    assert_eq!(engine.metrics().unwrap().dedup_window_effective, 1);
    assert_eq!(engine.append(&checkpoint_scope, vec![turn(5)], Some("5")).unwrap().seqs, vec![6]);
    assert_eq!(engine.append(&checkpoint_scope, vec![turn(0)], Some("0")).unwrap().seqs, vec![7]);
    let mut session = scope("x");
    session.tenant = "first".into();
    let appended = engine.append(&session, vec![turn(1)], Some("same")).unwrap();
    let fact = engine.fact_put("first", new_fact("x", 1, "a"), Some("same"), None).unwrap();
    assert!(!fact.deduplicated && fact.lsn > appended.last_lsn, "session x and fact key x");
}

#[test]
fn checkpoint_bounds_replay_and_corrupt_derived_data_falls_back() {
    let disk = Arc::new(SimDisk::new(5));
    let engine = open(disk.clone(), options());
    engine.append(&scope("s"), (0..100).map(turn).collect(), Some("initial")).unwrap();
    let before = engine.fact_put("tenant", new_fact("fact/key", 1, "a"), Some("f"), None).unwrap();
    engine.checkpoint().unwrap();
    engine.append(&scope("s"), (100..107).map(turn).collect(), Some("tail")).unwrap();
    let after = engine.fact_put("tenant", new_fact("fact/key", 2, "a"), None, None).unwrap();
    let oracle = engine.window(&scope("s"), WindowQuery::Last(200)).unwrap();
    let facts = |engine: &Engine| {
        (
            engine.fact_get("tenant", "fact/key").unwrap(),
            engine.fact_history("tenant", "fact/key", usize::MAX).unwrap(),
            engine.fact_list("tenant", query("", 100, usize::MAX)).unwrap(),
        )
    };
    let fact_oracle = facts(&engine);
    assert_eq!(fact_oracle.1.revisions.len(), 2);
    drop(engine);
    let engine = open(disk.clone(), options());
    assert_eq!(engine.metrics().unwrap().replayed_records, 8);
    assert_eq!(engine.window(&scope("s"), WindowQuery::Last(200)).unwrap(), oracle);
    drop(engine);
    let checkpoint = disk
        .list(std::path::Path::new("db"))
        .unwrap()
        .into_iter()
        .find(|p| p.file_name().unwrap().to_str().unwrap().starts_with("ckpt-"))
        .unwrap();
    let file = disk.open(&checkpoint).unwrap();
    let mut image = vec![0; disk.len(file).unwrap() as usize];
    disk.read_at(file, 0, &mut image).unwrap();
    // Tag 4 records are written last, so the key's last occurrence is inside one.
    let offset = image.windows(8).rposition(|w| w == b"fact/key").unwrap();
    disk.flip_bit(file, offset as u64).unwrap();
    disk.close(file).unwrap();
    let engine = open(disk, options());
    assert_eq!(engine.metrics().unwrap().checkpoint_warnings, 1);
    assert_eq!(engine.metrics().unwrap().replayed_records, 110);
    assert_eq!(engine.window(&scope("s"), WindowQuery::Last(200)).unwrap(), oracle);
    assert_eq!(facts(&engine), fact_oracle, "full replay rebuilds the same fact reads");
    let retry = engine.fact_put("tenant", new_fact("fact/key", 1, "a"), Some("f"), None).unwrap();
    assert!(retry.deduplicated && retry.lsn == before.lsn && retry.txn == before.txn);
    engine.fact_put("tenant", new_fact("fact/key", 3, "a"), None, Some(after.lsn)).unwrap();
}

#[test]
fn hot_page_budget_evicts_reloads_and_bounds_full_replay() {
    let disk = Arc::new(SimDisk::new(13));
    let opts = EngineOptions { hot_page_bytes: 128, ..options() };
    let engine = open(disk.clone(), opts.clone());
    let oldest = |engine: &Engine, i: u64| {
        let query = WindowQuery::Since { seq: 1, limit: 1 };
        engine.window(&scope(&i.to_string()), query).unwrap()[0].content.clone()
    };
    for i in 0u64..12 {
        engine.append(&scope(&i.to_string()), vec![turn(i)], Some("key")).unwrap();
        engine.checkpoint().unwrap();
        assert!(engine.metrics().unwrap().hot_page_bytes <= 128, "session {i}");
    }
    for i in 0u64..12 {
        assert_eq!(oldest(&engine, i), i.to_le_bytes(), "cold reload of session {i}");
    }
    engine.append(&scope("0"), vec![turn(12)], None).unwrap();
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = open(disk.clone(), opts.clone());
    assert_eq!(engine.window(&scope("0"), WindowQuery::Last(2)).unwrap().len(), 2);
    assert!(engine.metrics().unwrap().hot_page_bytes <= 128);
    drop(engine);
    // A corrupt manifest forces full replay, which must spill to stay within one page.
    let file = disk.open(Path::new("db/CHECKPOINT")).unwrap();
    disk.flip_bit(file, 12).unwrap();
    disk.close(file).unwrap();
    let engine = open(disk, opts);
    assert_eq!(engine.metrics().unwrap().checkpoint_warnings, 1);
    for i in 0u64..12 {
        assert_eq!(oldest(&engine, i), i.to_le_bytes(), "replayed session {i}");
        let retry = engine.append(&scope(&i.to_string()), vec![turn(i)], Some("key")).unwrap();
        assert!(retry.deduplicated && retry.seqs == [1], "replayed retry key {i}");
    }
    assert!(engine.metrics().unwrap().hot_page_bytes <= 128);
}

#[test]
fn write_outages_report_health_preserve_reads_and_retry_keys() {
    #[derive(Clone, Copy, PartialEq)]
    enum Fault {
        Floor,
        ColdFloor,
        ReopenFull,
        ReopenDirtyFull,
        ReopenAllocation,
        ReopenStat,
        SpaceQuery,
        SyncError,
    }
    use Fault::*;
    for (case, fault, health) in [
        ("disk floor", Floor, Health::ReadOnly),
        ("cold reads with no free space", ColdFloor, Health::ReadOnly),
        ("reopen while full", ReopenFull, Health::ReadOnly),
        ("reopen with dirty pages then full", ReopenDirtyFull, Health::ReadOnly),
        ("reopen allocation race", ReopenAllocation, Health::ReadOnly),
        ("reopen with space query failure", ReopenStat, Health::ReadOnly),
        ("space query outage", SpaceQuery, Health::ReadOnly),
        ("ambiguous sync", SyncError, Health::Poisoned),
    ] {
        let reopen = matches!(fault, ReopenFull | ReopenDirtyFull | ReopenAllocation | ReopenStat);
        let history = if reopen || fault == ColdFloor { 512 } else { 1 };
        for seed in 0..if fault == SyncError { 32 } else { 1 } {
            eprintln!("case: {case}, seed={seed}");
            let (outage, no_space) =
                (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
            let (down, full) = (outage.clone(), no_space.clone());
            let io = Arc::new(HookIo::sim(seed).with(move |op, next| match op {
                Op::Available(_) | Op::Total(_) if down.load(Ordering::SeqCst) => {
                    Err(zerodb_tape::Error::Io(
                        std::io::ErrorKind::Other,
                        "injected space query failure".into(),
                    ))
                }
                Op::Allocate(..) if full.swap(false, Ordering::SeqCst) => {
                    Err(zerodb_tape::Error::Io(
                        std::io::ErrorKind::StorageFull,
                        "injected allocation failure".into(),
                    ))
                }
                _ => next(),
            }));
            let (disk, db) = (io.inner.clone(), Path::new("db"));
            // Leaves `headroom` bytes free above what the database already uses.
            let fill = |headroom| {
                let used = disk.total_bytes(db).unwrap() - disk.available_bytes(db).unwrap();
                disk.set_capacity(used + headroom).unwrap();
            };
            let mut opts = options();
            opts.tape.free_space_floor = Some(4096);
            if matches!(fault, ColdFloor | ReopenDirtyFull) {
                opts.tape.lsn_index_bytes = 4224;
            }
            let mut engine = open(io.clone(), opts.clone());
            assert_eq!(engine.health(), Health::Ok, "{case}");
            let s = scope("s");
            let original = engine.append(&s, vec![turn(1); history], Some("original")).unwrap();
            let before = engine.metrics().unwrap();
            match fault {
                Floor => fill(4095),
                ColdFloor => fill(0),
                SpaceQuery => outage.store(true, Ordering::SeqCst),
                SyncError => disk.fail_next_sync().unwrap(),
                ReopenFull | ReopenDirtyFull | ReopenAllocation | ReopenStat => {
                    if fault == ReopenDirtyFull {
                        // Skip open-time replay, so recovery's last dirty page stays resident.
                        engine.checkpoint().unwrap();
                    }
                    drop(engine);
                    for name in ["LSN-OFFSETS", "LSN-ROUTES"] {
                        let file = disk.open(&db.join(name)).unwrap();
                        disk.set_len(file, 0).unwrap();
                        disk.set_len(file, 4096).unwrap();
                        disk.sync_data(file).unwrap();
                        disk.close(file).unwrap();
                    }
                    if matches!(fault, ReopenFull | ReopenDirtyFull) {
                        // Open drops the old spare, so fill the disk without it.
                        disk.remove(&db.join("spare.zdb")).unwrap();
                        disk.sync_dir(db).unwrap();
                        // Recovery write-backs fit in 5 pages. Reservation plus floor needs two
                        // 1 MiB index chunks, so open stays read-only.
                        fill(if fault == ReopenDirtyFull { 20480 } else { 0 });
                    } else if fault == ReopenAllocation {
                        no_space.store(true, Ordering::SeqCst);
                    } else {
                        opts.tape.free_space_floor = None;
                        outage.store(true, Ordering::SeqCst);
                    }
                    engine = open(io.clone(), opts.clone());
                    assert_eq!(engine.health(), Health::ReadOnly, "{case}: open did not poison");
                    assert!(!no_space.load(Ordering::SeqCst), "{case}: reservation was attempted");
                    if matches!(fault, ReopenAllocation | ReopenDirtyFull) {
                        fill(0);
                    }
                    let full_before = disk.storage_full_returns().unwrap();
                    let oldest =
                        engine.window(&s, WindowQuery::Since { seq: 1, limit: 1 }).unwrap();
                    assert_eq!(
                        oldest[0].content,
                        turn(1).content,
                        "{case}: acknowledged turn survived reopen"
                    );
                    assert_eq!(oldest[0].lsn, original.first_lsn, "{case}");
                    if fault == ReopenDirtyFull {
                        let index = engine.metrics().unwrap().tape_index;
                        let full = disk.storage_full_returns().unwrap();
                        assert!(
                            full > full_before,
                            "{case}: cold read could not write back a dirty page"
                        );
                        assert!(
                            index.resident_bytes <= index.budget_bytes + 4224,
                            "{case}: cold read overshoot is one page"
                        );
                    }
                    if fault != ReopenStat {
                        assert_eq!(
                            disk.available_bytes(db).unwrap(),
                            0,
                            "{case}: zero free after reopen"
                        );
                    }
                }
            }
            for attempt in 0..2 {
                let error = engine.append(&s, vec![turn(2)], Some("retry")).unwrap_err();
                let unknown = fault == SyncError && attempt == 0;
                let outcome = if unknown { Outcome::Unknown } else { Outcome::NotCommitted };
                assert_eq!(error.outcome, outcome, "{case}, attempt={attempt}");
                assert!(
                    match fault {
                        SpaceQuery | ReopenStat => matches!(
                            &error.kind,
                            ErrorKind::Storage(zerodb_tape::Error::Io(std::io::ErrorKind::Other, message))
                                if message == "injected space query failure"
                        ),
                        SyncError if unknown => {
                            matches!(
                                error.kind,
                                ErrorKind::Storage(zerodb_tape::Error::SyncFailed(_))
                            )
                        }
                        SyncError => matches!(error.kind, ErrorKind::Poisoned),
                        _ => matches!(error.kind, ErrorKind::Storage(zerodb_tape::Error::DiskFull)),
                    },
                    "{case}, attempt={attempt}: {error:?}"
                );
                assert_eq!(engine.health(), health, "{case}");
                if fault != SyncError {
                    assert_deduplicated(
                        &original,
                        engine.append(&s, vec![turn(1); history], Some("original")).unwrap(),
                        &format!("{case}: committed retries keep their durable receipt"),
                    );
                    let conflict = engine.append(&s, vec![turn(2)], Some("original")).unwrap_err();
                    assert!(matches!(conflict.kind, ErrorKind::IdempotencyConflict), "{case}");
                    assert_eq!(conflict.outcome, Outcome::NotCommitted, "{case}");
                    assert_eq!(engine.health(), health, "{case}: retries do not recheck space");
                }
                if fault == ColdFloor {
                    assert!(
                        before.tape_index.dirty_page_evictions > 0,
                        "{case}: multiple dirty pages"
                    );
                    let before_cold = engine.metrics().unwrap().tape_index;
                    let cold = engine.window(&s, WindowQuery::Since { seq: 1, limit: 3 }).unwrap();
                    let after_cold = engine.metrics().unwrap().tape_index;
                    if attempt == 0 {
                        assert!(
                            after_cold.dirty_page_evictions > before_cold.dirty_page_evictions,
                            "{case}: oldest read evicted a dirty page"
                        );
                    }
                    assert_eq!(
                        cold.iter().map(|turn| turn.seq).collect::<Vec<_>>(),
                        [1, 2, 3],
                        "{case}"
                    );
                    assert!(cold.iter().all(|value| value.content == turn(1).content), "{case}");
                    assert!(
                        after_cold.page_reloads > before_cold.page_reloads,
                        "{case}: cold index read"
                    );
                    assert_eq!(disk.available_bytes(db).unwrap(), 0, "{case}");
                    assert_eq!(
                        engine.health(),
                        Health::ReadOnly,
                        "{case}: cold read kept read-only"
                    );
                }
                let window = engine.window(&s, WindowQuery::Last(10)).unwrap();
                assert_eq!(window.len(), history.min(10), "{case}");
                assert!(window.iter().all(|value| value.content == turn(1).content), "{case}");
                assert_eq!(window.last().unwrap().lsn, original.last_lsn, "{case}");
                let after = engine.metrics().unwrap();
                assert_eq!(after.durable_lsn, before.durable_lsn, "{case}");
                assert_eq!(after.dedup_entries, before.dedup_entries, "{case}");
                assert_eq!(after.tenants, before.tenants, "{case}");
            }
            disk.set_capacity(1 << 40).unwrap();
            outage.store(false, Ordering::SeqCst);
            assert_eq!(engine.health(), health, "{case}: no write has rechecked space");
            if fault == SyncError {
                drop(engine);
                engine = open(Arc::new(disk.crash().unwrap()), opts.clone());
                assert_eq!(engine.health(), Health::Ok, "{case}");
            }
            // Only an ambiguous sync can leave the first attempt durable across the crash.
            let survived = engine.metrics().unwrap().durable_lsn > original.last_lsn;
            assert!(!survived || fault == SyncError, "{case}: a rejection wrote nothing");
            let retried = engine.append(&s, vec![turn(2)], Some("retry")).unwrap();
            assert_eq!(retried.deduplicated, survived, "{case}: retry after restart");
            assert_eq!(engine.health(), Health::Ok, "{case}");
            if reopen {
                for (name, width) in [("LSN-OFFSETS", 8), ("LSN-ROUTES", 16)] {
                    let file = disk.open(&db.join(name)).unwrap();
                    // Index backing grows in whole 1 MiB chunks.
                    let required = (retried.last_lsn * width).div_ceil(1 << 20) << 20;
                    assert_eq!(
                        disk.len(file).unwrap(),
                        required,
                        "{case}: retried {name} reservation"
                    );
                    disk.close(file).unwrap();
                }
            }
            assert_eq!(retried.seqs, vec![history as u64 + 1], "{case}");
            assert_eq!(retried.txn, original.txn + 1, "{case}");
            assert_eq!(retried.first_lsn, original.last_lsn + 1, "{case}");
            assert_eq!(retried.last_lsn, retried.first_lsn, "{case}");
            let retried = Appended { deduplicated: false, ..retried };
            let retry = |engine: &Engine| engine.append(&s, vec![turn(2)], Some("retry")).unwrap();
            let again = |engine: &Engine| {
                engine.append(&s, vec![turn(1); history], Some("original")).unwrap()
            };
            assert_deduplicated(&retried, retry(&engine), case);
            assert_eq!(engine.metrics().unwrap().durable_lsn, retried.last_lsn, "{case}");
            assert_eq!(engine.metrics().unwrap().dedup_entries, before.dedup_entries + 1, "{case}");
            let window = engine.window(&s, WindowQuery::Last(10)).unwrap();
            let first = history as u64 + 1 - history.min(9) as u64;
            let seqs = window.iter().map(|turn| turn.seq).collect::<Vec<_>>();
            assert_eq!(seqs, (first..=history as u64 + 1).collect::<Vec<_>>(), "{case}");
            let (last, older) = window.split_last().unwrap();
            assert!(older.iter().all(|value| value.content == turn(1).content), "{case}");
            assert_eq!(last.content, turn(2).content, "{case}");
            assert_deduplicated(&original, again(&engine), case);
            if fault != SyncError {
                drop(engine);
                let engine = open(Arc::new(disk.crash().unwrap()), opts);
                assert_eq!(engine.health(), Health::Ok, "{case}");
                assert_deduplicated(&original, again(&engine), case);
                assert_deduplicated(&retried, retry(&engine), case);
                assert_eq!(engine.window(&s, WindowQuery::Last(10)).unwrap(), window, "{case}");
            }
        }
    }
}

#[test]
fn concurrent_same_key_writes_once() {
    let engine = open(Arc::new(SimDisk::new(8)), options());
    let barrier = Barrier::new(8);
    std::thread::scope(|threads| {
        let mut handles = Vec::new();
        for _ in 0..8 {
            handles.push(threads.spawn(|| {
                barrier.wait();
                engine.append(&scope("s"), vec![turn(1), turn(2)], Some("same")).unwrap()
            }));
        }
        let receipts: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let fresh: Vec<_> = receipts.iter().filter(|r| !r.deduplicated).collect();
        assert_eq!(fresh.len(), 1, "exactly one caller commits");
        for receipt in &receipts {
            assert_eq!(Appended { deduplicated: false, ..receipt.clone() }, *fresh[0]);
        }
    });
    assert_eq!(engine.window(&scope("s"), WindowQuery::Last(100)).unwrap().len(), 2);
    assert_eq!(engine.metrics().unwrap().durable_lsn, 3);
}

#[test]
fn invalid_and_over_limit_writes_are_typed_and_write_nothing() {
    let opts = EngineOptions { max_sessions: 1, max_pages: 1, ..options() };
    let engine = open(Arc::new(SimDisk::new(9)), opts);
    let s = scope("s");
    let too_big = NewTurn { content: vec![0; 4 * 1024 * 1024], ..turn(1) };
    let append = |turns| engine.append(&s, turns, None).unwrap_err();
    let fact = |key: &str| engine.fact_put("new", new_fact(key, 1, "a"), None, None).unwrap_err();
    for (case, error, kind) in [
        ("empty batch", append(vec![]), "InvalidInput"),
        ("1024 turns", append(vec![turn(1); 1024]), "Storage(BatchTooLarge)"),
        ("4 MiB turn", append(vec![too_big]), "Storage(BatchTooLarge)"),
        ("empty fact key", fact(""), "InvalidInput"),
        ("1025-byte fact key", fact(&"k".repeat(1025)), "InvalidInput"),
    ] {
        assert_eq!(format!("{:?}", error.kind), kind, "{case}");
        assert_eq!(error.outcome, Outcome::NotCommitted, "{case}");
    }
    assert_eq!(engine.metrics().unwrap().tenants, 0);
    assert_eq!(engine.metrics().unwrap().durable_lsn, 0);
    let seqs = engine.append(&s, vec![turn(1); 16], None).unwrap().seqs;
    assert_eq!(seqs, (1..=16).collect::<Vec<_>>(), "rejections consumed no sequences");
    for (session, limit) in [("other", "sessions"), ("s", "page_directory")] {
        let error = engine.append(&scope(session), vec![turn(1)], None).unwrap_err();
        assert!(matches!(error.kind, ErrorKind::ResourceExhausted(l) if l == limit), "{error}");
    }
    assert_eq!(engine.window(&s, WindowQuery::Last(100)).unwrap().len(), 16);
}

#[test]
fn automatic_worker_checkpoint_and_checkpoint_failure_do_not_poison_tape() {
    let disk = Arc::new(SimDisk::new(11));
    let opts = EngineOptions { checkpoint_bytes: 1, ..options() };
    let engine = open(disk, opts);
    let receipt = engine.append(&scope("s"), vec![turn(1)], None).unwrap();
    for _ in 0..1000 {
        if engine.metrics().unwrap().checkpoint_watermark == receipt.last_lsn {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(engine.metrics().unwrap().checkpoint_watermark, receipt.last_lsn);
    drop(engine);
    let disk = Arc::new(SimDisk::new(12));
    let engine = open(disk.clone(), options());
    engine.append(&scope("s"), vec![turn(1)], None).unwrap();
    disk.fail_next_sync().unwrap();
    assert!(engine.checkpoint().is_err());
    assert_eq!(engine.health(), Health::Ok);
    engine.append(&scope("s"), vec![turn(2)], None).unwrap();
    assert_eq!(engine.health(), Health::Ok);
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = open(disk, options());
    assert_eq!(engine.metrics().unwrap().replayed_records, 0);
    assert_eq!(engine.window(&scope("s"), WindowQuery::Last(10)).unwrap().len(), 2);
}

#[test]
fn long_session_evicts_its_own_old_pages() {
    let disk = Arc::new(SimDisk::new(14));
    let opts = EngineOptions { hot_page_bytes: 32768, ..options() };
    let engine = open(disk.clone(), opts.clone());
    let s = scope("long");
    for i in 0..6000 {
        engine.append(&s, vec![turn(i)], None).unwrap();
        if i % 100 == 0 {
            engine.checkpoint().unwrap();
        }
    }
    assert_eq!(engine.window(&s, WindowQuery::Last(3)).unwrap()[0].seq, 5998);
    assert_eq!(engine.window(&s, WindowQuery::Since { seq: 1, limit: 3 }).unwrap()[0].seq, 1);
    assert!(engine.metrics().unwrap().hot_page_bytes <= 32768);
    drop(engine);
    // This budget makes some appends hit a hot-page rejection and resubmit. A resubmitted turn
    // must store the same bytes as a first-try append of that turn.
    let (tape, _) = Tape::open(disk, Arc::new(SimClock::default()), "db", opts.tape).unwrap();
    let mut seq = 0;
    for record in tape.scan(0).map(Result::unwrap).filter(|record| record.kind == Kind::Turn) {
        seq += 1;
        let expected = zerodb_core::turn::encode("agent", "long", seq, &turn(seq - 1), None);
        assert_eq!(record.payload.as_ref(), expected.unwrap(), "seq={seq}");
    }
    assert_eq!(seq, 6000);
}

#[test]
fn receipts_need_no_reads_and_background_io_stays_off_the_writer() {
    let io = Arc::new(HookIo::sim(15));
    let clock = Arc::new(SimClock::default());
    let engine = Engine::open_with(io.clone(), clock.clone(), "db", options()).unwrap();
    let (reads, writes) = (io.tally(|op| matches!(op, Op::Read(..)) as u64), writes(&io));
    let receipt = engine.append(&scope("s"), vec![turn(1)], Some("key")).unwrap();
    assert_eq!(receipt.seqs, vec![1]);
    let retry = engine.append(&scope("s"), vec![turn(1)], Some("key")).unwrap();
    assert_deduplicated(&receipt, retry, "retry");
    assert_eq!(reads.load(Ordering::SeqCst), 0, "append and retry must not read");
    for i in 0..20 {
        engine.append(&scope(&format!("session-{i}")), vec![turn(i)], None).unwrap();
    }
    engine.checkpoint().unwrap();
    let threads = |prefix: &str| -> Vec<String> {
        let writes = writes.lock().unwrap();
        writes.iter().filter(|(_, file)| file.starts_with(prefix)).map(|w| w.0.clone()).collect()
    };
    assert_eq!(threads("ckpt-").len(), 1, "the checkpoint file is written in one buffer");
    let checkpoint = [threads("ckpt-"), threads("CHECKPOINT.next")].concat();
    assert!(checkpoint.iter().all(|thread| thread == "zerodb-checkpoint"), "{checkpoint:?}");
    writes.lock().unwrap().clear();
    clock.advance(31_000_000_000);
    engine.append(&scope("s"), vec![turn(2)], None).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while threads("SUPERBLOCK.tmp").is_empty() {
        assert!(std::time::Instant::now() < deadline, "no periodic superblock install");
        std::thread::yield_now();
    }
    let superblock = threads("SUPERBLOCK.tmp");
    assert!(superblock.iter().all(|thread| thread == "zerodb-maintenance"), "{superblock:?}");
}

#[test]
fn replay_spills_by_dirty_capacity_not_clean_resident_pages() {
    let io = Arc::new(HookIo::sim(16));
    let engine = open(io.clone(), options());
    for i in 0..1300 {
        engine.append(&scope(&i.to_string()), vec![turn(i)], None).unwrap();
    }
    drop(engine);
    let writes = writes(&io);
    let engine = open(io, EngineOptions { hot_page_bytes: 128 * 1024, ..options() });
    let writes = writes.lock().unwrap();
    let files: std::collections::BTreeSet<_> =
        writes.iter().filter(|(_, file)| file.starts_with("ckpt-")).map(|w| &w.1).collect();
    assert!(!files.is_empty());
    assert!(files.len() <= 3, "{} checkpoints for 1300 dirty pages", files.len());
    let newest = engine.window(&scope("1299"), WindowQuery::Last(1)).unwrap();
    assert_eq!(newest[0].content, 1299u64.to_le_bytes());
}

#[test]
fn failed_sync_with_a_full_dedup_window_cannot_checkpoint_tentative_eviction() {
    let disk = Arc::new(SimDisk::new(17));
    let opts = EngineOptions { dedup_per_tenant: 1, dedup_global: 1, ..options() };
    let engine = open(disk.clone(), opts.clone());
    let receipt = engine.append(&scope("s"), vec![turn(1)], Some("original")).unwrap();
    let watermark = engine.checkpoint().unwrap();
    disk.fail_next_sync().unwrap();
    assert_eq!(
        engine.append(&scope("s"), vec![turn(2)], Some("unknown")).unwrap_err().outcome,
        Outcome::Unknown
    );
    assert_eq!(engine.checkpoint().unwrap(), watermark);
    drop(engine);
    let engine = open(Arc::new(disk.crash().unwrap()), opts);
    let window = engine.window(&scope("s"), WindowQuery::Last(10)).unwrap();
    if window.len() == 1 {
        assert_deduplicated(
            &receipt,
            engine.append(&scope("s"), vec![turn(1)], Some("original")).unwrap(),
            "retry",
        );
    } else {
        assert_eq!(
            engine.append(&scope("s"), vec![turn(2)], Some("unknown")).unwrap().seqs,
            vec![2]
        );
    }
}

#[test]
fn cold_window_and_pin_io_do_not_hold_the_writer_state_lock() {
    for append in [false, true] {
        eprintln!("case: append={append:?}");
        let io = Arc::new(HookIo::sim(18));
        let engine = open(io.clone(), EngineOptions { hot_page_bytes: 256, ..options() });
        for i in 0..3 {
            engine.append(&scope(&i.to_string()), vec![turn(i)], None).unwrap();
            engine.checkpoint().unwrap();
        }
        let (entered, blocked) = std::sync::mpsc::sync_channel(1);
        let (release, resumed) = std::sync::mpsc::sync_channel(1);
        // The first checkpoint read parks until the test releases it.
        let gate = Mutex::new(Some((entered, resumed)));
        io.add(move |op, next| {
            let cold = matches!(op, Op::Read(..)) && op.name().starts_with("ckpt-");
            let parked = cold.then(|| gate.lock().unwrap().take()).flatten();
            if let Some((entered, resumed)) = parked {
                entered.send(()).unwrap();
                resumed.recv().unwrap();
            }
            next()
        });
        std::thread::scope(|threads| {
            threads.spawn(|| {
                if append {
                    engine.append(&scope("0"), vec![turn(10)], None).unwrap();
                } else {
                    engine.window(&scope("0"), WindowQuery::Last(1)).unwrap();
                }
            });
            blocked.recv_timeout(Duration::from_secs(2)).unwrap();
            let (done, completed) = std::sync::mpsc::sync_channel(1);
            let engine = &engine;
            threads.spawn(move || {
                let result = engine.append(&scope("2"), vec![turn(11)], None);
                let _ = done.send(result);
            });
            let result = completed.recv_timeout(Duration::from_secs(1));
            if result.is_ok() {
                for i in 0..6 {
                    engine.append(&scope("2"), vec![turn(20 + i)], None).unwrap();
                    engine.checkpoint().unwrap();
                }
            }
            release.send(()).unwrap();
            assert!(result.is_ok(), "writer stalled behind cold I/O, append={append}");
            result.unwrap().unwrap();
        });
    }
}

#[test]
fn open_sweeps_orphan_checkpoints_only_after_taking_the_tape_lock() {
    let disk = Arc::new(SimDisk::new(19));
    let engine = open(disk.clone(), options());
    engine.append(&scope("s"), vec![turn(1)], None).unwrap();
    engine.checkpoint().unwrap();
    for path in ["db/ckpt-orphan.zdb", "db/CHECKPOINT.next"] {
        let id = disk.create(std::path::Path::new(path)).unwrap();
        disk.write_at(id, 0, b"partial").unwrap();
        disk.sync_data(id).unwrap();
        disk.close(id).unwrap();
    }
    disk.sync_dir(std::path::Path::new("db")).unwrap();
    assert!(
        Engine::open_with(disk.clone(), Arc::new(SimClock::default()), "db", options()).is_err()
    );
    assert!(disk.exists(std::path::Path::new("db/ckpt-orphan.zdb")).unwrap());
    drop(engine);
    let engine = open(disk.clone(), options());
    assert!(!disk.exists(std::path::Path::new("db/ckpt-orphan.zdb")).unwrap());
    assert!(!disk.exists(std::path::Path::new("db/CHECKPOINT.next")).unwrap());
    assert_eq!(engine.metrics().unwrap().checkpoint_files, 1);
    assert_eq!(engine.window(&scope("s"), WindowQuery::Last(1)).unwrap()[0].seq, 1);
}

#[test]
fn checkpoints_advance_while_tiny_budget_writers_append_continuously() {
    let engine =
        open(Arc::new(SimDisk::new(20)), EngineOptions { hot_page_bytes: 2048, ..options() });
    let (barrier, active) = (Barrier::new(5), std::sync::atomic::AtomicUsize::new(4));
    let (acked, retries) = (AtomicU64::new(0), AtomicU64::new(0));
    std::thread::scope(|threads| {
        for thread in 0..4 {
            let (engine, barrier, active, acked, retries) =
                (&engine, &barrier, &active, &acked, &retries);
            threads.spawn(move || {
                barrier.wait();
                // Other writers can refill the 2 KiB budget between the engine's flush and its
                // one retry. That rejection is documented back-pressure, so retry it like a client.
                let result = (|| -> Result<(), zerodb_core::EngineError> {
                    for i in 0..128 {
                        let session = format!("{thread}-{i}");
                        let mut attempts = 0;
                        let receipt = loop {
                            match engine.append(&scope(&session), vec![turn(i)], Some("key")) {
                                Err(error)
                                    if matches!(
                                        error.kind,
                                        ErrorKind::ResourceExhausted("hot_pages")
                                    ) && attempts < 32 =>
                                {
                                    assert_eq!(error.outcome, Outcome::NotCommitted);
                                    attempts += 1;
                                    retries.fetch_add(1, Ordering::Relaxed);
                                    std::thread::yield_now();
                                }
                                result => break result?,
                            }
                        };
                        acked.fetch_max(receipt.last_lsn, Ordering::Release);
                    }
                    Ok(())
                })();
                active.fetch_sub(1, Ordering::Release);
                result.unwrap();
            });
        }
        threads.spawn(|| {
            barrier.wait();
            let mut advances = 0;
            let mut previous = 0;
            while active.load(Ordering::Acquire) > 0 {
                let target = acked.load(Ordering::Acquire);
                let watermark = engine.checkpoint().unwrap();
                assert!(
                    watermark >= target,
                    "checkpoint {watermark} missed acknowledged boundary {target}"
                );
                if watermark > previous {
                    advances += 1;
                    previous = watermark;
                }
                std::thread::sleep(Duration::from_micros(100));
            }
            assert!(advances > 1, "checkpoints did not advance under load");
        });
    });
    eprintln!("hot_pages retries: {}", retries.load(Ordering::Relaxed));
    assert_eq!(engine.checkpoint().unwrap(), acked.load(Ordering::Acquire));
    for thread in 0..4 {
        for i in 0..128 {
            assert_eq!(
                engine.window(&scope(&format!("{thread}-{i}")), WindowQuery::Last(1)).unwrap()[0]
                    .seq,
                1
            );
        }
    }
}

#[test]
fn stdio_eight_threads_sixty_four_sessions_reopen_equals_oracle() {
    let dir = TestDir::new();
    let engine = Engine::open(&dir.0, options()).unwrap();
    let oracle = Mutex::new(HashMap::<u64, Vec<(u64, Vec<u8>)>>::new());
    let barrier = Barrier::new(8);
    std::thread::scope(|threads| {
        for thread in 0..8 {
            let (engine, oracle, barrier) = (&engine, &oracle, &barrier);
            threads.spawn(move || {
                barrier.wait();
                for round in 0..32 {
                    let session = thread * 8 + round % 8;
                    let value = thread * 1000 + round;
                    let s = scope(&session.to_string());
                    let receipt =
                        engine.append(&s, vec![turn(value)], Some(&round.to_string())).unwrap();
                    let recent = engine.window(&s, WindowQuery::Last(1)).unwrap();
                    assert_eq!(recent[0].seq, receipt.seqs[0]);
                    oracle
                        .lock()
                        .unwrap()
                        .entry(session)
                        .or_default()
                        .push((receipt.seqs[0], value.to_le_bytes().to_vec()));
                }
            });
        }
    });
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = Engine::open(&dir.0, options()).unwrap();
    for (session, mut values) in oracle.into_inner().unwrap() {
        values.sort_by_key(|v| v.0);
        let window = engine.window(&scope(&session.to_string()), WindowQuery::Last(100)).unwrap();
        assert_eq!(window.iter().map(|t| (t.seq, t.content.clone())).collect::<Vec<_>>(), values);
    }
    drop(engine);
}

#[test]
fn engine_metrics_expose_eviction_without_changing_the_index_budget() {
    let opts = EngineOptions {
        tape: TapeOptions { segment_size: 8192, lsn_index_bytes: 12 * 1024, ..options().tape },
        ..options()
    };
    let engine = open(Arc::new(SimDisk::new(78)), opts);
    assert_eq!(engine.metrics().unwrap().tape_index.page_evictions, 0);
    for _ in 0..20 {
        engine.append(&scope("s"), vec![turn(u64::from_le_bytes([7; 8])); 64], None).unwrap();
    }
    let index = engine.metrics().unwrap().tape_index;
    assert!(index.page_evictions > 0 && index.dirty_page_evictions > 0, "{index:?}");
    assert_eq!(index.budget_bytes, 12 * 1024);
    assert!(index.resident_bytes <= index.budget_bytes);
}

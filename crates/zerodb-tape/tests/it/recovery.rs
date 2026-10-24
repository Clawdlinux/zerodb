use crate::support::wire::{damage, setup, transaction, write, Damage};
use crate::support::*;
use std::ops::RangeInclusive;
use std::sync::Arc;
use zerodb_tape::sim::frame::{self, Kind};
use zerodb_tape::sim::{recover, DataDir, SimClock, WriterCore};
use zerodb_tape::{Committed, Error, HookIo, Io, NoopStateMachine, SimDisk, Tape};

/// Transactions chained from segment 1, offset 0.
fn chain(txns: &[(u64, u64)]) -> Vec<u8> {
    let mut at = (1, 0);
    txns.iter()
        .flat_map(|&(lsn, txn)| {
            let (bytes, next) = transaction(lsn, txn, at);
            at = next;
            bytes
        })
        .collect()
}

fn gap(io: &SimDisk, dir: &mut DataDir) {
    two_segments(io, dir, false);
}

fn partial(io: &SimDisk, dir: &mut DataDir) {
    two_segments(io, dir, true);
}

/// Transaction 1 in segment 1, then transaction 2 or its first 8 bytes in a new segment 2.
fn two_segments(io: &SimDisk, dir: &mut DataDir, partial_header: bool) {
    dir.prepare_next_segment(dir.segment_size()).unwrap();
    let (mut last, _) = transaction(2, 2, (2, 0));
    if partial_header {
        last[..4].fill(0);
        last.truncate(8);
    }
    write(io, 1, 0, &chain(&[(1, 1)]));
    write(io, 2, 0, &last);
}

/// Transaction 1 with one footer field flipped and its checksum still valid.
fn bad_footer(io: &SimDisk, field: usize) {
    let bytes = chain(&[(1, 1)]);
    let used = frame::decode(&bytes, 1, 0).unwrap().1;
    let (mut end, _) = frame::decode(&bytes[used..], 1, used as u64).unwrap();
    end.payload[field] ^= 1;
    write(io, 1, 0, &[&bytes[..used], &frame::encode(&end, 1, used as u64).unwrap()].concat());
}

fn chained(io: &SimDisk, txns: &[(u64, u64)]) {
    write(io, 1, 0, &chain(txns));
}

/// Transaction 1, then transaction 2 without its last 10 bytes.
fn torn(io: &SimDisk) {
    let bytes = chain(&[(1, 1), (2, 2)]);
    write(io, 1, 0, &bytes[..bytes.len() - 10]);
}

fn duplicate(io: &SimDisk, _: &mut DataDir) {
    chained(io, &[(1, 1), (1, 2)]);
}

fn lsn_gap(io: &SimDisk, dir: &mut DataDir) {
    let second = dir.prepare_next_segment(1024).unwrap();
    write(io, 1, 0, &transaction(u64::MAX, 1, (1, 0)).0);
    let file = io.open(&dir.segment_path(second)).unwrap();
    io.set_len(file, 1025).unwrap();
    io.sync_data(file).unwrap();
    io.close(file).unwrap();
}

fn flipped(io: &SimDisk) {
    chained(io, &[(1, 1)]);
    let file = io.open(&seg(1)).unwrap();
    io.flip_bit(file, 105).unwrap();
    io.close(file).unwrap();
}

fn later_segments(io: &SimDisk, dir: &mut DataDir) {
    dir.prepare_next_segment(1024).unwrap();
    torn(io);
    write(io, 2, 0, &transaction(3, 3, (2, 0)).0);
}

type Prepare = fn(&SimDisk, &mut DataDir);
/// Recovered durable LSN and truncated LSNs, or the corrupt LSN.
type Expect = Result<(u64, Option<RangeInclusive<u64>>), u64>;

#[test]
fn recovery_keeps_proved_records_and_truncates_unproved_tails() {
    let big = (frame::MAX_FRAME_SIZE + frame::HEADER_LEN + 44 + 1024) as u64;
    let cases: [(&str, u64, u64, Prepare, Expect); 17] = [
        ("dense LSNs", 1024, 2, |io, _| chained(io, &[(1, 1), (2, 2)]), Ok((2, None))),
        ("proved oversized gap", big, 2, gap, Err(2)),
        ("unproved oversized tail", big, 0, gap, Ok((1, Some(2..=2)))),
        ("proved partial length", big, 2, partial, Err(2)),
        ("unproved partial length", big, 0, partial, Ok((1, Some(2..=2)))),
        ("unproved LSN gap", 1024, 0, lsn_gap, Ok((0, Some(1..=u64::MAX)))),
        ("proved LSN gap", 1024, u64::MAX, lsn_gap, Err(1)),
        ("missing footer", 1024, 0, |io, _| torn(io), Ok((1, Some(2..=2)))),
        ("record damage", 1024, 1, |io, _| flipped(io), Err(1)),
        ("missing segment", 1024, 1, |io, _| io.remove(&seg(1)).unwrap(), Err(1)),
        ("early end", 1024, 1, |io, _| write(io, 1, 0, &[0; 8]), Err(1)),
        ("first LSN binding", 1024, 0, |io, _| bad_footer(io, 8), Ok((0, Some(1..=1)))),
        ("synced LSN binding", 1024, 0, |io, _| bad_footer(io, 16), Ok((0, Some(1..=1)))),
        ("records checksum", 1024, 0, |io, _| bad_footer(io, 28), Ok((0, Some(1..=1)))),
        ("unproved duplicate", 1024, 0, duplicate, Ok((1, Some(2..=2)))),
        ("proved duplicate", 1024, 2, duplicate, Err(2)),
        ("discarded later segments", 1024, 0, later_segments, Ok((1, Some(2..=3)))),
    ];
    for (case, size, superblock, prepare, expected) in cases {
        let io = Arc::new(SimDisk::new(1));
        let mut dir = DataDir::open(io.clone(), "db", None, size).unwrap();
        dir.prepare_next_segment(size).unwrap();
        prepare(&io, &mut dir);
        dir.install_superblock(superblock).unwrap();
        let actual = match recover(&mut dir, size) {
            Ok(state) => {
                assert_eq!(state.report.segments, 1, "{case}: later segments removed");
                Ok((state.report.durable_lsn, state.report.truncated.map(|(range, _)| range)))
            }
            Err(Error::CorruptionDetected { lsn, .. }) => Err(lsn),
            Err(error) => panic!("{case}: {error}"),
        };
        assert_eq!(actual, expected, "{case}");
        let Ok((durable, _)) = actual else { continue };
        // Repairs are durable. Two later opens see a clean tail behind the new boundary.
        dir.install_superblock(durable).unwrap();
        drop(dir);
        let crash = image(&io);
        for reopen in 0..2 {
            let mut dir = DataDir::open(crash.clone(), "db", None, size).unwrap();
            let report = recover(&mut dir, size).unwrap().report;
            let state = (report.durable_lsn, report.segments, report.truncated);
            assert_eq!(state, (durable, 1, None), "{case}, reopen={reopen}");
        }
    }
}

#[test]
fn truncation_resumes_at_the_cut_and_reopens_twice_after_new_ack() {
    let cases =
        [3, 5].into_iter().flat_map(|n| [(n, false, false), (n, false, true), (n, true, true)]);
    for (count, empty, offset_zero) in cases {
        let case = format!("count={count}, empty={empty}, offset_zero={offset_zero}");
        let (io, mut dir) = setup();
        for _ in 1..count {
            dir.prepare_next_segment(1024).unwrap();
        }
        write(&io, 1, 0, &chain(&[(1, 1)]));
        dir.install_superblock(1).unwrap();
        let (mut last, mut cut) = (1, 0);
        if !offset_zero {
            let (prefix, _) = transaction(2, 2, (2, 0));
            write(&io, 2, 0, &prefix);
            (last, cut) = (2, prefix.len() as u64);
        }
        if !empty {
            write(&io, 2, cut, &[1; 8]);
        }
        for id in 3..=count {
            let file = io.open(&seg(id)).unwrap();
            io.write_at(file, 0, &transaction(last + id - 2, id, (id, 0)).0).unwrap();
            io.close(file).unwrap();
        }
        drop(dir);
        let clock = clock();
        let (mut core, report) =
            WriterCore::open(io.clone(), clock.clone(), "db", opts(1024)).unwrap();
        assert_eq!((report.durable_lsn, report.segments), (last, 2), "{case}");
        assert!(report.truncated.is_some(), "{case}");
        let committed = commit(&mut core, vec![record(Kind::Turn, 1, b"after repair")]).unwrap();
        let Committed::Durable { last_lsn, .. } = committed;
        assert_eq!(last_lsn, last + 1, "{case}");
        clock.advance(1_000_000_000);
        core.update_superblock().unwrap();
        drop(core);
        for _ in 0..2 {
            let (core, report) =
                WriterCore::open(image(&io), clock.clone(), "db", opts(1024)).unwrap();
            assert!(report.truncated.is_none(), "{case}");
            assert_eq!(&core.read(1).unwrap().unwrap().payload, b"hello", "{case}");
            let repaired = core.read(last + 1).unwrap().unwrap().payload;
            assert_eq!(repaired, b"after repair", "{case}");
        }
        let segments = DataDir::open(io.clone(), "db", None, 1024).unwrap().segments().to_vec();
        assert_eq!(segments, [1, 2], "{case}");
        assert!((3..=count).all(|id| !io.exists(&seg(id)).unwrap()), "{case}");
    }
}

fn at(images: &[(String, SimDisk)], event: &str) -> usize {
    images.iter().position(|(e, _)| e == event).unwrap()
}

#[test]
fn manifest_is_durable_before_tail_files_are_removed() {
    for (case, fail_manifest) in [("trim", false), ("failed manifest sync", true)] {
        let io = Arc::new(HookIo::sim(1));
        let mut dir = DataDir::open(io.clone(), "db", None, 1024).unwrap();
        for _ in 0..3 {
            dir.prepare_next_segment(1024).unwrap();
        }
        write(&io.inner, 1, 0, &chain(&[(1, 1)]));
        dir.install_superblock(1).unwrap();
        write(&io.inner, 2, 0, &[1; 8]);
        let images = io.capture();
        if fail_manifest {
            io.fail_nth_sync(3, data_sync);
            assert!(recover(&mut dir, 1024).is_err(), "{case}");
            assert_eq!(dir.segments(), &[1, 2, 3], "{case}");
            assert!(io.exists(&seg(3)).unwrap(), "{case}");
        } else {
            recover(&mut dir, 1024).unwrap();
            let images = images.lock().unwrap();
            let installed = at(&images, "dir-sync db");
            assert!(installed < at(&images, "remove db/seg-0000000000000003.zdb"), "{case}");
            // A crash after the manifest trim leaves an orphan. The next open removes it durably.
            let image = Arc::new(images[installed].1.clone());
            assert!(image.exists(&seg(3)).unwrap(), "{case}");
            drop(DataDir::open(image.clone(), "db", None, 1024).unwrap());
            assert!(!image.exists(&seg(3)).unwrap(), "{case}: orphan removed before recovery");
            let crash = Arc::new(image.crash().unwrap());
            assert!(!crash.exists(&seg(3)).unwrap(), "{case}: orphan cleanup is durable");
            let (tape, report) = Tape::open(crash, clock(), "db", opts(1024)).unwrap();
            assert_eq!(report.durable_lsn, 1, "{case}");
            assert_eq!(&tape.read(1).unwrap().unwrap().payload, b"hello", "{case}");
            for (event, image) in images.iter() {
                for reopen in 0..2 {
                    let mut dir = DataDir::open(Arc::new(image.clone()), "db", None, 1024).unwrap();
                    let durable = recover(&mut dir, 1024).unwrap().report.durable_lsn;
                    assert_eq!(durable, 1, "{case}: crash after {event}, reopen={reopen}");
                }
            }
        }
        drop(dir);
        assert_eq!(io.open_files(), 0, "{case}");
    }
}

#[test]
fn every_operator_cut_crash_image_reopens_without_a_missing_durable_prefix() {
    let io = Arc::new(HookIo::sim(6));
    let (mut core, _) = open(io.clone(), opts(128));
    for _ in 0..3 {
        commit(&mut core, vec![record(Kind::Turn, 1, [7; 8])]).unwrap();
    }
    core.shutdown().unwrap();
    drop(core);
    let images = io.capture();
    Tape::recover_truncate_at_with(io.clone(), "db", 1).unwrap();
    let images = images.lock().unwrap();
    let data_synced = at(&images, "sync db/seg-0000000000000001.zdb");
    assert!(data_synced < at(&images, "rename db/SUPERBLOCK.tmp db/SUPERBLOCK"));
    for (index, (event, image)) in images.iter().enumerate() {
        let (_, report) = Tape::open(Arc::new(image.clone()), clock(), "db", opts(128))
            .unwrap_or_else(|error| panic!("crash after {event}: {error}"));
        let allowed: &[u64] = if index >= data_synced { &[1] } else { &[1, 3] };
        assert!(allowed.contains(&report.durable_lsn), "crash after {event}");
    }
}

/// A writer that committed one transaction per entry, with that many records each.
fn written(
    seed: u64,
    size: u64,
    now: u64,
    txns: &[usize],
) -> (Arc<SimDisk>, WriterCore<NoopStateMachine>) {
    let disk = Arc::new(SimDisk::new(seed));
    let clock = Arc::new(SimClock::new(now));
    let (mut core, _) = WriterCore::open(disk.clone(), clock, "db", opts(size)).unwrap();
    for &count in txns {
        commit(&mut core, vec![record(Kind::Turn, 1, [7; 8]); count]).unwrap();
    }
    (disk, core)
}

#[test]
fn footer_lookahead_separates_proved_corruption_from_tail_damage() {
    use Damage::*;
    // Durable LSN and truncated LSNs after open, or the corrupt LSN.
    type Opened = Result<(u64, RangeInclusive<u64>), u64>;
    let mut cases: Vec<(String, u64, Vec<usize>, Damage, Opened)> = vec![
        ("unproved record damage".into(), 1024, vec![1], Flip(40), Ok((0, 1..=1))),
        ("zeroed multi-record footer".into(), 1024, vec![3, 1], ZeroFooter(144), Err(3)),
    ];
    for (size, segment, offset) in [(128, 2, 0), (1024, 1, 120)] {
        for offset in [0, 4, 40, 76] {
            let case = format!("proved damage: size={size}, offset={offset}");
            cases.push((case, size, vec![1, 1, 1], Flip(offset), Err(1)));
        }
        for fault in [GarbageFooter, MisdirectedFooter(segment, offset)] {
            let case = format!("replaced footer: size={size}, fault={fault:?}");
            cases.push((case, size, vec![1, 1], fault, Err(2)));
        }
    }
    for field in [8, 20, 48, 56, 64, 68] {
        let case = format!("last pending LSN: field={field}");
        cases.push((case, 1024, vec![3, 1], Flip(144 + field), Err(3)));
    }
    for field in [0, 8, 16, 24, 28] {
        let case = format!("CRC-valid but invalid proof: field={field}");
        cases.push((case, 1024, vec![1, 1], InvalidProof(field), Ok((0, 1..=2))));
    }
    for (case, size, txns, fault, expected) in cases {
        let disk = written(73, size, 0, &txns).0;
        damage(&disk, size, fault, &case);
        let actual = match Tape::open(disk, clock(), "db", opts(size)) {
            Ok((_, report)) => {
                assert_eq!(report.truncated.as_ref().map(|t| t.1), Some(size), "{case}");
                Ok((report.durable_lsn, report.truncated.unwrap().0))
            }
            Err(Error::CorruptionDetected { lsn, .. }) => Err(lsn),
            Err(error) => panic!("{case}: {error}"),
        };
        assert_eq!(actual, expected, "{case}");
    }
}

#[test]
fn operator_cut_rejects_invalid_targets_and_corrupt_prefixes() {
    enum Expected {
        Corruption(u64),
        Invalid(&'static [&'static str]),
    }
    use Expected::*;
    let gap_size = (frame::MAX_FRAME_SIZE + frame::HEADER_LEN + 32 + 241) as u64;
    fn zero_gap(disk: &Arc<SimDisk>, size: u64) {
        DataDir::open(disk.clone(), "db", None, size).unwrap().prepare_next_segment(size).unwrap();
    }
    type Case = (&'static str, u64, &'static [usize], u64, fn(&Arc<SimDisk>, u64), Expected);
    let cases: [Case; 5] = [
        ("interior LSN", 1024, &[1, 3], 2, |_, _| {}, Invalid(&["transaction", "1", "4"])),
        (
            "corrupt prefix",
            1024,
            &[1, 1],
            2,
            |d, s| damage(d, s, Damage::Flip(40), ""),
            Corruption(1),
        ),
        ("empty footer", 1024, &[], 1, |d, s| damage(d, s, Damage::EmptyFooter, ""), Corruption(1)),
        ("zero gap", gap_size, &[1, 1], 3, zero_gap, Corruption(3)),
        (
            "beyond log",
            1024,
            &[1, 1],
            3,
            |_, _| {},
            Invalid(&["cut LSN 3 exceeds last valid cut point 2"]),
        ),
    ];
    for (case, size, txns, cut, fault, expected) in cases {
        let (disk, mut core) = written(74, size, 0, txns);
        core.shutdown().unwrap();
        drop(core);
        fault(&disk, size);
        let error = Tape::recover_truncate_at_with(disk.clone(), "db", cut).unwrap_err();
        match expected {
            Corruption(lsn) => assert!(
                matches!(&error, Error::CorruptionDetected { lsn: actual, .. } if *actual == lsn),
                "{case}: {error:?}"
            ),
            Invalid(messages) => {
                assert!(matches!(error, Error::InvalidFrame(_)), "{case}");
                let text = error.to_string();
                assert!(messages.iter().all(|m| text.contains(m)), "{case}: {error}");
            }
        }
        // Cutting everything needs no valid prefix.
        let report = Tape::recover_truncate_at_with(disk, "db", 0).unwrap();
        assert_eq!(report.cut_lsn, Some(0), "{case}");
    }
}

#[test]
fn operator_cut_discards_oversized_tail_and_bounds_the_superblock() {
    for (floor, (cut, kept_bytes, commit_time)) in [0, 1, 5]
        .into_iter()
        .flat_map(|f| [(0, 0, 1), (1, 120, 101), (4, 256 + 216, 102)].map(|c| (f, c)))
    {
        let case = format!("floor={floor}, cut={cut}");
        let (disk, core) = written(76, 256, 100, &[1, 3, 1]);
        drop(core);
        let mut dir = DataDir::open(disk.clone(), "db", None, 256).unwrap();
        dir.install_superblock(floor).unwrap();
        let id = dir.prepare_next_segment(256).unwrap();
        let tail = dir.segment_path(id);
        let file = disk.open(&tail).unwrap();
        disk.set_len(file, 257).unwrap();
        disk.sync_data(file).unwrap();
        disk.close(file).unwrap();
        drop(dir);
        let report = Tape::recover_truncate_at_with(disk.clone(), "db", cut).unwrap();
        assert_eq!(report.cut_lsn, Some(cut), "{case}");
        assert!(report.lsns_may_be_reused, "{case}");
        assert_eq!(report.truncated.as_ref().unwrap().0, cut + 1..=5, "{case}");
        assert_eq!(report.dropped_bytes, 1025 - kept_bytes, "{case}");
        assert!(!disk.exists(&tail).unwrap(), "{case}");
        let superblock = DataDir::open(disk.clone(), "db", None, 256).unwrap().superblock();
        assert_eq!(superblock, floor.min(cut), "{case}");
        let (mut core, report) = open(disk, opts(256));
        assert_eq!(report.durable_lsn, cut, "{case}");
        commit(&mut core, vec![record(Kind::Turn, 1, [7; 8])]).unwrap();
        assert_eq!(core.durable_lsn(), cut + 1, "{case}");
        let time = core.read(cut + 1).unwrap().unwrap().header.commit_time_ns;
        assert_eq!(time, commit_time, "{case}");
    }
}

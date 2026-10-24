use crate::support::*;
use std::sync::Arc;
use zerodb_tape::sim::WriterCore;
use zerodb_tape::{Kind, SimDisk, TapeOptions};

#[test]
fn paged_offsets_cross_segments_recover_and_reload_within_budget() {
    let small = TapeOptions { lsn_index_bytes: 1, ..TapeOptions::default() };
    let error = WriterCore::open(Arc::new(SimDisk::new(12)), clock(), "db", small).unwrap_err();
    assert!(error.to_string().contains("index"), "a budget below one page is rejected: {error}");
    let disk = Arc::new(SimDisk::new(11));
    let opts = TapeOptions { lsn_index_bytes: 12 * 1024, ..opts(4096) };
    let budget = opts.lsn_index_bytes;
    let (mut core, _) = open(disk.clone(), opts.clone());
    let expected: Vec<_> = (0..30 * 64u64).map(|n| n.to_le_bytes().to_vec()).collect();
    for group in expected.chunks(64) {
        commit(&mut core, group.iter().map(|c| record(Kind::Turn, 1, c)).collect()).unwrap();
        assert!(core.index_stats().unwrap().resident_bytes <= budget);
    }
    let stats = core.index_stats().unwrap();
    assert!(stats.page_evictions > 0 && stats.dirty_page_evictions > 0, "{stats:?}");
    disk.enospc_after_bytes(0).unwrap();
    assert_eq!(core.read(1).unwrap().unwrap().payload, expected[0], "full disk keeps reads");
    let full = core.index_stats().unwrap();
    let dirty = stats.dirty_page_evictions;
    assert_eq!(full.dirty_page_evictions, dirty, "failed write-back keeps the page dirty");
    assert!(full.resident_bytes <= budget + 4224);
    disk.enospc_after_bytes(usize::MAX).unwrap();
    assert_eq!(core.read(1920).unwrap().unwrap().payload, expected[1919]);
    let reloads = core.index_stats().unwrap().page_reloads;
    for lsn in [1, 1920, 256, 1025, 512, 1919, 1] {
        assert_eq!(core.read(lsn).unwrap().unwrap().payload, expected[lsn as usize - 1]);
        assert!(core.index_stats().unwrap().resident_bytes <= budget, "lsn={lsn}");
    }
    assert!(core.index_stats().unwrap().page_reloads > reloads);
    assert!(core.read(0).unwrap().is_none() && core.read(1921).unwrap().is_none());
    core.shutdown().unwrap();
    drop(core);
    let (core, report) = open(image(&disk), opts);
    assert_eq!(report.durable_lsn, 1920);
    assert!(report.segments > 1);
    for (index, content) in expected.iter().enumerate().rev() {
        let lsn = index as u64 + 1;
        assert_eq!(core.read(lsn).unwrap().unwrap().payload, *content, "lsn={lsn}");
        assert!(core.index_stats().unwrap().resident_bytes <= budget, "lsn={lsn}");
    }
}

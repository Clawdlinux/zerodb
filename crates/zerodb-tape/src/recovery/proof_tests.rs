#![allow(clippy::unwrap_used)]

use super::*;

/// `count` records of transaction 2, then a footer that claims the last `closed` of them.
fn run(count: usize, closed: usize, bad_crc: bool) -> Vec<u8> {
    let header = |kind, lsn| frame::Header {
        kind,
        version: 2,
        flags: 0,
        txn: 2,
        lsn,
        commit_time_ns: 2,
        tenant_id: u32::from(kind == Kind::Turn),
    };
    let (mut bytes, mut crcs) = (Vec::new(), Vec::new());
    for lsn in 1..=count as u64 {
        let record = Frame { header: header(Kind::Turn, lsn), payload: vec![7; 8] };
        let encoded = frame::encode(&record, 1, bytes.len() as u64).unwrap();
        crcs.extend_from_slice(&encoded[..4]);
        bytes.extend(encoded);
    }
    let first = count - closed + 1;
    let footer = Footer {
        txn: 2,
        first_lsn: first as u64,
        synced_lsn: u64::from(first > 1),
        record_count: closed as u32,
        records_crc: crate::crc32c::crc32c(&crcs[(first - 1) * 4..]) ^ u32::from(bad_crc),
    };
    let end =
        Frame { header: header(Kind::Footer, count as u64), payload: footer.encode().to_vec() };
    bytes.extend(frame::encode(&end, 1, bytes.len() as u64).unwrap());
    bytes
}

#[test]
fn footer_proofs_do_not_rescan_every_record_suffix() {
    // (records, records the footer closes, bad checksum, proved)
    for (count, closed, bad_crc, proved) in [
        (1024, 1024, false, false),
        (1024, 1024, true, false),
        (16, 15, false, true),
        (1400, 1024, false, true),
    ] {
        let bytes = run(count, closed, bad_crc);
        let mut decoded = 0;
        let result = scan_proof(&bytes, 1, 0, &Recovered::default(), || decoded += 1);
        assert_eq!(result, proved, "count={count}, closed={closed}, bad_crc={bad_crc}");
        assert!(decoded <= count + 128, "count={count}: decoded {decoded} frames");
    }
}

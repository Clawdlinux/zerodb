use crate::support::wire::*;
use proptest::prelude::*;
use zerodb_tape::sim::frame::{
    self, DecodeError, Footer, Kind, HEADER_LEN, MAX_FRAME_SIZE, VERSION,
};
use zerodb_tape::Error;

proptest! {
    #[test]
    fn round_trip(
        kind in prop::sample::select(vec![1u8, 2, 3, 5, 6]),
        txn in any::<u64>(), lsn in any::<u64>(), time in any::<u64>(),
        tenant in any::<u32>(), content in prop::collection::vec(any::<u8>(), 0..8192),
        segment in any::<u64>(), offset in any::<u64>(),
    ) {
        let kind = Kind::try_from(kind).unwrap();
        let mut original = frame(kind, &content);
        original.header.txn = txn;
        original.header.lsn = lsn;
        original.header.commit_time_ns = time;
        original.header.tenant_id = tenant;
        if kind == Kind::Footer {
            original.payload = Footer {
                txn, first_lsn: lsn, synced_lsn: 0, record_count: 1, records_crc: tenant,
            }.encode().to_vec();
        }
        let encoded = frame::encode(&original, segment, offset).unwrap();
        let (decoded, consumed) = frame::decode(&encoded, segment, offset).unwrap();
        prop_assert_eq!(decoded, original);
        prop_assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn arbitrary_bytes_never_panic(data in prop::collection::vec(any::<u8>(), 0..4096)) {
        let _ = decode(&data);
    }
}

#[test]
fn fixed_header_layout_is_little_endian() {
    let original = frame(Kind::Turn, b"content");
    let bytes = encode(&original).unwrap();
    assert_eq!(HEADER_LEN, 40);
    assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize, bytes.len());
    assert_eq!(&bytes[8..12], &[1, 2, 0, 0]);
    assert_eq!(&bytes[12..20], &11u64.to_le_bytes());
    assert_eq!(&bytes[20..28], &22u64.to_le_bytes());
    assert_eq!(&bytes[28..36], &33u64.to_le_bytes());
    assert_eq!(&bytes[36..40], &44u32.to_le_bytes());
    assert_eq!(&bytes[40..], &original.payload);
    assert_eq!(VERSION, 2);
    assert_eq!(bytes.len(), HEADER_LEN + original.payload.len());
    assert_eq!(decode(&bytes).unwrap().0, original);
    for (case, segment, offset) in [("different segment", 8, 128), ("different offset", 7, 129)] {
        assert_eq!(frame::decode(&bytes, segment, offset), Err(DecodeError::BadCrc), "{case}");
    }
}

#[test]
fn footer_layout_and_payload_lengths_follow_the_wire_contract() {
    let footer =
        Footer { txn: 7, first_lsn: 11, synced_lsn: 10, record_count: 3, records_crc: 0x12345678 };
    let payload = footer.encode();
    assert_eq!(payload.len(), 32);
    assert_eq!(&payload[..8], &7u64.to_le_bytes());
    assert_eq!(&payload[8..16], &11u64.to_le_bytes());
    assert_eq!(&payload[16..24], &10u64.to_le_bytes());
    assert_eq!(&payload[24..28], &3u32.to_le_bytes());
    assert_eq!(&payload[28..], &0x12345678u32.to_le_bytes());
    assert_eq!(Footer::decode(&payload).unwrap(), footer);
    for (case, len) in [("short footer", 31), ("long footer", 33)] {
        assert!(Footer::decode(&vec![0; len]).is_err(), "{case}");
    }
}

#[test]
fn every_single_bit_flip_is_detected() {
    for content_len in [7, 8, 88] {
        let encoded = encode(&frame(Kind::Turn, &vec![3; content_len])).unwrap();
        for bit in 0..encoded.len() * 8 {
            let mut damaged = encoded.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            let detected =
                matches!(decode(&damaged), Err(DecodeError::BadCrc | DecodeError::BadLength));
            assert!(detected, "content_len={content_len}, bit={bit}");
        }
    }
}

#[test]
fn boundaries_versions_and_end_marker() {
    assert_eq!(decode(&[0; 8]), Err(DecodeError::EndOfData));
    assert_eq!(decode(&[]), Err(DecodeError::EndOfData));
    for length in 1..HEADER_LEN {
        if length != 8 {
            let mut bytes = vec![0; length];
            if length >= 8 {
                bytes[4..8].copy_from_slice(&(HEADER_LEN as u32).to_le_bytes());
            }
            assert_eq!(decode(&bytes), Err(DecodeError::Truncated));
        }
    }
    let original = frame(Kind::Turn, b"content");
    let mut bytes = encode(&original).unwrap();
    bytes[9] = 1;
    refresh_crc(&mut bytes);
    assert_eq!(decode(&bytes), Err(DecodeError::UnknownVersion));
    bytes[9] = VERSION;
    for kind in [0, 4, 7, 99] {
        bytes[8] = kind;
        refresh_crc(&mut bytes);
        assert_eq!(decode(&bytes), Err(DecodeError::BadLength), "kind={kind}");
    }
    let bytes = encode(&original).unwrap();
    assert_eq!(decode(&bytes[..bytes.len() - 1]), Err(DecodeError::BadLength));
    let mut concatenated = bytes.clone();
    concatenated.extend_from_slice(&bytes);
    assert_eq!(decode(&concatenated).unwrap(), (original, bytes.len()));
    // A version 1 frame with the legacy unbound checksum reports an unsupported format.
    let mut legacy = vec![0; 120];
    legacy[4..8].copy_from_slice(&120u32.to_le_bytes());
    legacy[8..10].copy_from_slice(&[1, 1]);
    let crc = zerodb_tape::sim::crc32c(&legacy[4..]);
    legacy[..4].copy_from_slice(&crc.to_le_bytes());
    assert_eq!(frame::decode(&legacy, 1, 0), Err(DecodeError::UnknownVersion));
}

#[test]
fn maximum_frame_size_and_invalid_metadata_are_enforced() {
    let mut original = frame(Kind::Turn, b"");
    original.payload.resize(MAX_FRAME_SIZE - HEADER_LEN, 0);
    assert_eq!(encode(&original).unwrap().len(), MAX_FRAME_SIZE);
    assert_eq!(decode(&encode(&original).unwrap()).unwrap().0, original);
    original.payload.push(0);
    assert!(matches!(encode(&original), Err(Error::BatchTooLarge)));
    let mut oversized = vec![0; HEADER_LEN];
    oversized[4..8].copy_from_slice(&((MAX_FRAME_SIZE + 1) as u32).to_le_bytes());
    assert_eq!(decode(&oversized), Err(DecodeError::BadLength));
    let mut invalid = frame(Kind::Turn, b"content");
    invalid.header.flags = 1;
    assert!(matches!(encode(&invalid), Err(Error::InvalidFrame(_))));
    invalid.header.flags = 0;
    invalid.header.version = 1;
    assert!(matches!(encode(&invalid), Err(Error::InvalidFrame(_))));
}

mod checksum {
    use proptest::prelude::*;
    use zerodb_tape::sim::{crc32c, Crc32c};

    #[test]
    fn known_vectors() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(&[0; 32]), 0x8A91_36AA);
    }

    proptest! {
        #[test]
        fn incremental_matches_one_shot(
            data in prop::collection::vec(any::<u8>(), 0..8192),
            splits in prop::collection::vec(any::<usize>(), 0..64),
        ) {
            let mut checksum = Crc32c::new();
            let mut start = 0;
            for split in splits {
                let end = start + split % (data.len() - start + 1);
                checksum.update(&data[start..end]);
                start = end;
            }
            checksum.update(&data[start..]);
            prop_assert_eq!(checksum.finish(), crc32c(&data));
        }

        #[test]
        fn slicing_matches_bitwise(data in prop::collection::vec(any::<u8>(), 0..4096)) {
            let mut crc = !0u32;
            for byte in &data {
                crc ^= u32::from(*byte);
                for _ in 0..8 {
                    crc = (crc >> 1) ^ (0x82F6_3B78 & 0u32.wrapping_sub(crc & 1));
                }
            }
            prop_assert_eq!(crc32c(&data), !crc);
        }
    }
}

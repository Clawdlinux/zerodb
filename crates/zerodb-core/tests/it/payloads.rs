use crate::support::fact;
use proptest::prelude::*;
use zerodb_core::forget::{Forget, Selector};
use zerodb_core::{fact, forget, turn, NewTurn, Role};

const MIB4: usize = 4 * 1024 * 1024;

proptest! {
    #[test]
    fn payloads_round_trip(
        a in ".{0,40}", b in ".{0,40}", c in ".{0,40}", n in any::<u64>(),
        content in prop::collection::vec(any::<u8>(), 0..4096),
        meta in prop::collection::vec(any::<u8>(), 0..1024),
        nums in prop::collection::vec(any::<u64>(), 0..30),
        key in prop::option::of(".{0,30}"), tag in 0u8..4, flag in any::<bool>(),
    ) {
        let value = fact::Fact {
            key: c.clone(), agent: a.clone(), content: content.clone(), meta: meta.clone(),
            source_refs: nums.clone(), idem_key: key.clone(), tombstone: flag,
        };
        prop_assert_eq!(fact::decode(&fact::encode(&value).unwrap()).unwrap(), value);
        let selector = match tag % 3 {
            0 => Selector::Session { agent: a.clone(), session: b.clone() },
            1 => Selector::FactKey(c),
            _ => Selector::Lsns(nums),
        };
        let value = Forget { selector, idem_key: key.clone(), up_to_lsn: flag.then_some(n) };
        prop_assert_eq!(forget::decode(&forget::encode(&value).unwrap()).unwrap(), value);
        let role = [Role::System, Role::User, Role::Assistant, Role::Tool][tag as usize];
        let value = NewTurn { role, content, meta };
        let bytes = turn::encode(&a, &b, n, &value, key.as_deref()).unwrap();
        // A wrong size estimate either regrows the buffer or over-reserves it.
        prop_assert_eq!(bytes.capacity(), bytes.len());
        let decoded = turn::decode(&bytes).unwrap();
        prop_assert_eq!((decoded.agent_id, decoded.session_id, decoded.seq), (a, b, n));
        prop_assert_eq!((decoded.turn, decoded.idem_key), (value, key));
    }

    #[test]
    fn garbage_never_panics(data in prop::collection::vec(any::<u8>(), 0..8192)) {
        let _ = fact::decode(&data);
        let _ = forget::decode(&data);
        let _ = turn::decode(&data);
    }
}

#[test]
fn payloads_reject_truncation_trailing_bytes_and_unknown_versions() {
    let forget = Forget {
        selector: Selector::FactKey("key".into()),
        idem_key: Some("retry".into()),
        up_to_lsn: Some(1),
    };
    let user = NewTurn { role: Role::User, content: b"hello".to_vec(), meta: vec![] };
    type Decodes = fn(&[u8]) -> bool;
    let cases: [(&str, Vec<u8>, Decodes); 3] = [
        ("fact", fact::encode(&fact()).unwrap(), |b| fact::decode(b).is_ok()),
        ("forget", forget::encode(&forget).unwrap(), |b| forget::decode(b).is_ok()),
        ("turn", turn::encode("a", "s", 4, &user, None).unwrap(), |b| turn::decode(b).is_ok()),
    ];
    for (case, mut data, ok) in cases {
        assert!(ok(&data), "{case}");
        assert!((0..data.len()).all(|n| !ok(&data[..n])), "{case}: truncated");
        data.push(0);
        assert!(!ok(&data), "{case}: trailing byte");
        data.pop();
        data[0] = 2;
        assert!(!ok(&data), "{case}: unknown version");
    }
    let mut data = fact::encode(&fact()).unwrap();
    for (case, at) in [("tombstone flag", data.len() - 1), ("retry key tag", 21)] {
        data[at] = 2;
        assert!(fact::decode(&data).is_err(), "{case}");
    }
}

#[test]
fn payload_limits_apply_to_every_field_in_both_directions() {
    type Set = fn(&mut fact::Fact, usize);
    let fields: [(&str, usize, usize, Set); 6] = [
        ("key", 1024, 1, |f, n| f.key = "k".repeat(n)),
        ("content", MIB4, 5, |f, n| f.content = vec![0; n]),
        ("meta", 65536, 9, |f, n| f.meta = vec![0; n]),
        ("agent", 255, 13, |f, n| f.agent = "a".repeat(n)),
        ("source refs", 1024, 17, |f, n| f.source_refs = vec![1; n]),
        ("retry key", 128, 22, |f, n| f.idem_key = Some("i".repeat(n))),
    ];
    for (field, max, length_offset, set) in fields {
        let mut value = fact();
        set(&mut value, max);
        let mut encoded = fact::encode(&value).unwrap();
        assert_eq!(fact::decode(&encoded).unwrap(), value, "fact {field}");
        set(&mut value, max + 1);
        assert!(fact::encode(&value).is_err(), "fact {field}");
        encoded[length_offset..length_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(fact::decode(&encoded).is_err(), "fact {field}: length past the limit");
    }

    let forget =
        |selector, idem_key| forget::encode(&Forget { selector, idem_key, up_to_lsn: None });
    let user = |content, meta| NewTurn {
        role: Role::User,
        content: vec![0; content],
        meta: vec![0; meta],
    };
    let turn = |agent: &str, session: &str, key: Option<&str>, value| {
        turn::encode(agent, session, 0, &value, key)
    };
    let (a, s, k) = ("a".repeat(256), "s".repeat(256), "k".repeat(129));
    let session = |agent: &str, session: &str| Selector::Session {
        agent: agent.into(),
        session: session.into(),
    };
    for (case, rejected) in [
        ("forget agent", forget(session(&a, ""), None).is_err()),
        ("forget session", forget(session("", &s), None).is_err()),
        ("forget fact key", forget(Selector::FactKey("k".repeat(1025)), None).is_err()),
        ("forget lsns", forget(Selector::Lsns(vec![1; 1025]), None).is_err()),
        ("forget retry key", forget(Selector::Lsns(vec![]), Some(k.clone())).is_err()),
        ("forget selector tag", forget::decode(&[1, 4]).is_err()),
        ("forget session length", forget::decode(&[1, 1, 0xff, 0xff, 0xff, 0xff]).is_err()),
        ("forget key length", forget::decode(&[1, 2, 0xff, 0xff, 0xff, 0xff]).is_err()),
        ("forget lsns length", forget::decode(&[1, 3, 0xff, 0xff, 0xff, 0xff]).is_err()),
        ("turn agent", turn(&a, "s", None, user(0, 0)).is_err()),
        ("turn session", turn("a", &s, None, user(0, 0)).is_err()),
        ("turn retry key", turn("a", "s", Some(&k), user(0, 0)).is_err()),
        ("turn meta", turn("a", "s", None, user(0, 65537)).is_err()),
        ("turn content", turn("a", "s", None, user(MIB4 + 1, 65536)).is_err()),
    ] {
        assert!(rejected, "{case}");
    }
    let max = turn(&a[1..], &s[1..], Some(&k[1..]), user(MIB4, 65536)).unwrap();
    assert_eq!(
        turn::decode(&max).unwrap().turn,
        user(MIB4, 65536),
        "every turn field at its limit"
    );
}

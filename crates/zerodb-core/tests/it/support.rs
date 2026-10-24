use std::sync::{Arc, Mutex};
use std::time::Duration;
use zerodb_core::{
    fact, Appended, Engine, EngineOptions, FactQuery, NewFact, NewTurn, Role, Scope,
};
use zerodb_tape::sim::SimClock;
use zerodb_tape::{HookIo, Io, Op, TapeOptions};

pub(crate) fn scope(session: &str) -> Scope {
    Scope { tenant: "tenant".into(), agent: "agent".into(), session: session.into() }
}

pub(crate) fn turn(value: u64) -> NewTurn {
    NewTurn { role: Role::User, content: value.to_le_bytes().to_vec(), meta: vec![] }
}

pub(crate) fn options() -> EngineOptions {
    EngineOptions {
        tape: TapeOptions { segment_size: 64 * 1024, ..TapeOptions::default() },
        checkpoint_interval: Duration::from_secs(3600),
        checkpoint_bytes: u64::MAX,
        ..EngineOptions::default()
    }
}

#[track_caller]
pub(crate) fn open(io: Arc<dyn Io>, opts: EngineOptions) -> Engine {
    Engine::open_with(io, Arc::new(SimClock::default()), "db", opts).unwrap()
}

/// Asserts a retry returned the original receipt and reported that it wrote nothing.
#[track_caller]
pub(crate) fn assert_deduplicated(original: &Appended, retried: Appended, case: &str) {
    assert!(retried.deduplicated, "{case}: a retry must report deduplicated");
    assert!(!original.deduplicated, "{case}: the original must be a fresh commit");
    assert_eq!(Appended { deduplicated: false, ..retried }, *original, "{case}");
}

/// Logs every later write as (thread name, file name).
pub(crate) fn writes(io: &HookIo) -> Arc<Mutex<Vec<(String, String)>>> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let out = log.clone();
    io.add(move |op, next| {
        if let Op::Write(..) = op {
            let thread = std::thread::current().name().unwrap_or("unnamed").to_owned();
            out.lock().unwrap().push((thread, op.name().to_owned()));
        }
        next()
    });
    log
}

pub(crate) fn query(prefix: &str, limit: usize, max_bytes: usize) -> FactQuery {
    FactQuery { prefix: prefix.into(), after: None, limit, max_bytes }
}

pub(crate) fn new_fact(key: &str, value: u64, agent: &str) -> NewFact {
    NewFact {
        key: key.into(),
        content: value.to_le_bytes().to_vec(),
        meta: vec![],
        agent: agent.into(),
    }
}

pub(crate) fn fact() -> fact::Fact {
    fact::Fact {
        key: String::new(),
        content: vec![],
        meta: vec![],
        agent: String::new(),
        source_refs: vec![],
        idem_key: None,
        tombstone: false,
    }
}

mod fs {
    const ROOT: &str = env!("CARGO_TARGET_TMPDIR");
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/support.rs"));
}
pub(crate) use fs::TestDir;

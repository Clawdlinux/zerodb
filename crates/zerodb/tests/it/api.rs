use crate::support::{append_body, serve, sim_engine, Dir, Via, TOKEN_A, TOKEN_B};
use serde_json::{json, Value};
use std::sync::Arc;
use zerodb::outcome::{self, Attempt, Merge};
use zerodb::serve::MAX_CONNECTIONS;
use zerodb_core::{EngineError, EngineOptions, ErrorKind, NewTurn, Role, Scope};
use zerodb_tape::{Error, Outcome, SimDisk};

fn seqs(window: &Value) -> Vec<u64> {
    window["turns"].as_array().unwrap().iter().map(|turn| turn["seq"].as_u64().unwrap()).collect()
}

/// What a retry must return unchanged: `seqs` and the LSNs for an append, `lsn` for a fact.
fn receipt(body: &Value) -> Value {
    json!([body["txn"], body["first_lsn"], body["last_lsn"], body["seqs"], body["lsn"]])
}

#[test]
fn writes_reads_and_retries_over_both_listeners() {
    let disk = SimDisk::new(1);
    let dir = Dir::new();
    let tune: fn(&mut EngineOptions) = |opts| opts.max_facts = 8;
    let engine = sim_engine(&disk, tune);
    let running = serve(engine.clone(), &dir, MAX_CONNECTIONS);
    let repeats = || {
        let (_, stats) = running.vias[1].call(None, None, "GET", "/v1/stats", None);
        stats["turn_repeats"].as_u64().unwrap()
    };
    let before = repeats();
    let mut firsts = Vec::new();
    for via in &running.vias {
        let name = via.name();
        let session = format!("s-{name}");
        let first_body = append_body(&session, "k1", "hello");
        let (status, first) = via.a("POST", "/v1/append", Some(&first_body));
        assert_eq!(status, 200, "{name}: {first}");
        assert_eq!(first["outcome"], "committed", "{name}");
        assert_eq!(first["deduplicated"], false, "{name}");
        assert_eq!(first["seqs"], json!([1]), "{name}");

        // I4: the writer reads its own write right after the ack.
        let window_of = |query: &str| {
            via.a("GET", &format!("/v1/window?agent=a&session={session}&{query}"), None)
        };
        let (status, window) = window_of("last=10");
        assert_eq!(status, 200, "{name}");
        let turn = &window["turns"][0];
        assert_eq!(turn["seq"], 1, "{name}");
        assert_eq!(turn["lsn"], first["first_lsn"], "{name}");
        assert_eq!(turn["content"], "hello", "{name}");
        assert_eq!(turn["role"], "user", "{name}");
        assert_eq!(turn["meta"], json!({"k": 1}), "{name}");
        assert!(turn["commit_time_ns"].as_u64().unwrap() > 0, "{name}");
        assert!(turn.get("lossy").is_none(), "{name}");
        assert_eq!(window["truncated"], false, "{name}");

        let (status, retry) = via.a("POST", "/v1/append", Some(&first_body));
        assert_eq!(status, 200, "{name}");
        assert_eq!(retry["deduplicated"], true, "{name}: retry");
        assert_eq!(receipt(&retry), receipt(&first), "{name}: retry keeps the receipt");

        let changed = append_body(&session, "k1", "changed");
        let (status, conflict) = via.a("POST", "/v1/append", Some(&changed));
        assert_eq!(status, 409, "{name}");
        let expected = json!({"outcome": "not_committed", "error": "idempotency_conflict"});
        assert_eq!(conflict, expected, "{name}");

        // Seqs 2 to 4 each hold 100 content bytes and 7 meta bytes.
        let mut more = append_body(&session, "k2", "");
        more["turns"] = ["b", "c", "d"]
            .map(|c| json!({"role": "assistant", "content": c.repeat(100), "meta": {"k": 1}}))
            .into();
        let (status, appended) = via.a("POST", "/v1/append", Some(&more));
        assert_eq!((status, &appended["seqs"]), (200, &json!([2, 3, 4])), "{name}");
        for (query, expected, truncated) in [
            ("last=10", vec![1, 2, 3, 4], false),
            ("last=5000", vec![1, 2, 3, 4], false),
            ("last=10&max_bytes=250", vec![3, 4], true),
            ("last=10&max_bytes=100", vec![], true),
            ("since_seq=2&limit=10&max_bytes=250", vec![2, 3], true),
            ("since_seq=1&limit=2", vec![1, 2], false),
        ] {
            let (status, window) = window_of(query);
            assert_eq!(status, 200, "{name} {query}");
            assert_eq!(seqs(&window), expected, "{name} {query}");
            assert_eq!(window["truncated"], truncated, "{name} {query}");
        }
        let (status, _) = window_of("last=1&since_seq=1");
        assert_eq!(status, 400, "{name}: last and since_seq together");
        // Engine limits answer 400 with a message that names the limit, not the input.
        let long = "x".repeat(256);
        for (row, field, value) in [
            ("no turns", "turns", json!([])),
            (
                "meta over 64 KiB",
                "turns",
                json!([{"role": "user", "content": "", "meta": {"m": "x".repeat(65536)}}]),
            ),
            ("agent over 255 bytes", "agent", json!(long)),
            ("session over 255 bytes", "session", json!(long)),
        ] {
            let mut body = append_body(&session, row, "v");
            body[field] = value;
            let (status, rejected) = via.a("POST", "/v1/append", Some(&body));
            assert_eq!(
                (status, &rejected["outcome"]),
                (400, &json!("not_committed")),
                "{name} {row}"
            );
            assert_eq!(rejected["error"], "invalid_input", "{name} {row}");
            let message = rejected["message"].as_str().unwrap_or_default();
            assert!(
                message.contains("255 bytes") && !message.contains(&long),
                "{name} {row}: {rejected}"
            );
        }
        firsts.push(("/v1/append", name, first_body, first));

        // Facts over the same listener.
        let conflict = |error: &str| (409, json!({"outcome": "not_committed", "error": error}));
        // A `/`, a space, a `+` and a multibyte character, decoded from the query string.
        let key = format!("{name}/dir/a b+\u{e9}");
        let get =
            |key: &str| via.a("GET", &format!("/v1/facts/get?key={}", query_value(key)), None);
        let put = |body: &Value| via.a("POST", "/v1/facts/put", Some(body));
        let mut first_body = fact_body(&key, "v1", "p1");
        first_body["meta"] = json!({"m": 1});
        first_body["agent"] = json!("x");
        let (status, first) = put(&first_body);
        assert_eq!(status, 200, "{name}: {first}");
        assert_eq!(
            (&first["outcome"], &first["deduplicated"]),
            (&json!("committed"), &json!(false))
        );
        let (status, got) = get(&key);
        let fact = &got["fact"];
        assert_eq!(
            (status, &fact["key"], &fact["content"]),
            (200, &json!(key), &json!("v1")),
            "{name}"
        );
        assert_eq!(
            (&fact["agent"], &fact["meta"], &fact["lsn"]),
            (&json!("x"), &json!({"m": 1}), &first["lsn"])
        );
        assert!(fact["commit_time_ns"].as_u64().unwrap() > 0, "{name}");

        let lsn = first["lsn"].as_u64().unwrap();
        let mut second = fact_body(&key, "v2", "p2");
        second["if_lsn"] = json!(lsn - 1);
        assert_eq!(put(&second), conflict("conflict"), "{name}: stale if_lsn");
        assert_eq!(
            put(&fact_body(&key, "changed", "p1")),
            conflict("idempotency_conflict"),
            "{name}"
        );
        let (status, retry) = put(&first_body);
        assert_eq!((status, &retry["deduplicated"]), (200, &json!(true)), "{name}: retry");
        assert_eq!((&retry["txn"], &retry["lsn"]), (&first["txn"], &first["lsn"]), "{name}");
        second["if_lsn"] = json!(lsn);
        assert_eq!(put(&second).0, 200, "{name}: current if_lsn");
        let delete = json!({"key": key, "idem_key": "d1"});
        let (status, deleted) = via.a("POST", "/v1/facts/delete", Some(&delete));
        assert_eq!((status, &deleted["outcome"]), (200, &json!("committed")), "{name}");
        assert_eq!(get(&key), (200, json!({"fact": null})), "{name}: deleted");
        let history = |max_bytes: usize| {
            let target =
                format!("/v1/facts/history?key={}&max_bytes={max_bytes}", query_value(&key));
            via.a("GET", &target, None).1
        };
        let full = history(1000);
        let revisions = full["revisions"].as_array().unwrap();
        let shape: Vec<_> = revisions.iter().map(|r| (&r["deleted"], &r["content"])).collect();
        let (yes, no) = (json!(true), json!(false));
        let expected = [(&yes, &json!("")), (&no, &json!("v2")), (&no, &json!("v1"))];
        assert_eq!(shape, expected, "{name}: newest first, the tombstone first");
        assert_eq!((&full["truncated"], &revisions[0]["lsn"]), (&no, &deleted["lsn"]), "{name}");
        let cut = history(1);
        assert_eq!((cut["revisions"].as_array().unwrap().len(), &cut["truncated"]), (1, &yes));

        for n in 1..=3 {
            assert_eq!(put(&fact_body(&format!("{name}/list/{n}"), "v", &format!("l{n}"))).0, 200);
        }
        let list = |query: &str| {
            via.a("GET", &format!("/v1/facts/list?prefix={name}/list/&{query}"), None).1
        };
        let page = list("limit=2");
        assert_eq!(keys(&page), [format!("{name}/list/1"), format!("{name}/list/2")], "{name}");
        let page = list(&format!("after={}", query_value(page["next"].as_str().unwrap())));
        assert_eq!(
            (keys(&page), &page["next"]),
            (vec![format!("{name}/list/3").as_str()], &Value::Null)
        );
        for (case, body) in [
            ("1025-byte key", fact_body(&"k".repeat(1025), "v", "long")),
            ("content as a number", json!({"key": "n", "content": 1, "idem_key": "n"})),
            (
                "source refs",
                json!({"key": "n", "content": "v", "idem_key": "r", "source_refs": [1]}),
            ),
        ] {
            let (status, rejected) = put(&body);
            assert_eq!(
                (status, &rejected["error"]),
                (400, &json!("invalid_input")),
                "{name} {case}"
            );
        }
        firsts.push(("/v1/facts/put", name, first_body, first));
    }
    // The second listener's "hello" and three 100-byte turns repeat the first's. Other
    // tests share the counter, so this is a lower bound.
    assert!(repeats() - before >= 4, "repeats counted: {before} then {}", repeats());

    // Only direct Rust API writes can store content that is not UTF-8.
    let raw = Scope { tenant: "a".into(), agent: "a".into(), session: "raw".into() };
    let turn = NewTurn { role: Role::Tool, content: vec![b'o', 0xff, b'k'], meta: vec![] };
    engine.append(&raw, vec![turn], None).unwrap();
    let (_, window) = running.vias[1].a("GET", "/v1/window?agent=a&session=raw&last=1", None);
    let turn = &window["turns"][0];
    assert_eq!(turn["content"], "o\u{fffd}k");
    assert_eq!(
        (&turn["lossy"], &turn["meta"], &turn["role"]),
        (&json!(true), &Value::Null, &json!("tool"))
    );

    let full =
        json!({"outcome": "not_committed", "error": "resource_exhausted", "resource": "facts"});
    let new = fact_body("new", "v", "new");
    assert_eq!(running.vias[0].a("POST", "/v1/facts/put", Some(&new)), (429, full), "8 keys held");

    running.server.shutdown();
    assert_eq!(Arc::strong_count(&engine), 1, "shutdown releases the engine");
    drop(engine);
    // Reopen on the same directory. The old socket file is stale and gets replaced.
    let running = serve(sim_engine(&disk, tune), &dir, MAX_CONNECTIONS);
    for (target, name, body, first) in firsts {
        let via = &running.vias[usize::from(name == "unix")];
        let (status, retry) = via.a("POST", target, Some(&body));
        assert_eq!((status, &retry["deduplicated"]), (200, &json!(true)), "{name} {target}");
        assert_eq!(receipt(&retry), receipt(&first), "{name} {target}: original receipt");
    }
    running.server.shutdown();
}

/// A query value: `+` for a space, `/` as is, `%XX` for every other reserved byte.
fn query_value(text: &str) -> String {
    let byte = |byte: u8| match byte {
        b' ' => "+".to_owned(),
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
            char::from(byte).to_string()
        }
        _ => format!("%{byte:02X}"),
    };
    text.bytes().map(byte).collect()
}

fn fact_body(key: &str, content: &str, idem_key: &str) -> Value {
    json!({"key": key, "content": content, "idem_key": idem_key})
}

fn keys(page: &Value) -> Vec<&str> {
    page["facts"].as_array().unwrap().iter().map(|fact| fact["key"].as_str().unwrap()).collect()
}

#[test]
fn auth_and_host_rules() {
    let dir = Dir::new();
    let engine = sim_engine(&SimDisk::new(2), |_| {});
    let running = serve(engine.clone(), &dir, MAX_CONNECTIONS);
    let [tcp, unix] = &running.vias;
    let (status, _) = unix.a("POST", "/v1/append", Some(&append_body("secret", "k", "a-data")));
    assert_eq!(status, 200);

    let window = "/v1/window?agent=a&session=secret&last=10";
    let bad = "token-x-0123456789abcdefghijklmnopqrstuv";
    let (lo, evil, wrong) = (Some("127.0.0.1"), Some("evil.example"), Some(bad));
    let (a, b, health, stats) = (Some(TOKEN_A), Some(TOKEN_B), "/healthz", "/v1/stats");
    // (case, via, Host, token, target, status, error or the number of A's turns read)
    let rows = [
        ("no token", tcp, lo, None, window, 401, Err("unauthorized")),
        ("bad token", tcp, lo, wrong, window, 401, Err("unauthorized")),
        ("bad token on unix", unix, None, wrong, window, 401, Err("unauthorized")),
        ("tenant B reads A's session", tcp, lo, b, window, 200, Ok(0)),
        ("tenant A", tcp, lo, a, window, 200, Ok(1)),
        ("localhost with port", tcp, Some("LocalHost:7425"), a, window, 200, Ok(1)),
        ("ipv6 loopback", tcp, Some("[::1]:1"), a, window, 200, Ok(1)),
        ("foreign host on tcp", tcp, evil, a, window, 403, Err("forbidden_host")),
        ("loopback prefix", tcp, Some("127.0.0.1.evil"), a, window, 403, Err("forbidden_host")),
        ("missing host on tcp", tcp, None, a, window, 403, Err("forbidden_host")),
        ("foreign host on unix", unix, evil, a, window, 200, Ok(1)),
        ("missing host on unix", unix, None, a, window, 200, Ok(1)),
        ("health without token", tcp, lo, None, health, 200, Ok(0)),
        ("stats on unix without token", unix, None, None, stats, 200, Ok(0)),
        ("stats on tcp", tcp, lo, a, stats, 404, Err("not_found")),
        ("stats on tcp without token", tcp, lo, None, stats, 401, Err("unauthorized")),
        ("health with foreign host", tcp, evil, None, health, 403, Err("forbidden_host")),
    ];
    for (case, via, host, token, target, status, expect) in rows {
        let (got, body) = via.call(host, token, "GET", target, None);
        assert_eq!(got, status, "{case}: {body}");
        match expect {
            Err(error) => {
                assert_eq!(body, json!({"outcome": "not_committed", "error": error}), "{case}")
            }
            Ok(count) if target == window => {
                let turns = body["turns"].as_array().unwrap();
                assert_eq!(turns.len(), count, "{case}");
                assert!(turns.iter().all(|turn| turn["content"] == "a-data"), "{case}");
            }
            Ok(_) => assert_eq!(body["instance"], running.instance.as_str(), "{case}"),
        }
    }

    // I11: tenant B writing the same names gets its own session, never A's.
    let b_data = append_body("secret", "k", "b-data");
    let (status, appended) = tcp.call(lo, b, "POST", "/v1/append", Some(&b_data));
    assert_eq!((status, &appended["seqs"]), (200, &json!([1])), "tenant B write");
    let (_, a) = tcp.a("GET", window, None);
    assert_eq!(a["turns"].as_array().unwrap().len(), 1, "A still sees only its turn");
    let rule = json!({"key": "secret/rule", "content": "a-rule", "idem_key": "f"});
    assert_eq!(tcp.a("POST", "/v1/facts/put", Some(&rule)).0, 200);
    for (target, nothing) in [
        ("/v1/facts/get?key=secret/rule", json!({"fact": null})),
        ("/v1/facts/list?prefix=", json!({"facts": [], "next": null})),
    ] {
        let got = tcp.call(lo, b, "GET", target, None);
        assert_eq!(got, (200, nothing), "tenant B sees nothing of A's facts: {target}");
    }
    assert_eq!(tcp.a("GET", "/v1/facts/get?key=secret/rule", None).1["fact"]["content"], "a-rule");

    // Auth runs before the body is read: a bad token declaring 8 MiB gets 401 and a
    // closed connection without sending a byte of body.
    let head = format!(
        "POST /v1/append HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {bad}\r\n\
         Content-Length: {}\r\n\r\n",
        8 << 20
    );
    let started = std::time::Instant::now();
    let wire = tcp.raw(head.as_bytes());
    assert!(wire.starts_with("HTTP/1.1 401 "), "{wire}");
    assert!(wire.contains("Connection: close"), "{wire}");
    assert!(started.elapsed().as_secs() < 5, "closed without waiting for the body");
    running.server.shutdown();

    // Past the connection cap a new connection gets 503 busy and is closed.
    let running = serve(engine, &dir, 2);
    let unix = &running.vias[1];
    let Via::Unix(path) = unix else { unreachable!() };
    let idle = [0, 1].map(|_| std::os::unix::net::UnixStream::connect(path).unwrap());
    // It answers on accept without reading, so send nothing: closing over unread bytes
    // makes Linux reset the connection before the client reads the answer.
    let wire = unix.raw(b"");
    let (head, body) = wire.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 503 "), "{wire}");
    let busy = json!({"outcome": "not_committed", "error": "busy"});
    assert_eq!(serde_json::from_str::<Value>(body).unwrap(), busy);
    // Stats count it. Ask over an accepted connection, since the cap is still full.
    use std::io::{Read, Write};
    let mut conn = &idle[0];
    conn.write_all(b"GET /v1/stats HTTP/1.1\r\nConnection: close\r\n\r\n").unwrap();
    let mut wire = String::new();
    conn.read_to_string(&mut wire).unwrap();
    let stats: Value = serde_json::from_str(wire.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert!(stats["busy_replies"].as_u64().unwrap() >= 1, "{wire}");
    drop(idle);
    // Shutdown needs no connection to the socket path, which may be gone or replaced.
    std::fs::remove_file(path).unwrap();
    running.server.shutdown();
}

#[test]
fn health_tracks_engine_state() {
    let disk = SimDisk::new(3);
    let dir = Dir::new();
    let engine = sim_engine(&disk, |o| (o.tape.free_space_floor, o.max_sessions) = (Some(4096), 1));
    let running = serve(engine, &dir, MAX_CONNECTIONS);
    let unix = &running.vias[1];
    let instance = running.instance.as_str();
    let health = || unix.call(None, None, "GET", "/healthz", None);
    let stats = || {
        let (status, stats) = unix.call(None, None, "GET", "/v1/stats", None);
        assert_eq!(status, 200, "stats serve in every state: {stats}");
        (stats["health"].clone(), stats["durable_lsn"].clone(), stats["facts"].clone())
    };
    let append = |key: &str, content: &str| {
        unix.a("POST", "/v1/append", Some(&append_body("s", key, content)))
    };
    let contents = || {
        let (status, window) = unix.a("GET", "/v1/window?agent=a&session=s&last=100", None);
        assert_eq!(status, 200, "windows keep serving");
        window["turns"].as_array().unwrap().iter().map(|t| t["content"].clone()).collect::<Vec<_>>()
    };
    assert_eq!(health(), (200, json!({"status": "ok", "instance": instance})));

    // Over the 4 MiB tape batch but under the 8 MiB body cap.
    let mut big = append_body("s", "big", "");
    big["turns"] =
        ["x", "y"].map(|c| json!({"role": "user", "content": c.repeat(2_200_000)})).into();
    let too_large = json!({"outcome": "not_committed", "error": "too_large"});
    assert_eq!(unix.a("POST", "/v1/append", Some(&big)), (413, too_large.clone()));
    // More than 1,024 turns is rejected before the engine encodes them.
    let mut many = append_body("s", "many", "");
    many["turns"] = vec![json!({"role": "user", "content": ""}); 1025].into();
    assert_eq!(unix.a("POST", "/v1/append", Some(&many)), (413, too_large.clone()));
    let head = format!(
        "POST /v1/append HTTP/1.1\r\nAuthorization: Bearer {TOKEN_A}\r\nContent-Length: {}\r\n\r\n",
        (8 << 20) + 1
    );
    let wire = unix.raw(head.as_bytes());
    assert!(wire.starts_with("HTTP/1.1 413 ") && wire.ends_with(&too_large.to_string()), "{wire}");
    assert_eq!(health().1["status"], "ok", "a rejected batch changes nothing");

    // I14: below the floor, writes stop without loss and reads go on.
    assert_eq!(append("first", "one").0, 200);
    let fact =
        |content: &str, key: &str| json!({"key": "rule", "content": content, "idem_key": key});
    assert_eq!(unix.a("POST", "/v1/facts/put", Some(&fact("kept", "f1"))).0, 200);
    let second = append_body("s2", "other", "v");
    let exhausted =
        json!({"outcome": "not_committed", "error": "resource_exhausted", "resource": "sessions"});
    assert_eq!(
        unix.a("POST", "/v1/append", Some(&second)),
        (429, exhausted),
        "one session allowed"
    );
    let capacity = SimDisk::DEFAULT_CAPACITY;
    let free = zerodb_tape::Io::available_bytes(&disk, "db".as_ref()).unwrap();
    disk.set_capacity(capacity - free + 4095).unwrap();
    let disk_full = json!({"outcome": "not_committed", "error": "disk_full"});
    assert_eq!(append("floor", "two"), (507, disk_full.clone()));
    assert_eq!(unix.a("POST", "/v1/facts/put", Some(&fact("new", "f2"))), (507, disk_full));
    let (status, rule) = unix.a("GET", "/v1/facts/get?key=rule", None);
    assert_eq!((status, &rule["fact"]["content"]), (200, &json!("kept")), "fact reads serve");
    assert_eq!(health(), (200, json!({"status": "read_only", "instance": instance})));
    let (_, kept) = unix.a("GET", "/v1/facts/get?key=rule", None);
    assert_eq!(stats(), (json!("read_only"), kept["fact"]["lsn"].clone(), json!(1)));
    // The same numbers for Prometheus, on the socket only.
    let wire = unix.raw(b"GET /metrics HTTP/1.1\r\nConnection: close\r\n\r\n");
    let (head, text) = wire.split_once("\r\n\r\n").unwrap();
    assert!(head.contains("Content-Type: text/plain; version=0.0.4"), "{head}");
    let value = |name: &str| {
        let line = text.lines().find(|line| line.split(' ').next() == Some(name));
        line.and_then(|line| line.split(' ').nth(1)).unwrap_or_else(|| panic!("{name}: {text}"))
    };
    assert_eq!((value("zerodb_read_only"), value("zerodb_facts")), ("1", "1"), "{text}");
    for line in ["# TYPE zerodb_writer_groups_total counter", "# TYPE zerodb_facts gauge"] {
        assert!(text.lines().any(|got| got == line), "{line}: {text}");
    }
    value("zerodb_checkpoint_snapshot_lock_seconds");
    assert!(value("zerodb_turn_content_bytes_bucket{le=\"64\"}").parse::<u64>().unwrap() >= 1);
    let count = value("zerodb_append_ack_seconds_count");
    assert_eq!(value("zerodb_append_ack_seconds_bucket{le=\"+Inf\"}"), count, "{text}");
    assert!(count.parse::<u64>().unwrap() >= 1, "the committed append is counted: {text}");
    let tcp = &running.vias[0];
    assert_eq!(tcp.call(Some("127.0.0.1"), None, "GET", "/metrics", None).0, 401, "not on TCP");
    assert_eq!(contents(), ["one"]);
    disk.set_capacity(capacity).unwrap();
    let (status, committed) = append("floor", "two");
    assert_eq!((status, &committed["deduplicated"]), (200, &json!(false)), "{committed}");
    assert_eq!(append("floor", "two").1["deduplicated"], true);
    assert_eq!(contents(), ["one", "two"], "the retried key committed once");
    assert_eq!(health().1["status"], "ok");

    // An ambiguous sync poisons the writer.
    disk.fail_next_sync().unwrap();
    let unknown = append("sync", "three");
    assert_eq!(unknown, (500, json!({"outcome": "unknown", "error": "storage"})));
    assert_eq!(health(), (503, json!({"status": "poisoned", "instance": instance})));
    assert_eq!(stats().0, "poisoned");
    assert_eq!(contents(), ["one", "two"], "reads keep the published view");
    // A failed sync publishes no dedup entry, so the retry gets not_committed from the
    // poisoned writer. Only the outcome rule keeps the caller at unknown.
    let retry = append("sync", "three");
    assert_eq!(retry, (503, json!({"outcome": "not_committed", "error": "poisoned"})));
    let mut merge = Merge::default();
    for body in [&unknown.1, &retry.1] {
        merge.record(Attempt::Answered(outcome::parse(body["outcome"].as_str().unwrap()).unwrap()));
    }
    assert_eq!(merge.finish(), Outcome::Unknown);
    running.server.shutdown();

    // The engine has no public hook to fail a receipt after commit, or a space-error
    // cleanup, so these two rows drive the response mapping directly.
    let failure = |kind, outcome| Err(EngineError { kind, outcome });
    for (case, result, status, body) in [
        (
            "receipt failure after commit",
            failure(ErrorKind::Poisoned, Outcome::Committed),
            200,
            json!({"outcome": "committed", "receipt": null, "error": "poisoned"}),
        ),
        (
            "space error with a failed cleanup",
            failure(ErrorKind::Storage(Error::DiskFull), Outcome::Unknown),
            507,
            json!({"outcome": "unknown", "error": "disk_full"}),
        ),
    ] {
        let response = zerodb::api::append_response(result);
        assert_eq!(response.status, status, "{case}");
        assert_eq!(serde_json::from_slice::<Value>(&response.body).unwrap(), body, "{case}");
    }
}

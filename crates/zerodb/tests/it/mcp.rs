use crate::support::{
    self, chmod, init, init_config, kill_holder, private_file, text, wait_for, Dir, Holders, Mcp,
    TOKEN_A,
};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use zerodb_tape::SimDisk;

fn contents(result: &Value) -> Vec<&str> {
    let turns = result["structuredContent"]["turns"].as_array().unwrap();
    turns.iter().map(|turn| turn["content"].as_str().unwrap()).collect()
}

/// Checks a committed tool result: `seq` for an append, `lsn` for a fact write. Returns it.
fn committed(result: &Value, case: &str) -> u64 {
    let content = &result["structuredContent"];
    let id = if content["seq"].is_null() { "lsn" } else { "seq" };
    let n = content[id].as_u64().unwrap_or_else(|| panic!("{case}: {result}"));
    assert_eq!(text(result), format!("committed {id}={n}"), "{case}");
    assert!(content["lsn"].is_u64() && result["isError"].is_null(), "{case}");
    let keys: Vec<&String> = content.as_object().unwrap().keys().collect();
    let shape = ["committed", "deduplicated", "lsn", "seq"];
    assert_eq!(keys, shape[..if id == "seq" { 4 } else { 3 }], "{case}: one shape");
    n
}

/// A field of `data`'s `LOCK`.
fn lock_field(data: &Path, name: &str) -> String {
    let lock = std::fs::read_to_string(data.join("LOCK")).unwrap();
    lock.split(' ').find_map(|field| field.strip_prefix(name)).unwrap().to_owned()
}

/// What a fake listener saw.
#[derive(Clone, Default)]
struct Seen {
    connections: Arc<AtomicUsize>,
    authorized: Arc<AtomicUsize>,
}

/// A fake server: every request gets `/healthz` JSON with `instance`.
fn fake<S: Read + Write>(
    accept: impl Fn() -> std::io::Result<S> + Send + 'static,
    instance: &str,
) -> Seen {
    let (seen, body) = (Seen::default(), json!({"status": "ok", "instance": instance}).to_string());
    let record = seen.clone();
    std::thread::spawn(move || {
        while let Ok(mut stream) = accept() {
            record.connections.fetch_add(1, Ordering::SeqCst);
            let (mut head, mut byte) = (Vec::new(), [0]);
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
            }
            if String::from_utf8_lossy(&head).to_ascii_lowercase().contains("authorization:") {
                record.authorized.fetch_add(1, Ordering::SeqCst);
            }
            let length = body.len();
            let _ = write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {length}\r\n\r\n{body}");
        }
    });
    seen
}

fn fake_unix(socket: &Path, instance: &str) -> Seen {
    let listener = UnixListener::bind(socket).unwrap();
    fake(move || listener.accept().map(|(stream, _)| stream), instance)
}

fn lock_text(instance: &str, socket: &Path, tcp: &str) -> String {
    format!("pid=999999999 instance={instance} unix={} tcp={tcp}\n", socket.display())
}

/// Root-owned sockets that exist on stock macOS and Linux hosts. A test cannot create a
/// socket owned by another user without root, so it points `LOCK` at one of these.
const FOREIGN_SOCKETS: [&str; 5] = [
    "/run/systemd/notify",
    "/var/run/mDNSResponder",
    "/var/run/syslog",
    "/run/dbus/system_bus_socket",
    "/run/systemd/journal/stdout",
];

const FOREIGN_ROW: &str = "a socket owned by another user";

/// A root-owned socket from [`FOREIGN_SOCKETS`], when the tests do not run as root.
fn foreign_socket() -> Option<&'static Path> {
    let euid = rustix::process::geteuid().as_raw();
    FOREIGN_SOCKETS.iter().map(Path::new).find(|path| {
        std::fs::symlink_metadata(path)
            .is_ok_and(|meta| meta.file_type().is_socket() && meta.uid() != euid)
    })
}

/// Drives the adapter as a model's client would, against a server started by the test.
#[test]
fn tool_calls_over_stdio() {
    let dir = Dir::new();
    // 200 back-to-back connections can outrun reaping under a busy suite, so no lower cap.
    let cap = zerodb::serve::MAX_CONNECTIONS;
    let running = support::serve(support::sim_engine(&SimDisk::new(4), |_| {}), &dir, cap);
    let url = format!("http://{}", running.server.tcp_addr().unwrap());
    let args = ["--url", &url, "--token-file", &private_file(&dir, "t", TOKEN_A), "--agent", "ag"];
    let mut mcp = Mcp::start(&dir.0, &[&args[..], &["--session", "s1"]].concat(), None);

    assert_eq!(mcp.request("ping", json!({}))["result"], json!({}), "ping before initialize");
    for method in ["server/discover", "tools/list"] {
        let reply = mcp.request(method, json!({}));
        assert_eq!(reply["error"]["code"], -32601, "{method} before initialize");
    }
    for (asked, answered) in [("2024-11-05", "2024-11-05"), ("2099-01-01", "2025-11-25")] {
        let reply = mcp.request("initialize", init(asked));
        assert_eq!(reply["result"]["protocolVersion"], answered, "asked for {asked}");
        assert_eq!(reply["result"]["capabilities"], json!({"tools": {}}), "asked for {asked}");
    }
    mcp.send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
    let cancelled = json!({"requestId": 1, "reason": "test"});
    mcp.send(&json!({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": cancelled}));
    // `response` checks ids, so a reply to either notification would fail here.
    assert_eq!(mcp.request("ping", json!({}))["result"], json!({}), "ping after initialize");

    // An invalid envelope runs no tool. The default-session window below shows no turn.
    for (case, envelope) in [
        ("no jsonrpc", json!({})),
        ("jsonrpc 1.0", json!({"jsonrpc": "1.0"})),
        ("object id", json!({"jsonrpc": "2.0", "id": {"a": 1}})),
        ("array id", json!({"jsonrpc": "2.0", "id": [1]})),
    ] {
        mcp.next_id += 1;
        let params = json!({"name": "memory_append", "arguments": {"content": "bad envelope"}});
        let mut message = json!({"id": mcp.next_id, "method": "tools/call", "params": params});
        message.as_object_mut().unwrap().extend(envelope.as_object().unwrap().clone());
        mcp.send(&message);
        let reply = mcp.response();
        let id = if envelope.get("id").is_some() { json!(null) } else { json!(mcp.next_id) };
        assert_eq!((&reply["error"]["code"], &reply["id"]), (&json!(-32600), &id), "{case}");
    }
    let tools = mcp.request("tools/list", json!({}))["result"]["tools"].clone();
    let names: Vec<&str> =
        tools.as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["memory_append", "memory_recent", "fact_set", "fact_get", "fact_list"]);
    let (set, get, list) =
        (&tools[2]["inputSchema"], &tools[3]["inputSchema"], &tools[4]["inputSchema"]);
    assert_eq!((&set["required"], &get["required"]), (&json!(["key"]), &json!(["key"])));
    for field in ["key", "content", "delete", "meta"] {
        assert!(set["properties"][field].is_object(), "fact_set {field}");
    }
    assert_eq!(list["required"], json!(["prefix"]));
    let limit = &list["properties"]["limit"];
    assert_eq!((&limit["default"], &limit["maximum"]), (&json!(50), &json!(200)));
    assert_eq!(list["properties"]["max_bytes"]["default"], 65536);
    let (append, recent) = (&tools[0]["inputSchema"], &tools[1]["inputSchema"]);
    assert_eq!(append["required"], json!(["content"]));
    for field in ["content", "role", "session", "meta"] {
        assert!(append["properties"][field].is_object(), "memory_append {field}");
    }
    assert_eq!(append["properties"]["role"]["default"], "assistant");
    assert_eq!(
        (&recent["properties"]["n"]["default"], &recent["properties"]["n"]["maximum"]),
        (&json!(20), &json!(200))
    );
    assert_eq!(recent["properties"]["max_bytes"]["default"], 65536);
    assert!(recent["properties"]["session"].is_object() && recent["required"].is_null());

    let one = mcp.call("memory_append", json!({"content": "one", "meta": {"k": 1}}));
    let first = committed(&one, "default session");
    assert_eq!(one["structuredContent"]["deduplicated"], false);
    let two = mcp.call("memory_append", json!({"content": "two", "role": "user", "session": "s2"}));
    committed(&two, "session argument");
    let three = mcp.call("memory_append", json!({"content": "three"}));
    assert_eq!(committed(&three, "second append"), first + 1, "seqs follow in a session");

    let window = mcp.call("memory_recent", json!({"n": 1}));
    assert_eq!(contents(&window), ["three"], "n keeps the newest");
    let window = mcp.call("memory_recent", json!({}));
    assert_eq!(contents(&window), ["one", "three"], "default session, oldest first");
    assert_eq!(text(&window), format!("#{first} assistant: one\n#{} assistant: three", first + 1));
    assert_eq!(window["structuredContent"]["turns"][0]["meta"], json!({"k": 1}));
    let window = mcp.call("memory_recent", json!({"session": "s2", "max_bytes": 3}));
    assert_eq!(contents(&window), ["two"], "session argument");
    let window = mcp.call("memory_recent", json!({"session": "s2", "max_bytes": 2}));
    assert_eq!(
        (contents(&window).len(), &window["structuredContent"]["truncated"]),
        (0, &json!(true))
    );
    // The adapter writes as `--agent` inside the token's tenant, whatever the session.
    let (_, server) = running.vias[1].a("GET", "/v1/window?agent=ag&session=s2&last=10", None);
    assert_eq!(server["turns"][0]["content"], "two");

    let first = mcp.call("fact_set", json!({"key": "repo/a", "content": "one"}));
    let lsn = committed(&first, "first set");
    assert_eq!(first["structuredContent"]["deduplicated"], false);
    let second = mcp.call("fact_set", json!({"key": "repo/a", "content": "two", "meta": {"k": 2}}));
    assert!(committed(&second, "second set") > lsn);
    let got = mcp.call("fact_get", json!({"key": "repo/a"}));
    let fact = &got["structuredContent"]["fact"];
    assert_eq!(
        (text(&got), &fact["content"], &fact["meta"]),
        ("two", &json!("two"), &json!({"k": 2}))
    );
    assert_eq!(fact["agent"], "ag", "fact_set writes as --agent");
    committed(&mcp.call("fact_set", json!({"key": "repo/a", "delete": true})), "delete");
    let got = mcp.call("fact_get", json!({"key": "repo/a"}));
    assert_eq!(
        (text(&got), &got["structuredContent"]),
        ("no current value", &json!({"fact": null}))
    );
    for key in ["repo/b", "repo/c"] {
        committed(&mcp.call("fact_set", json!({"key": key, "content": key})), key);
    }
    let page = mcp.call("fact_list", json!({"prefix": "repo/", "limit": 1}));
    assert_eq!(text(&page), "repo/b: repo/b\nmore: call again with after=repo/b");
    let mut fields: Vec<&str> =
        page["structuredContent"].as_object().unwrap().keys().map(String::as_str).collect();
    fields.sort();
    assert_eq!(
        (fields, &page["structuredContent"]["next"]),
        (vec!["facts", "next"], &json!("repo/b"))
    );
    let page = mcp.call("fact_list", json!({"prefix": "repo/", "after": "repo/b"}));
    assert_eq!((text(&page), &page["structuredContent"]["next"]), ("repo/c: repo/c", &Value::Null));
    committed(&mcp.call("fact_set", json!({"key": "e", "content": ""})), "empty");
    assert_eq!(text(&mcp.call("fact_get", json!({"key": "e"}))), "(empty)");
    // Keys count toward `max_bytes`, so a page of escaped NULs fits the response cap.
    let nul = |n| "\0".repeat(n);
    for i in 0..200 {
        let body = json!({"key": format!("n/{}{i:04}", nul(1018)), "content": nul(41_900),
                          "idem_key": format!("n{i}")});
        assert_eq!(running.vias[1].a("POST", "/v1/facts/put", Some(&body)).0, 200);
    }
    let query = json!({"prefix": "n/", "limit": 200, "max_bytes": 8 << 20});
    let page = &mcp.call("fact_list", query)["structuredContent"];
    let (count, next) = (page["facts"].as_array().map(Vec::len), format!("n/{}0194", nul(1018)));
    assert!(count == Some(195) && page["next"] == json!(next), "{count:?}");
    let one = "pass exactly one of content or delete: true";
    for (case, args, expect) in [
        ("content and delete", json!({"key": "k", "content": "x", "delete": true}), one),
        ("neither", json!({"key": "k"}), one),
        (
            "delete with meta",
            json!({"key": "k", "delete": true, "meta": {}}),
            "meta must be an object, and only with content",
        ),
    ] {
        let rejected = mcp.call("fact_set", args);
        assert_eq!((rejected["isError"].clone(), text(&rejected)), (json!(true), expect), "{case}");
    }

    let rejected = mcp.call("memory_append", json!({"content": "x", "role": "robot"}));
    assert_eq!(rejected["isError"], true);
    let expect = "not_committed: nothing was written (invalid_input: role must be";
    assert!(text(&rejected).starts_with(expect), "{rejected}");
    let missing = mcp.call("memory_append", json!({"role": "user"}));
    assert_eq!((missing["isError"].clone(), text(&missing)), (json!(true), "content is required"));
    let reply = mcp.request("tools/call", json!({"name": "memory_delete", "arguments": {}}));
    assert_eq!(reply["error"]["code"], -32602, "unknown tool");

    // A line over 8 MiB is refused, and the next line is read as usual.
    mcp.next_id += 1;
    let long =
        json!({"jsonrpc": "2.0", "id": mcp.next_id, "method": "ping", "pad": "x".repeat(8 << 20)});
    mcp.send(&long);
    let refused = json!({"code": -32600, "message": "message longer than 8 MiB"});
    assert_eq!(mcp.response(), json!({"jsonrpc": "2.0", "id": null, "error": refused}));
    assert_eq!(mcp.request("ping", json!({}))["result"], json!({}), "ping after a long line");

    let (status, _) = mcp.close();
    assert_eq!(status.code(), Some(0), "stdin closed: {}", mcp.process.stderr());
    let stdout = mcp.process.stdout();
    for line in stdout.lines() {
        let message: Value = serde_json::from_str(line).unwrap();
        assert_eq!(message["jsonrpc"], "2.0", "stdout holds only JSON-RPC: {line}");
    }
    let hex_run = stdout.as_bytes().windows(32).any(|w| w.iter().all(u8::is_ascii_hexdigit));
    let hidden = !hex_run && !stdout.contains("idem") && !stdout.contains("if_lsn");
    assert!(hidden, "no retry key or condition reaches the model: {stdout}");
    assert!(!stdout.contains(TOKEN_A) && !stdout.contains("tenant"), "{stdout}");

    // The same tools from a shell: one JSON line on stdout, the reason on stderr.
    let cli = |verb: &[&str], code: i32| {
        let mut process = support::Process::spawn(&dir.0, &[verb, &args[..]].concat());
        let (status, out) = process.wait();
        assert_eq!(status.code(), Some(code), "{verb:?}: {out}");
        (process.stdout(), process.stderr())
    };
    // (command, a JSON pointer into what it prints, the value there)
    for (verb, pointer, expected) in [
        (&["append", "--session", "sh", "--content", "hi", "--role", "user"][..], "/seq", json!(1)),
        (&["recent", "--session", "sh", "--n", "1"], "/turns/0/content", json!("hi")),
        (&["fact", "set", "--key", "sh/k", "--content", "v"], "/committed", json!(true)),
        (&["fact", "get", "--key", "sh/k"], "/fact/content", json!("v")),
        (&["fact", "list", "--prefix", "sh/", "--limit", "5"], "/facts/0/key", json!("sh/k")),
        (&["fact", "set", "--key", "sh/k", "--delete"], "/committed", json!(true)),
        (&["fact", "get", "--key", "sh/k"], "", json!({"fact": null})),
    ] {
        let (out, _) = cli(verb, 0);
        let printed: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(printed.pointer(pointer), Some(&expected), "{verb:?}: {out}");
    }
    // (command, exit code, what stderr says)
    for (verb, code, reason) in [
        (&["fact", "set", "--key", "k", "--content", "x", "--delete"][..], 1, "pass exactly one"),
        (&["append", "--content", "x", "--role", "robot"], 1, "not_committed: nothing was written"),
        (&["recent", "--n", "x"], 2, ""),
        (&["fact", "drop", "--key", "k"], 2, ""),
        (&["recent", "--key", "k"], 2, ""),
    ] {
        let (out, err) = cli(verb, code);
        assert!(out.is_empty() && err.contains(reason), "{verb:?}: {out} {err}");
    }
    // Every attempt is sent and then cut off, so the outcome stays unknown.
    let lossy = TcpListener::bind("127.0.0.1:0").unwrap();
    let lossy_url = format!("http://{}", lossy.local_addr().unwrap());
    std::thread::spawn(move || {
        for mut conn in lossy.incoming().map_while(Result::ok) {
            let _ = conn.read(&mut [0; 4096]);
        }
    });
    let unknown = ["append", "--url", &lossy_url, "--content", "x", "--retry-deadline", "2500ms"];
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_zerodb"));
    command.args(unknown).current_dir(&dir.0).env("ZERODB_TOKEN", TOKEN_A);
    let (status, out) = support::Process::start(command.stdin(std::process::Stdio::null())).wait();
    assert_eq!(status.code(), Some(3), "unknown outcome: {out}");
    assert!(out.contains("zerodb: unknown: the write may be saved"), "{out}");

    // No attempt sent a byte, so nothing was written. A listener whose backlog is full
    // never accepts: connecting must still stop at the deadline.
    let _full = support::saturated(&dir.0.join("full"));
    // FIFOs where a file belongs must not block an open: the call fails by its deadline.
    init_config(&dir);
    for name in ["fifo-log", "fifo-lock"] {
        std::fs::DirBuilder::new().mode(0o700).create(dir.0.join(name)).unwrap();
    }
    support::mkfifo(&dir.0.join("fifo-log/serve.log"));
    support::mkfifo(&dir.0.join("fifo-lock/LOCK"));
    let (gone, full) = (dir.0.join("gone"), format!("unix://{}", dir.0.join("full").display()));
    let gone = format!("unix://{}", gone.display());
    let rows: [(&str, &[&str], &str, &str); 6] = [
        ("no socket", &["--url", &gone], "2500ms", ""),
        ("no LOCK", &["--data-dir", "empty"], "2500ms", ""),
        ("full backlog", &["--url", &full], "2500ms", ""),
        ("server up, deadline under 2 s", &["--url", &url], "1s", "less than 2 s left"),
        (
            "FIFO at serve.log on auto-start",
            &["--config", "c.json", "--data-dir", "fifo-log"],
            "2500ms",
            "",
        ),
        ("FIFO at LOCK during discovery", &["--data-dir", "fifo-lock"], "2500ms", ""),
    ];
    for (case, args, deadline, detail) in rows {
        let args = [args, &["--retry-deadline", deadline]].concat();
        let mut mcp = Mcp::start(&dir.0, &args, Some(TOKEN_A));
        mcp.handshake();
        let result = mcp.call("memory_append", json!({"content": "lost?"}));
        assert_eq!(result["isError"], true, "{case}");
        let expect = format!("not_committed: nothing was written ({detail}");
        assert!(text(&result).starts_with(&expect), "{case}: {result}");
        assert_eq!(mcp.close().0.code(), Some(0));
    }
    let (_, lost) = running.vias[1].a("GET", "/v1/window?agent=mcp&session=default&last=9", None);
    assert_eq!(lost["turns"], json!([]), "nothing sent under 2 s");
}

/// Adapters find or start one server, and send the token only to a server they trust.
#[test]
fn launch_race_starts_one_server() {
    let dir = Dir::new();
    let token = init_config(&dir);
    let data = dir.0.join("d");
    let _holders = Holders(vec![data.clone(), dir.0.join("B")]);
    let args = ["--config", "c.json", "--tenant", "a", "--data-dir", "d"];

    // Two adapters race to start a server on an empty data directory.
    let mut racers = [0, 1].map(|_| Mcp::start(&dir.0, &args, None));
    for mcp in &mut racers {
        mcp.handshake();
    }
    for mcp in &mut racers {
        mcp.post_call("memory_append", json!({"content": "race"}));
    }
    for mcp in &mut racers {
        committed(&mcp.response()["result"], "racer");
    }
    // JSON escapes each NUL to 6 bytes, so a 2 MiB window is a 12 MiB response.
    let nul = "\0".repeat(1 << 20);
    for _ in 0..2 {
        committed(&racers[0].call("memory_append", json!({"content": nul})), "NUL");
    }
    let window = racers[0].call("memory_recent", json!({"n": 2, "max_bytes": 2 << 20}));
    assert_eq!(contents(&window), [nul.as_str(); 2], "{}", text(&window).len());
    // The loser says the directory is locked and exits 75. It may still be starting when both
    // adapters already trust the winner, so wait for its line.
    let log = || std::fs::read_to_string(data.join("serve.log")).unwrap();
    wait_for("the losing server exits", || log().contains(" is locked by "));
    assert_eq!(log().matches("zerodb: serving").count(), 1, "one server: {}", log());
    assert!(!log().contains("in use"), "never EADDRINUSE: {}", log());
    assert!(!log().contains(" POST /v1/append "), "requests are not logged: {}", log());
    let stderr: String = racers.iter().map(|mcp| mcp.process.stderr()).collect();
    let exits = stderr.lines().filter(|line| line.contains("exited:"));
    assert!(exits.into_iter().all(|line| line.ends_with("exit status: 75")), "{stderr}");
    for mcp in &mut racers {
        assert_eq!(mcp.close().0.code(), Some(0));
    }

    // Each row stops the server, then points `LOCK` at something a fresh adapter must not
    // trust. The adapter refuses it without sending the token, starts its own server, and
    // commits. `true` marks a fake listener the adapter probes, `false` one it never touches.
    type Setup = fn(&Path, &Path, &str) -> (Option<String>, Vec<(Seen, bool)>);
    let rows: &[(&str, Setup, &str)] = &[
        (
            "a listener with another instance id, and one on the TCP address with the right id",
            |root, _, instance| {
                let socket = root.join("f.sock");
                let other = fake_unix(&socket, &"f".repeat(32));
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let tcp = listener.local_addr().unwrap().to_string();
                let stale = fake(move || listener.accept().map(|(stream, _)| stream), instance);
                (Some(lock_text(instance, &socket, &tcp)), vec![(other, true), (stale, false)])
            },
            "is not instance",
        ),
        (
            "a group-writable socket directory",
            |root, _, instance| {
                let parent = root.join("g");
                std::fs::create_dir(&parent).unwrap();
                chmod(&parent, 0o770);
                let seen = fake_unix(&parent.join("s"), instance);
                (Some(lock_text(instance, &parent.join("s"), "none")), vec![(seen, false)])
            },
            "not writable by group or others",
        ),
        (
            "a group-writable grandparent",
            |root, _, instance| {
                let parent = root.join("gw").join("p");
                std::fs::create_dir_all(&parent).unwrap();
                chmod(&root.join("gw"), 0o770);
                let seen = fake_unix(&parent.join("s"), instance);
                (Some(lock_text(instance, &parent.join("s"), "none")), vec![(seen, false)])
            },
            "not writable by group or others unless sticky",
        ),
        (
            FOREIGN_ROW,
            |_, _, instance| (Some(lock_text(instance, foreign_socket().unwrap(), "none")), vec![]),
            "must be a socket owned by uid",
        ),
        (
            "a killed server's stale socket and leftover staging directory",
            |_, data, instance| {
                // A start killed while installing leaves `.zs.<first 8 hex of its instance>`.
                let staging = data.join(format!(".zs.{}", &instance[..8]));
                std::fs::create_dir(&staging).unwrap();
                std::fs::write(staging.join("s"), "").unwrap();
                (None, vec![])
            },
            "Connection refused",
        ),
    ];
    for (case, setup, refusal) in rows {
        if *case == FOREIGN_ROW && foreign_socket().is_none() {
            let why = format!("none of {FOREIGN_SOCKETS:?} is a socket owned by another user");
            // GitHub runners have these sockets and run as non-root, so CI always covers it.
            assert!(std::env::var_os("CI").is_none(), "{case}: {why}");
            eprintln!("SKIPPED ROW {case:?}: {why}. Run as non-root on a stock host.");
            continue;
        }
        let old = lock_field(&data, "instance=");
        assert!(kill_holder(&data), "{case}: stop the server");
        let (lock, fakes) = setup(&dir.0, &data, &old);
        if let Some(lock) = lock {
            std::fs::write(data.join("LOCK"), lock).unwrap();
        }
        let mut mcp = Mcp::start(&dir.0, &args, None);
        mcp.handshake();
        committed(&mcp.call("memory_append", json!({"content": case})), case);
        let stderr = mcp.process.stderr();
        assert!(stderr.contains(refusal), "{case}: {stderr}");
        assert_ne!(lock_field(&data, "instance="), old, "{case}: the adapter started a server");
        for (seen, probed) in fakes {
            let connections = seen.connections.load(Ordering::SeqCst);
            assert_eq!(connections > 0, probed, "{case}: {connections} connections");
            assert_eq!(seen.authorized.load(Ordering::SeqCst), 0, "{case}: token sent");
        }
        assert_eq!(mcp.close().0.code(), Some(0), "{case}");
    }

    // A poisoned `--exit-on-poison` holder that is slow to exit: it stopped accepting, but a
    // response it is still writing keeps it alive. Spawned servers lose the lock race until it
    // exits, and the adapter keeps spawning until one wins.
    #[cfg(feature = "fault-inject")]
    {
        use crate::support::Via;
        use std::os::unix::net::UnixStream;
        assert!(kill_holder(&data));
        let serve = ["serve", "--config", "c.json", "--data-dir", "d", "--listen", "none"];
        let mut holder =
            support::Process::spawn(&dir.0, &[&serve[..], &["--exit-on-poison"]].concat());
        holder.ready();
        let socket = data.join(zerodb::listen::SOCKET_NAME);
        let via = Via::Unix(socket.clone());
        let mut big = support::append_body("big", "big", &"x".repeat(2 << 20));
        big["agent"] = json!("x");
        assert_eq!(via.call(None, Some(&token), "POST", "/v1/append", Some(&big)).0, 200);
        let mut slow = UnixStream::connect(&socket).unwrap();
        let target = "/v1/window?agent=x&session=big&last=1&max_bytes=8388608";
        write!(slow, "GET {target} HTTP/1.1\r\nAuthorization: Bearer {token}\r\n\r\n").unwrap();
        slow.read_exact(&mut [0]).unwrap();
        std::fs::write(data.join(zerodb::fault::TRIGGER), "").unwrap();
        let poison = support::append_body("s", "poison", "p");
        let unknown = json!({"outcome": "unknown", "error": "storage"});
        assert_eq!(
            via.call(None, Some(&token), "POST", "/v1/append", Some(&poison)),
            (500, unknown)
        );
        let mut mcp = Mcp::start(&dir.0, &args, None);
        mcp.handshake();
        mcp.post_call("memory_append", json!({"content": "after poison"}));
        let lost = || mcp.process.stderr().matches("exited: exit status: 75").count();
        wait_for("spawned servers lose to the slow holder twice", || lost() >= 2);
        assert!(holder.child.try_wait().unwrap().is_none(), "the holder is still exiting");
        drop(slow);
        let (status, output) = holder.wait();
        assert_eq!(status.code(), Some(70), "{output}");
        committed(&mcp.response()["result"], "after the slow holder");
        assert_eq!(mcp.close().0.code(), Some(0));
    }

    // A config naming directory A, overridden by `--data-dir B`.
    let config = json!({"data_dir": dir.0.join("A"), "tenants": [{"name": "a", "token": token}]});
    private_file(&dir, "c2.json", &config.to_string());
    let mut mcp =
        Mcp::start(&dir.0, &["--config", "c2.json", "--tenant", "a", "--data-dir", "B"], None);
    mcp.handshake();
    committed(&mcp.call("memory_append", json!({"content": "b"})), "--data-dir B");
    let socket = dir.0.join("B").join(zerodb::listen::SOCKET_NAME);
    assert_eq!(lock_field(&dir.0.join("B"), "unix="), format!("{}", socket.display()));
    assert!(!dir.0.join("A").exists(), "the config's directory is untouched");
    assert_eq!(mcp.close().0.code(), Some(0));
}

/// A write whose first answer is `unknown` is never reported as `not_committed`.
#[cfg(feature = "fault-inject")]
#[test]
fn unknown_outcome_never_downgrades() {
    // An auto-started server answers unknown and exits 70. The adapter starts another and
    // retries the same key. Only that key explains one copy and the dedup flag.
    let dir = Dir::new();
    init_config(&dir);
    let data = dir.0.join("d");
    let _holders = Holders(vec![data.clone()]);
    let mut mcp =
        Mcp::start(&dir.0, &["--config", "c.json", "--tenant", "a", "--data-dir", "d"], None);
    mcp.handshake();
    committed(&mcp.call("memory_append", json!({"content": "warm"})), "warm");
    std::fs::write(data.join(zerodb::fault::TRIGGER), "").unwrap();
    let result = mcp.call("memory_append", json!({"content": "x"}));
    committed(&result, "after the poison exit");
    assert_eq!(result["structuredContent"]["deduplicated"], true, "{result}");
    let stderr = mcp.process.stderr();
    assert!(stderr.contains("exited: exit status: 70"), "the server exited on poison: {stderr}");
    assert_eq!(contents(&mcp.call("memory_recent", json!({}))), ["warm", "x"], "one copy");
    std::fs::write(data.join(zerodb::fault::TRIGGER), "").unwrap();
    let result = mcp.call("fact_set", json!({"key": "rule", "content": "x"}));
    committed(&result, "fact_set after the poison exit");
    assert_eq!(result["structuredContent"]["deduplicated"], true, "{result}");
    let exits = mcp.process.stderr().matches("exited: exit status: 70").count();
    assert_eq!(exits, 2, "the server exited on poison again");
    assert_eq!(text(&mcp.call("fact_get", json!({"key": "rule"}))), "x");
    assert_eq!(mcp.close().0.code(), Some(0));

    // A server without `--exit-on-poison` and an adapter with `--url`: the first answer is
    // unknown, retries get not_committed from the poisoned server.
    let dir = Dir::new();
    let token = init_config(&dir);
    let data = dir.0.join("d");
    // The test reads request lines to see the unknown answer and the rejected retries.
    let serve =
        ["serve", "--config", "c.json", "--data-dir", "d", "--listen", "none", "--log-requests"];
    let url = format!("unix://{}", data.join(zerodb::listen::SOCKET_NAME).display());
    for (case, restart) in [("restarted after 1 s", true), ("no restart, 3 s deadline", false)] {
        let mut server = support::Process::spawn(&dir.0, &serve);
        server.ready();
        let deadline = if restart { "30s" } else { "3s" };
        let args = ["--url", &url, "--retry-deadline", deadline];
        let mut mcp = Mcp::start(&dir.0, &args, Some(&token));
        mcp.handshake();
        std::fs::write(data.join(zerodb::fault::TRIGGER), "").unwrap();
        mcp.post_call("memory_append", json!({"content": case}));
        let (unknown, rejected) = ("POST /v1/append 500", "POST /v1/append 503");
        wait_for("a retry gets not_committed", || server.stderr().contains(rejected));
        assert!(server.stderr().contains(unknown), "{case}: first answer unknown");
        let result = if restart {
            std::thread::sleep(std::time::Duration::from_secs(1));
            server.child.kill().unwrap();
            server.wait();
            server = support::Process::spawn(&dir.0, &serve);
            server.ready();
            let result = mcp.response()["result"].clone();
            committed(&result, case);
            assert_eq!(result["structuredContent"]["deduplicated"], true, "{case}: {result}");
            let window = mcp.call("memory_recent", json!({}));
            assert_eq!(contents(&window).iter().filter(|c| **c == case).count(), 1, "{case}");
            result
        } else {
            mcp.response()["result"].clone()
        };
        if !restart {
            assert_eq!(result["isError"], true, "{case}: {result}");
            let expect = "unknown: the write may be saved, and repeating the call may duplicate it";
            assert!(text(&result).starts_with(expect), "{case}: {result}");
            assert!(!text(&result).contains("nothing was written"), "{case}");
        }
        assert_eq!(mcp.close().0.code(), Some(0), "{case}");
    }
}

//! The S3 done criteria: two MCP clients writing to one server lose nothing, and a killed
//! server restarts with every acknowledged write. Kills are tied to sends, not timers.

use crate::support::{
    append_body, init_config, kill_lock_pid, locked, text, Dir, Holders, Mcp, Process, Via,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use zerodb::listen::SOCKET_NAME;
use zerodb::outcome::{parse, Outcome};

/// One write call: its unique content, merged outcome and seq, if the receipt had one.
type Call = (String, Outcome, Option<u64>);

/// A random number below the bound. The seed is printed, so a failure shows it, and
/// `ZERODB_SEED` replays the same kill points.
fn rng() -> impl FnMut(u64) -> u64 {
    let now = || SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
    let seed = std::env::var("ZERODB_SEED").map_or_else(|_| now() as u64, |s| s.parse().unwrap());
    eprintln!("seed {seed}");
    let mut state = seed;
    move |bound| {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (state >> 33) % bound
    }
}

/// Every turn of a session, read back over HTTP.
fn read_back(via: &Via, token: &str, agent: &str, session: &str) -> Vec<(u64, String)> {
    let mut turns: Vec<(u64, String)> = Vec::new();
    loop {
        let since = turns.last().map_or(1, |turn| turn.0 + 1);
        let target = format!(
            "/v1/window?agent={agent}&session={session}&since_seq={since}&limit=1000&max_bytes=8388608"
        );
        let (status, body) = via.call(None, Some(token), "GET", &target, None);
        assert_eq!((status, &body["truncated"]), (200, &json!(false)), "{body}");
        let page = body["turns"].as_array().unwrap();
        if page.is_empty() {
            return turns;
        }
        let turn = |t: &Value| (t["seq"].as_u64().unwrap(), t["content"].as_str().unwrap().into());
        turns.extend(page.iter().map(turn));
    }
}

/// The oracle. A `committed` call appears exactly once, at its receipt's seq, an `unknown`
/// call at most once, a `not_committed` call never. Seqs are dense and every turn comes
/// from a call.
fn check(case: &str, calls: &[Call], turns: &[(u64, String)]) {
    let seqs: Vec<u64> = turns.iter().map(|turn| turn.0).collect();
    assert_eq!(seqs, (1..=turns.len() as u64).collect::<Vec<_>>(), "{case}: dense seqs");
    let mut found: HashMap<&str, Vec<u64>> = HashMap::new();
    for (seq, content) in turns {
        found.entry(content).or_default().push(*seq);
    }
    for (content, outcome, seq) in calls {
        let at = found.remove(content.as_str()).unwrap_or_default();
        let ok = match outcome {
            Outcome::Committed => at.len() == 1 && seq.is_none_or(|seq| at == [seq]),
            Outcome::Unknown => at.len() <= 1,
            Outcome::NotCommitted => at.is_empty(),
        };
        assert!(ok, "{case}: {content} returned {outcome:?} with seq {seq:?}, found at {at:?}");
    }
    assert!(found.is_empty(), "{case}: turns no call wrote: {found:?}");
    let count = |outcome| calls.iter().filter(|call| call.1 == outcome).count();
    let counts = [Outcome::Committed, Outcome::Unknown, Outcome::NotCommitted].map(count);
    eprintln!("{case}: {} turns, committed/unknown/not_committed {counts:?}", turns.len());
}

/// Stands in for Firecracker's vsock forwarding: each connection on `listen` gets a new
/// connection to `upstream`, and bytes go both ways. Request heads are read only to count
/// requests and to set the `Host` a guest's client sends. After forwarding the whole body of
/// request number `kill_at[i]`, it SIGKILLs the server named in `data`'s `LOCK`.
struct Proxy {
    forwarded: AtomicU64,
    /// Request number and pid of each kill.
    kills: Mutex<Vec<(u64, i32)>>,
}

fn proxy(listen: &Path, upstream: PathBuf, data: PathBuf, kill_at: [u64; 2]) -> Arc<Proxy> {
    let listener = UnixListener::bind(listen).unwrap();
    let proxy = Arc::new(Proxy { forwarded: AtomicU64::new(0), kills: Mutex::default() });
    let shared = proxy.clone();
    std::thread::spawn(move || {
        for client in listener.incoming().map_while(Result::ok) {
            let (proxy, upstream, data) = (shared.clone(), upstream.clone(), data.clone());
            std::thread::spawn(move || {
                // No server: closing the connection gives the client a transport error.
                let Ok(mut server) = UnixStream::connect(&upstream) else { return };
                let (mut from, mut to) = (server.try_clone().unwrap(), client.try_clone().unwrap());
                std::thread::spawn(move || {
                    let _ = io::copy(&mut from, &mut to);
                    let _ = to.shutdown(Shutdown::Both);
                });
                let _ = proxy.forward(BufReader::new(client), &mut server, &data, kill_at);
            });
        }
    });
    proxy
}

impl Proxy {
    fn forward(
        &self,
        mut client: BufReader<UnixStream>,
        server: &mut UnixStream,
        data: &Path,
        kill_at: [u64; 2],
    ) -> io::Result<()> {
        loop {
            let (mut head, mut length, mut line) = (String::new(), 0, String::new());
            while line != "\r\n" {
                line.clear();
                if client.read_line(&mut line)? == 0 {
                    return Ok(());
                }
                let lower = line.to_ascii_lowercase();
                if lower.starts_with("host:") {
                    line = "Host: 172.16.0.2:5252\r\n".into();
                }
                if let Some(value) = lower.strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
                head.push_str(&line);
            }
            server.write_all(head.as_bytes())?;
            if io::copy(&mut (&mut client).take(length), server)? < length {
                return Ok(());
            }
            let number = self.forwarded.fetch_add(1, Ordering::SeqCst) + 1;
            if kill_at.contains(&number) {
                if let Some(pid) = kill_lock_pid(data) {
                    self.kills.lock().unwrap().push((number, pid));
                }
            }
        }
    }
}

/// Classifies a `memory_append` tool result.
fn outcome(result: &Value) -> Outcome {
    let word = text(result).split([':', ' ', ',']).next().unwrap();
    let outcome = parse(word).unwrap_or_else(|| panic!("{result}"));
    assert_eq!(outcome == Outcome::Committed, result["isError"].is_null(), "{result}");
    outcome
}

#[test]
#[ignore = "spawns processes and kills the server"]
fn two_mcp_clients_and_a_killed_server_lose_nothing() {
    let mut rng = rng();
    let dir = Dir::new();
    let token = init_config(&dir);
    let data = dir.0.join("d");
    let _holders = Holders(vec![data.clone()]);
    let first = 10 + rng(100);
    let kill_at = [first, first + 10 + rng(100)];
    let vsock = dir.0.join("v.sock_5252");
    let proxy = proxy(&vsock, data.join(SOCKET_NAME), data.clone(), kill_at);

    // H can start the server. G never sees `d` and reaches the server only through the
    // proxy. One adapter serves one call at a time, so each client is two adapters.
    let url = format!("unix://{}", vsock.display());
    let host = ["--data-dir", "d", "--config", "c.json", "--tenant", "a", "--session", "h"];
    let guest = ["--url", &url, "--session", "g"];
    let clients = [("h", &host[..], None), ("h", &host, None), ("g", &guest, Some(&*token))];
    let mut adapters = [clients[0], clients[1], clients[2], clients[2]]
        .map(|(session, args, token)| (session, Mcp::start(&dir.0, args, token)));
    for (_, mcp) in &mut adapters {
        mcp.handshake();
    }
    assert!(!locked(&data), "no server before the first call");
    // Only H restarts a killed server, so H keeps calling until G is done.
    let guests = AtomicUsize::new(2);
    let calls: Vec<(&str, Vec<Call>)> = std::thread::scope(|scope| {
        let threads = adapters.iter_mut().enumerate().map(|(i, (session, mcp))| {
            let guests = &guests;
            scope.spawn(move || {
                let host = *session == "h";
                let mut call = |n| {
                    let content = format!("{session}{i}-{n}");
                    let result = mcp.call("memory_append", json!({"content": content}));
                    (content, outcome(&result), result["structuredContent"]["seq"].as_u64())
                };
                let _done = (!host).then(|| Done(guests));
                let more = |&n: &u32| n < 250 || host && guests.load(Ordering::SeqCst) > 0;
                (*session, (0..).take_while(more).map(&mut call).collect())
            })
        });
        threads.collect::<Vec<_>>().into_iter().map(|thread| thread.join().unwrap()).collect()
    });
    for (_, mcp) in &mut adapters {
        assert_eq!(mcp.close().0.code(), Some(0), "{}", mcp.process.stderr());
    }

    let kills = proxy.kills.lock().unwrap().clone();
    assert_eq!(kills.iter().map(|kill| kill.0).collect::<Vec<_>>(), kill_at, "{kills:?}");
    assert_ne!(kills[0].1, kills[1].1, "each kill hit a new server");
    let log = std::fs::read_to_string(data.join("serve.log")).unwrap();
    assert_eq!(log.matches("zerodb: serving").count(), 3, "two restarts: {log}");
    let via = Via::Unix(data.join(SOCKET_NAME));
    for session in ["h", "g"] {
        let calls: Vec<Call> =
            calls.iter().filter(|(s, _)| *s == session).flat_map(|(_, c)| c.clone()).collect();
        assert!(calls.len() >= 500, "{session}: {}", calls.len());
        check(session, &calls, &read_back(&via, &token, "mcp", session));
    }
}

/// Counts a guest adapter out, even when it panics, so hosts stop calling.
struct Done<'a>(&'a AtomicUsize);

impl Drop for Done<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One append attempt, as a plain HTTP client sees it.
fn append(socket: &Path, token: &str, content: &str, gate: Option<Gate>) -> Call {
    let body = append_body("s", content, content);
    let (outcome, reply) = post(socket, token, "/v1/append", &body, gate);
    (content.into(), outcome, reply["seqs"][0].as_u64())
}

/// Says the request is written, then waits until the test has killed the server.
type Gate = (Sender<()>, Receiver<()>);

/// One write attempt: its outcome, and the reply when one was read. A `gate` opens
/// between writing the request and reading the reply.
fn post(
    socket: &Path,
    token: &str,
    target: &str,
    body: &Value,
    gate: Option<Gate>,
) -> (Outcome, Value) {
    let Ok(mut stream) = UnixStream::connect(socket) else {
        return (Outcome::NotCommitted, Value::Null);
    };
    let body = body.to_string();
    let length = body.len();
    let request = format!(
        "POST {target} HTTP/1.1\r\nAuthorization: Bearer {token}\r\nContent-Length: {length}\r\n\
         Connection: close\r\n\r\n{body}"
    );
    let sent = stream.write_all(request.as_bytes());
    if let Some((written, killed)) = gate {
        written.send(()).unwrap();
        killed.recv().unwrap();
    }
    // macOS fails `setsockopt` with EINVAL once the peer has closed. The read then
    // returns at once, so the error is ignored.
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let mut reply = Vec::new();
    if sent.and_then(|_| stream.read_to_end(&mut reply)).is_err() || reply.is_empty() {
        // Cut off before any reply, so the request may have been applied.
        return (Outcome::Unknown, Value::Null);
    }
    // A reply that is there but malformed is a server bug, not an unknown outcome.
    let text = String::from_utf8_lossy(&reply);
    let at = reply.windows(4).position(|w| w == b"\r\n\r\n");
    let at = at.unwrap_or_else(|| panic!("no end of head: {text}")) + 4;
    let head = String::from_utf8_lossy(&reply[..at]);
    let length = head.lines().find_map(|line| line.strip_prefix("Content-Length: "));
    let length: usize = length.and_then(|n| n.parse().ok()).unwrap_or_else(|| panic!("{text}"));
    if reply.len() - at < length {
        // Killed between the head and the body.
        return (Outcome::Unknown, Value::Null);
    }
    let reply: Value =
        serde_json::from_slice(&reply[at..]).unwrap_or_else(|e| panic!("{e}: {text}"));
    let outcome = reply["outcome"].as_str().and_then(parse);
    (outcome.unwrap_or_else(|| panic!("no outcome: {text}")), reply)
}

#[test]
#[ignore = "spawns processes and kills the server"]
fn killed_server_restarts_with_every_acknowledged_write() {
    let mut rng = rng();
    let dir = Dir::new();
    let token = init_config(&dir);
    let serve = ["serve", "--config", "c.json", "--data-dir", "d", "--listen", "none"];
    let mut server = Process::spawn(&dir.0, &serve);
    server.ready();
    let socket = dir.0.join("d").join(SOCKET_NAME);
    let gated = rng(200);
    let ((written, wrote), (killed, kill)) = (channel(), channel());
    let mut gate = Some((written, kill));

    // Each thread writes until a write is not committed. After each committed append it
    // puts the same content to its key `t<i>/rule`. Thread 0's append number `gated` is
    // written, then the server is killed before the thread reads the reply.
    type Puts = Vec<(String, Outcome)>;
    let (calls, puts): (Vec<Vec<Call>>, Vec<Puts>) = std::thread::scope(|scope| {
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let (socket, token, mut gate) =
                    (&socket, &token, if t == 0 { gate.take() } else { None });
                scope.spawn(move || {
                    let (mut calls, mut puts) = (Vec::new(), Vec::new());
                    for n in 0.. {
                        let (content, key) = (format!("{t}-{n}"), format!("t{t}/rule"));
                        let gate = if n == gated { gate.take() } else { None };
                        calls.push(append(socket, token, &content, gate));
                        if calls[n as usize].1 != Outcome::Committed {
                            break;
                        }
                        let put = json!({"key": key, "content": content, "idem_key": content});
                        puts.push((content, post(socket, token, "/v1/facts/put", &put, None).0));
                        if puts[n as usize].1 != Outcome::Committed {
                            break;
                        }
                    }
                    (calls, puts)
                })
            })
            .collect();
        let reached = wrote.recv().is_ok();
        server.child.kill().unwrap();
        let (status, output) = server.wait();
        assert_eq!(status.signal(), Some(9), "{output}");
        let _ = killed.send(());
        assert!(reached, "thread 0 reached request {gated}");
        threads.into_iter().map(|thread| thread.join().unwrap()).unzip()
    });

    let mut server = Process::spawn(&dir.0, &serve);
    server.ready();
    let via = Via::Unix(socket);
    check("8 writers", &calls.concat(), &read_back(&via, &token, "a", "s"));
    // I1 and I9 across SIGKILL: each key holds its last committed put, or a later put whose
    // outcome is unknown. Never an older value, never a put that was not committed.
    for (t, puts) in puts.iter().enumerate() {
        let target = format!("/v1/facts/get?key=t{t}/rule");
        let got = via.call(None, Some(&token), "GET", &target, None).1["fact"]["content"].clone();
        let committed = puts.iter().rposition(|put| put.1 == Outcome::Committed);
        let later = &puts[committed.map_or(0, |i| i + 1)..];
        let held = |content: &str| got == json!(content);
        let allowed = committed.map_or(got.is_null(), |i| held(&puts[i].0))
            || later.iter().any(|put| put.1 == Outcome::Unknown && held(&put.0));
        assert!(allowed, "t{t}/rule holds {got} after {puts:?}");
    }
    eprintln!("8 fact keys: {} puts", puts.concat().len());
}

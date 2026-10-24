use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use zerodb::auth::Auth;
use zerodb::config::{Tenant, Token};
use zerodb::serve::{self, Listeners, ServeConfig, Server};
use zerodb_core::{Engine, EngineOptions};
use zerodb_tape::{SimDisk, SystemClock, TapeOptions};

/// Every entry name in `dir` with its bytes, or `None` when it cannot be read as a file.
pub fn files(dir: &Path) -> BTreeMap<String, Option<Vec<u8>>> {
    let entries = std::fs::read_dir(dir).unwrap().map(|entry| entry.unwrap());
    entries.map(|e| (e.file_name().into_string().unwrap(), std::fs::read(e.path()).ok())).collect()
}

pub fn chmod(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// Makes a mode 0600 FIFO at `path`.
pub fn mkfifo(path: &Path) {
    let status = Command::new("mkfifo").args(["-m", "600"]).arg(path).status();
    assert!(status.unwrap().success(), "mkfifo {}", path.display());
}

/// A Unix listener at `path` that never accepts, with its backlog full. Holds the
/// listener and the queued connections until dropped.
pub fn saturated(path: &Path) -> Vec<OwnedFd> {
    use rustix::net::{AddressFamily, SocketAddrUnix, SocketType};
    let listener = UnixListener::bind(path).unwrap();
    rustix::net::listen(&listener, 0).unwrap();
    let mut held = vec![OwnedFd::from(listener)];
    for _ in 0..1024 {
        let socket = rustix::net::socket(AddressFamily::UNIX, SocketType::STREAM, None).unwrap();
        rustix::io::ioctl_fionbio(&socket, true).unwrap();
        if rustix::net::connect(&socket, &SocketAddrUnix::new(path).unwrap()).is_err() {
            return held;
        }
        held.push(socket);
    }
    panic!("the backlog of {} never filled", path.display());
}

pub const TOKEN_A: &str = "token-a-0123456789abcdefghijklmnopqrstuv";
pub const TOKEN_B: &str = "token-b-0123456789abcdefghijklmnopqrstuv";

/// A private directory with a short path. Socket paths, staging included, must fit
/// `sun_path` (104 bytes on macOS), so it lives under `target/s` by default.
/// Set `ZERODB_TEST_DIR` to a short directory when the checkout path is too deep.
#[derive(Debug)]
pub struct Dir(pub PathBuf);

impl Dir {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::var_os("ZERODB_TEST_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| Path::new(env!("CARGO_TARGET_TMPDIR")).parent().unwrap().join("s"));
        std::fs::create_dir_all(&root).unwrap();
        let name = format!("{:x}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed));
        let path = root.join(name);
        std::fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        let staging = path.join("d/.zs.01234567/s");
        assert!(
            staging.as_os_str().len() < zerodb::listen::SUN_PATH_BYTES,
            "{} is too deep for socket tests, set ZERODB_TEST_DIR to a short directory",
            path.display()
        );
        Self(path)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// An engine on a copy of `disk` with 1 MiB segments and no timed checkpoints. `tune`
/// edits the options first.
pub fn sim_engine(disk: &SimDisk, tune: fn(&mut EngineOptions)) -> Arc<Engine> {
    let tape = TapeOptions { segment_size: 1 << 20, ..TapeOptions::default() };
    let (checkpoint_interval, checkpoint_bytes) = (Duration::from_secs(3600), u64::MAX);
    let mut opts =
        EngineOptions { tape, checkpoint_interval, checkpoint_bytes, ..EngineOptions::default() };
    tune(&mut opts);
    let disk = Arc::new(disk.clone());
    Arc::new(Engine::open_with(disk, Arc::new(SystemClock), "db", opts).unwrap())
}

/// Where a request goes.
#[derive(Clone, Debug)]
pub enum Via {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

impl Via {
    pub fn name(&self) -> &'static str {
        match self {
            Via::Tcp(_) => "tcp",
            Via::Unix(_) => "unix",
        }
    }

    fn connect(&self) -> Box<dyn zerodb::http::Stream> {
        match self {
            Via::Tcp(addr) => Box::new(TcpStream::connect(addr).unwrap()),
            Via::Unix(path) => Box::new(UnixStream::connect(path).unwrap()),
        }
    }

    /// Writes raw bytes and reads until the server closes the connection.
    pub fn raw(&self, bytes: &[u8]) -> String {
        let mut stream = self.connect();
        // macOS fails `setsockopt` with EINVAL once the peer has closed, as after a busy
        // answer. The read below then returns buffered data or EOF at once.
        if let Err(error) = stream.set_read_timeout(Some(Duration::from_secs(10))) {
            assert_eq!(error.raw_os_error(), Some(rustix::io::Errno::INVAL.raw_os_error()));
        }
        stream.write_all(bytes).unwrap();
        let mut out = Vec::new();
        stream.read_to_end(&mut out).unwrap();
        String::from_utf8_lossy(&out).into_owned()
    }

    /// One request with the given `Host` and token. Returns the status and JSON body.
    pub fn call(
        &self,
        host: Option<&str>,
        token: Option<&str>,
        method: &str,
        target: &str,
        body: Option<&Value>,
    ) -> (u16, Value) {
        let auth = token.map(|token| format!("Bearer {token}"));
        let mut headers = vec![];
        headers.extend(host.map(|host| ("Host", host)));
        headers.extend(auth.as_deref().map(|auth| ("Authorization", auth)));
        let body = body.map(Value::to_string).unwrap_or_default();
        let deadline = Instant::now() + Duration::from_secs(30);
        let response = zerodb::http::exchange(
            &mut *self.connect(),
            method,
            target,
            &headers,
            body.as_bytes(),
            deadline,
            64 << 20,
        )
        .unwrap();
        (response.status, serde_json::from_slice(&response.body).unwrap())
    }

    /// A request from tenant A with a loopback `Host`.
    pub fn a(&self, method: &str, target: &str, body: Option<&Value>) -> (u16, Value) {
        self.call(Some("127.0.0.1"), Some(TOKEN_A), method, target, body)
    }
}

/// An append body with one user turn.
pub fn append_body(session: &str, key: &str, content: &str) -> Value {
    json!({"agent": "a", "session": session, "idem_key": key,
           "turns": [{"role": "user", "content": content, "meta": {"k": 1}}]})
}

/// A server on TCP port 0 and a Unix socket `a` in `dir`, serving tenants `a` and `b`.
pub struct Running {
    pub server: Server,
    pub vias: [Via; 2],
    pub instance: String,
}

pub fn serve(engine: Arc<Engine>, dir: &Dir, max_connections: usize) -> Running {
    let socket = dir.0.join("a");
    let instance = zerodb::random_hex(16).unwrap();
    let tenants = [("a", TOKEN_A), ("b", TOKEN_B)].map(|(name, token)| Tenant {
        name: name.into(),
        token: Token::new(token.into()).unwrap(),
    });
    let listeners = Listeners {
        tcp: Some(zerodb::listen::bind_tcp("127.0.0.1:0".parse().unwrap()).unwrap()),
        unix: Some(zerodb::listen::install_unix(&socket, None, &instance).unwrap()),
    };
    let config = ServeConfig {
        auth: Auth::new(&tenants),
        instance: instance.clone(),
        exit_on_poison: false,
        max_connections,
        log_requests: false,
    };
    let server = serve::run(engine, listeners, config).unwrap();
    let vias = [Via::Tcp(server.tcp_addr().unwrap()), Via::Unix(socket)];
    Running { server, vias, instance }
}

/// A spawned `zerodb` process. Its output is collected. Drop kills it.
pub struct Process {
    pub child: Child,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
    readers: Vec<std::thread::JoinHandle<()>>,
}

impl Process {
    /// Runs `zerodb` with `args` in directory `cwd`.
    pub fn spawn(cwd: &Path, args: &[&str]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_zerodb"));
        Self::start(command.args(args).current_dir(cwd).stdin(Stdio::null()))
    }

    /// Runs a command with stdout and stderr collected.
    pub fn start(command: &mut Command) -> Self {
        let mut child = command.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        let mut readers = Vec::new();
        let mut collect = |stream: Box<dyn Read + Send>| {
            let sink = Arc::new(Mutex::new(String::new()));
            let lines = BufReader::new(stream).lines();
            let writer = sink.clone();
            readers.push(std::thread::spawn(move || {
                for line in lines.map_while(Result::ok) {
                    let mut sink = writer.lock().unwrap();
                    sink.push_str(&line);
                    sink.push('\n');
                }
            }));
            sink
        };
        let stdout = collect(Box::new(child.stdout.take().unwrap()));
        let stderr = collect(Box::new(child.stderr.take().unwrap()));
        Self { child, stdout, stderr, readers }
    }

    /// Complete lines printed to stdout so far.
    pub fn stdout(&self) -> String {
        self.stdout.lock().unwrap().clone()
    }

    /// Complete lines printed to stderr so far.
    pub fn stderr(&self) -> String {
        self.stderr.lock().unwrap().clone()
    }

    /// Everything the process printed so far, stdout then stderr.
    fn output(&self) -> String {
        self.stdout() + &self.stderr()
    }

    /// Waits for the startup line and returns `instance` and the TCP address, if any.
    pub fn ready(&mut self) -> (String, Option<SocketAddr>) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let output = self.output();
            if let Some(line) = output.lines().find(|line| line.starts_with("zerodb: serving ")) {
                let field = |name: &str| {
                    let start = line.find(name).unwrap() + name.len();
                    line[start..].split(' ').next().unwrap().to_owned()
                };
                return (field("instance="), field("tcp=").parse().ok());
            }
            let exited = self.child.try_wait().unwrap();
            assert!(exited.is_none() && Instant::now() < deadline, "not serving: {output}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Waits up to 30 s for exit. Returns the status with everything printed.
    pub fn wait(&mut self) -> (ExitStatus, String) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                // A fixed sleep lost stderr on a busy runner. Readers end at the pipes' EOF.
                while !self.readers.iter().all(std::thread::JoinHandle::is_finished) {
                    assert!(Instant::now() < deadline, "pipes still open: {}", self.output());
                    std::thread::sleep(Duration::from_millis(5));
                }
                return (status, self.output());
            }
            assert!(Instant::now() < deadline, "did not exit: {}", self.output());
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A `zerodb mcp` process driven over stdin and stdout.
pub struct Mcp {
    pub process: Process,
    stdin: Option<ChildStdin>,
    read: usize,
    pub next_id: u64,
}

impl Mcp {
    /// `token` goes in `ZERODB_TOKEN`. Without it the flags name the token.
    pub fn start(cwd: &Path, args: &[&str], token: Option<&str>) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_zerodb"));
        command.arg("mcp").args(args).current_dir(cwd).stdin(Stdio::piped());
        command.env_remove("ZERODB_TOKEN");
        if let Some(token) = token {
            command.env("ZERODB_TOKEN", token);
        }
        let mut process = Process::start(&mut command);
        let stdin = process.child.stdin.take();
        Self { process, stdin, read: 0, next_id: 0 }
    }

    pub fn send(&mut self, message: &Value) {
        writeln!(self.stdin.as_mut().unwrap(), "{message}").unwrap();
    }

    /// Sends a request without waiting for its response.
    pub fn post(&mut self, method: &str, params: Value) {
        self.next_id += 1;
        let id = self.next_id;
        self.send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
    }

    /// The next line on stdout: the response to the oldest unanswered request.
    pub fn response(&mut self) -> Value {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(line) = self.process.stdout().lines().nth(self.read) {
                self.read += 1;
                let response: Value = serde_json::from_str(line).unwrap();
                if !response["id"].is_null() {
                    assert_eq!(response["id"], self.read as u64, "responses come in order");
                }
                return response;
            }
            assert!(Instant::now() < deadline, "no response: {}", self.process.stderr());
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    pub fn request(&mut self, method: &str, params: Value) -> Value {
        self.post(method, params);
        self.response()
    }

    pub fn handshake(&mut self) {
        let reply = self.request("initialize", init("2025-11-25"));
        assert_eq!(reply["result"]["protocolVersion"], "2025-11-25");
        self.send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
    }

    pub fn post_call(&mut self, tool: &str, arguments: Value) {
        self.post("tools/call", json!({"name": tool, "arguments": arguments}));
    }

    /// Calls a tool and returns its result.
    pub fn call(&mut self, tool: &str, arguments: Value) -> Value {
        self.post_call(tool, arguments);
        self.response()["result"].clone()
    }

    /// Closes stdin and waits for the exit.
    pub fn close(&mut self) -> (ExitStatus, String) {
        self.stdin = None;
        self.process.wait()
    }
}

pub fn init(version: &str) -> Value {
    json!({"protocolVersion": version, "capabilities": {},
           "clientInfo": {"name": "test", "version": "1"}})
}

pub fn text(result: &Value) -> &str {
    result["content"][0]["text"].as_str().unwrap()
}

/// Polls `done` every 10 ms for up to 30 s.
pub fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// `zerodb init` in `dir`. Returns the token.
pub fn init_config(dir: &Dir) -> String {
    let (status, token) =
        Process::spawn(&dir.0, &["init", "--config", "c.json", "--tenant", "a"]).wait();
    assert!(status.success(), "{token}");
    token.trim().to_owned()
}

pub fn locked(data: &Path) -> bool {
    File::open(data.join("LOCK")).is_ok_and(|file| file.try_lock().is_err())
}

/// Sends SIGKILL to the pid in `data`'s `LOCK`. Returns the pid if the signal was sent.
pub fn kill_lock_pid(data: &Path) -> Option<i32> {
    let lock = std::fs::read_to_string(data.join("LOCK")).ok()?;
    let pid = lock.strip_prefix("pid=")?.split(' ').next()?.parse().ok()?;
    let target = rustix::process::Pid::from_raw(pid)?;
    rustix::process::kill_process(target, rustix::process::Signal::KILL).ok()?;
    Some(pid)
}

/// Kills the server holding `data`'s lock, found through `LOCK`, and waits until the
/// lock is free. Auto-started servers outlive their adapters, so tests stop them here.
pub fn kill_holder(data: &Path) -> bool {
    if locked(data) {
        kill_lock_pid(data);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while locked(data) {
        if Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    true
}

/// Stops the servers of these data directories, also when a test fails.
pub struct Holders(pub Vec<PathBuf>);

impl Drop for Holders {
    fn drop(&mut self) {
        for data in &self.0 {
            kill_holder(data);
        }
    }
}

/// A new mode 0600 file in `dir`. Returns its path.
pub fn private_file(dir: &Dir, name: &str, content: &str) -> String {
    let path = dir.0.join(name);
    let mut file = File::options().write(true).create_new(true).mode(0o600).open(&path).unwrap();
    file.write_all(content.as_bytes()).unwrap();
    path.to_str().unwrap().to_owned()
}

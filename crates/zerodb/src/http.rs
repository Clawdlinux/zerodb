//! A small HTTP/1.1 subset for the server and its clients, over TCP or Unix sockets.
//!
//! `httparse` parses the request line and headers. This module does the framing:
//! limits, total deadlines, `Content-Length` and keep-alive. Per request the order is
//! head, then the caller's `Host` and auth check, then the length cap, then the body.
//! A request that fails before the body gets its error and the connection closes
//! with the body unread.

use serde_json::{json, Value};
use std::fmt;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::os::unix::net::UnixStream;
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// A byte stream with timeouts, implemented for TCP and Unix sockets.
pub trait Stream: Read + Write {
    /// Sets the timeout of each blocking read.
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    /// Sets the timeout of each blocking write.
    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    /// Shuts down one or both halves.
    fn shutdown(&self, how: Shutdown) -> io::Result<()>;
}

macro_rules! impl_stream {
    ($type:ty) => {
        impl Stream for $type {
            fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
                <$type>::set_read_timeout(self, timeout)
            }
            fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
                <$type>::set_write_timeout(self, timeout)
            }
            fn shutdown(&self, how: Shutdown) -> io::Result<()> {
                <$type>::shutdown(self, how)
            }
        }
    };
}
impl_stream!(TcpStream);
impl_stream!(UnixStream);

/// Size limits and total deadlines. Deadlines bound a whole phase, not one read,
/// so a slow trickle cannot hold a thread.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Largest request head, request line included.
    pub head_bytes: usize,
    /// Most header lines.
    pub header_lines: usize,
    /// Largest request body.
    pub body_bytes: u64,
    /// Time to receive a whole head, counted from its first byte or from accept.
    pub head_timeout: Duration,
    /// Time to receive a whole body.
    pub body_timeout: Duration,
    /// Time a kept-alive connection may wait for its next request.
    pub idle_timeout: Duration,
    /// Time to write one response.
    pub write_timeout: Duration,
    /// After closing our half, time spent discarding input so the peer reads our
    /// response before a TCP reset can drop it.
    pub linger: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            head_bytes: 16 * 1024,
            header_lines: 64,
            body_bytes: 8 << 20,
            head_timeout: Duration::from_secs(10),
            body_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            linger: Duration::from_secs(1),
        }
    }
}

/// A parsed and framed request head.
pub struct Head {
    /// Request method, such as `GET`.
    pub method: String,
    /// Request target, path and query.
    pub target: String,
    /// 0 for HTTP/1.0, 1 for HTTP/1.1.
    pub minor_version: u8,
    /// Declared body length. Zero without `Content-Length`.
    pub content_length: u64,
    headers: Vec<(String, Vec<u8>)>,
    expect_continue: bool,
    keep_alive: bool,
}

impl Head {
    /// The target without its query string.
    pub fn path(&self) -> &str {
        self.target.split_once('?').map_or(&self.target, |(path, _)| path)
    }

    /// Every value of a header. `name` must be lowercase.
    pub fn headers<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.headers
            .iter()
            .filter(move |(key, _)| key.as_str() == name)
            .map(|(_, value)| &value[..])
    }

    /// The first value of a header. `name` must be lowercase.
    pub fn header<'a>(&'a self, name: &'a str) -> Option<&'a [u8]> {
        self.headers(name).next()
    }
}

impl fmt::Debug for Head {
    /// Shows header names only and no query, so tokens and session names stay out of logs.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Head")
            .field("method", &self.method)
            .field("path", &self.path())
            .field("minor_version", &self.minor_version)
            .field("content_length", &self.content_length)
            .field("headers", &self.headers.iter().map(|(name, _)| name).collect::<Vec<_>>())
            .finish()
    }
}

/// A request with its whole body.
pub struct Request {
    /// The head.
    pub head: Head,
    /// Exactly `head.content_length` bytes.
    pub body: Vec<u8>,
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
            .field("head", &self.head)
            .field("body_len", &self.body.len())
            .finish()
    }
}

/// A JSON response.
pub struct Response {
    /// Status code.
    pub status: u16,
    /// Body bytes.
    pub body: Vec<u8>,
    /// `Content-Type`.
    pub content_type: &'static str,
    /// A share of a [`Budget`] that lasts until the response is dropped, after its write.
    pub(crate) _held: Option<Held>,
}

impl Response {
    /// A response with a JSON body.
    pub fn json(status: u16, body: &Value) -> Self {
        Self::text(status, "application/json", body.to_string())
    }

    /// A response with a text body of `content_type`.
    pub fn text(status: u16, content_type: &'static str, body: String) -> Self {
        Self { status, body: body.into_bytes(), content_type, _held: None }
    }

    /// `{"outcome": "not_committed", "error": <error>, "message"?: <message>}`. Nothing
    /// reached the engine.
    pub fn error(status: u16, error: &str, message: Option<&str>) -> Self {
        let mut body = json!({"outcome": "not_committed", "error": error});
        if let Some(message) = message {
            body["message"] = json!(message);
        }
        Self::json(status, &body)
    }
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .field("body_len", &self.body.len())
            .finish()
    }
}

/// The server side of one connection.
pub struct ServerConn<S> {
    stream: S,
    limits: Limits,
    buf: Vec<u8>,
    accepted: Instant,
    served: u64,
    keep_alive: bool,
    open: bool,
}

impl<S> fmt::Debug for ServerConn<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerConn")
            .field("served", &self.served)
            .field("open", &self.open)
            .finish_non_exhaustive()
    }
}

impl<S: Stream> ServerConn<S> {
    /// Wraps an accepted connection. The first head deadline starts now.
    pub fn new(stream: S, limits: Limits) -> Self {
        Self {
            stream,
            limits,
            buf: Vec::new(),
            accepted: Instant::now(),
            served: 0,
            keep_alive: false,
            open: true,
        }
    }

    /// Reads the next request. `check` sees the head before the body is read and
    /// returns the response to send instead, such as 401 or 403. Returns `None`
    /// once the connection is closed: by the peer, by a deadline, or after an error
    /// response. Every returned request needs one [`ServerConn::respond`] call.
    pub fn next_request<T>(
        &mut self,
        check: impl FnOnce(&Head) -> Result<T, Response>,
    ) -> Option<(Request, T)> {
        if !self.open {
            return None;
        }
        let head = match self.read_head() {
            Ok(head) => head,
            Err(rejection) => {
                if let Some(response) = rejection {
                    let _ = self.write_response(&response, true);
                }
                self.close();
                return None;
            }
        };
        let checked = match check(&head) {
            Ok(checked) => checked,
            Err(response) => return self.reject(&response),
        };
        if head.content_length > self.limits.body_bytes {
            return self.reject(&Response::error(413, "too_large", None));
        }
        if head.expect_continue && head.content_length > 0 && head.minor_version == 1 {
            let deadline = Instant::now() + self.limits.write_timeout;
            if write_until(&mut self.stream, b"HTTP/1.1 100 Continue\r\n\r\n", deadline).is_err() {
                self.close();
                return None;
            }
        }
        let Ok(body) = self.read_body(head.content_length) else {
            self.close();
            return None;
        };
        self.served += 1;
        self.keep_alive = head.keep_alive;
        Some((Request { head, body }, checked))
    }

    /// Writes the response to the last request. Closes the connection after it when
    /// the request asked to, the response says so, or the write fails.
    pub fn respond(&mut self, response: &Response) -> io::Result<()> {
        if !self.open {
            return Err(io::Error::new(ErrorKind::NotConnected, "connection closed"));
        }
        let close = !self.keep_alive;
        let result = self.write_response(response, close);
        if close || result.is_err() {
            self.close();
        }
        result
    }

    fn reject<T>(&mut self, response: &Response) -> Option<T> {
        let _ = self.write_response(response, true);
        self.close();
        None
    }

    /// `Err(None)` ends the connection quietly. `Err(Some(_))` answers first.
    fn read_head(&mut self) -> Result<Head, Option<Response>> {
        let now = Instant::now();
        let mut head_deadline = if self.served == 0 {
            Some(self.accepted + self.limits.head_timeout)
        } else if !self.buf.is_empty() {
            Some(now + self.limits.head_timeout)
        } else {
            None
        };
        let idle_deadline = now + self.limits.idle_timeout;
        let mut chunk = [0; 8192];
        // Bytes already searched for the blank line that ends a head. httparse runs only
        // once that line or the size limit is reached, so a trickle costs one parse.
        let mut scanned = 0;
        loop {
            skip_leading_blank_lines(&mut self.buf, &mut scanned);
            let ready = self.buf.len() >= self.limits.head_bytes
                || ends_head(&self.buf[scanned.saturating_sub(2)..]);
            scanned = self.buf.len();
            if ready {
                if let Some((head, used)) = parse_head(&self.buf, &self.limits).map_err(Some)? {
                    self.buf.drain(..used);
                    return Ok(head);
                }
            }
            let deadline = head_deadline.unwrap_or(idle_deadline);
            match read_until(&mut self.stream, &mut chunk, deadline) {
                Ok(0) | Err(_) => return Err(None),
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            }
            head_deadline.get_or_insert_with(|| Instant::now() + self.limits.head_timeout);
        }
    }

    /// Grows the body as bytes arrive, never past the declared length. A connection
    /// holds at most `min(declared, 2 x received)` body bytes, so a client that declares
    /// 8 MiB and sends nothing costs nothing. The worst case per connection is the
    /// body cap, which bounds memory together with the connection cap.
    fn read_body(&mut self, len: u64) -> io::Result<Vec<u8>> {
        let len = usize::try_from(len).map_err(|_| io::Error::from(ErrorKind::InvalidData))?;
        let deadline = Instant::now() + self.limits.body_timeout;
        let buffered = len.min(self.buf.len());
        let mut body: Vec<u8> = self.buf.drain(..buffered).collect();
        let mut chunk = [0; 16 * 1024];
        while body.len() < len {
            let want = (len - body.len()).min(chunk.len());
            let n = read_until(&mut self.stream, &mut chunk[..want], deadline)?;
            if n == 0 {
                return Err(ErrorKind::UnexpectedEof.into());
            }
            if body.capacity() - body.len() < n {
                let target = (body.capacity() * 2).max(body.len() + n).min(len);
                body.reserve_exact(target - body.len());
            }
            body.extend_from_slice(&chunk[..n]);
        }
        Ok(body)
    }

    fn write_response(&mut self, response: &Response, close: bool) -> io::Result<()> {
        let deadline = Instant::now() + self.limits.write_timeout;
        write_response(&mut self.stream, response, close, deadline)
    }

    /// Closes our half, then discards input until EOF or the linger deadline.
    fn close(&mut self) {
        if !self.open {
            return;
        }
        self.open = false;
        self.buf = Vec::new();
        let _ = self.stream.flush();
        let _ = self.stream.shutdown(Shutdown::Write);
        let deadline = Instant::now() + self.limits.linger;
        let mut sink = [0; 8192];
        while let Ok(1..) = read_until(&mut self.stream, &mut sink, deadline) {}
    }
}

/// Drops empty lines before a request line, which RFC 9112 lets a server ignore.
/// Without this, each one would look like the end of a head.
fn skip_leading_blank_lines(buf: &mut Vec<u8>, scanned: &mut usize) {
    let mut start = 0;
    loop {
        match buf.get(start..) {
            Some([b'\n', ..]) => start += 1,
            Some([b'\r', b'\n', ..]) => start += 2,
            _ => break,
        }
    }
    if start > 0 {
        buf.drain(..start);
        *scanned = scanned.saturating_sub(start);
    }
}

/// Whether the bytes hold the blank line that ends a head: `\n\n` or `\n\r\n`.
fn ends_head(bytes: &[u8]) -> bool {
    bytes.windows(2).any(|pair| pair == b"\n\n")
        || bytes.windows(3).any(|triple| triple == b"\n\r\n")
}

/// Parses a complete head, or returns `None` when more bytes are needed.
fn parse_head(buf: &[u8], limits: &Limits) -> Result<Option<(Head, usize)>, Response> {
    let too_large = || Response::error(431, "headers_too_large", Some("request head too large"));
    let mut headers = vec![httparse::EMPTY_HEADER; limits.header_lines];
    let mut request = httparse::Request::new(&mut headers);
    let used = match request.parse(buf) {
        Ok(httparse::Status::Complete(used)) if used <= limits.head_bytes => used,
        Ok(httparse::Status::Partial) if buf.len() < limits.head_bytes => return Ok(None),
        Ok(_) | Err(httparse::Error::TooManyHeaders) => return Err(too_large()),
        Err(_) => return Err(Response::error(400, "bad_request", Some("malformed request head"))),
    };
    let bad = |message: &str| Response::error(400, "bad_request", Some(message));
    let headers: Vec<(String, Vec<u8>)> = request
        .headers
        .iter()
        .map(|header| (header.name.to_ascii_lowercase(), header.value.to_vec()))
        .collect();
    let values = |name: &'static str| headers.iter().filter(move |(key, _)| key.as_str() == name);
    if values("transfer-encoding").next().is_some() {
        return Err(Response::error(
            501,
            "not_implemented",
            Some("Transfer-Encoding is not supported"),
        ));
    }
    if values("content-length").count() > 1 {
        return Err(bad("duplicate Content-Length"));
    }
    if values("host").count() > 1 {
        return Err(bad("duplicate Host"));
    }
    let content_length = match values("content-length").next() {
        None => 0,
        Some((_, value)) => {
            parse_length(value).ok_or_else(|| bad("Content-Length must be 1 to 19 digits"))?
        }
    };
    let method = request.method.unwrap_or_default().to_owned();
    if method == "GET" && content_length > 0 {
        return Err(bad("GET must not have a body"));
    }
    let mut expect_continue = false;
    for (_, value) in values("expect") {
        if !value.eq_ignore_ascii_case(b"100-continue") {
            return Err(Response::error(417, "expectation_failed", Some("only 100-continue")));
        }
        expect_continue = true;
    }
    let minor_version = request.version.unwrap_or(0);
    let close = values("connection").any(|(_, value)| {
        value
            .split(|byte| *byte == b',')
            .any(|token| token.trim_ascii().eq_ignore_ascii_case(b"close"))
    });
    let head = Head {
        method,
        target: request.path.unwrap_or_default().to_owned(),
        minor_version,
        content_length,
        keep_alive: minor_version == 1 && !close,
        expect_continue,
        headers,
    };
    Ok(Some((head, used)))
}

/// 1 to 19 ASCII digits. No sign, no spaces, no list. 19 digits always fit a `u64`.
fn parse_length(value: &[u8]) -> Option<u64> {
    if !(1..=19).contains(&value.len()) || !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    value
        .iter()
        .try_fold(0u64, |total, digit| total.checked_mul(10)?.checked_add(u64::from(digit - b'0')))
}

/// Writes one response by `deadline`, the head and then the body, without copying the
/// body. The reason phrase is empty, which RFC 9112 allows: clients must ignore it.
pub fn write_response<S: Stream + ?Sized>(
    stream: &mut S,
    response: &Response,
    close: bool,
    deadline: Instant,
) -> io::Result<()> {
    let (status, length, kind) = (response.status, response.body.len(), response.content_type);
    let connection = if close { "close" } else { "keep-alive" };
    let head = format!(
        "HTTP/1.1 {status} \r\nContent-Type: {kind}\r\nContent-Length: {length}\r\nConnection: {connection}\r\n\r\n"
    );
    write_until(stream, head.as_bytes(), deadline)?;
    write_until(stream, &response.body, deadline)
}

/// A shared limit on what is in flight, such as parses or response bytes. A taker waits
/// for room. One that exceeds the whole limit goes alone, when nothing else is held.
pub(crate) struct Budget {
    used: Mutex<usize>,
    freed: Condvar,
    limit: usize,
}

/// A share of a [`Budget`], given back on drop.
pub(crate) struct Held(&'static Budget, usize);

impl Budget {
    pub(crate) const fn new(limit: usize) -> Self {
        Self { used: Mutex::new(0), freed: Condvar::new(), limit }
    }

    /// Takes `amount`, waiting up to `wait` for room. `None` when the wait runs out.
    pub(crate) fn take(&'static self, amount: usize, wait: Duration) -> Option<Held> {
        let used = self.used.lock().unwrap_or_else(PoisonError::into_inner);
        let full = |used: &mut usize| *used > 0 && used.saturating_add(amount) > self.limit;
        let waited = self.freed.wait_timeout_while(used, wait, full);
        let (mut used, waited) = waited.unwrap_or_else(PoisonError::into_inner);
        if waited.timed_out() {
            return None;
        }
        *used += amount;
        Some(Held(self, amount))
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        *self.0.used.lock().unwrap_or_else(PoisonError::into_inner) -= self.1;
        self.0.freed.notify_all();
    }
}

/// Time left before `deadline`, or `TimedOut` when none is.
pub(crate) fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|left| !left.is_zero())
        .ok_or_else(|| io::Error::new(ErrorKind::TimedOut, "deadline passed"))
}

const EINVAL: i32 = rustix::io::Errno::INVAL.raw_os_error();

fn retryable(error: &io::Error) -> bool {
    matches!(error.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::TimedOut)
}

fn read_until<S: Stream + ?Sized>(
    stream: &mut S,
    buf: &mut [u8],
    deadline: Instant,
) -> io::Result<usize> {
    loop {
        // macOS fails `setsockopt` with EINVAL once the peer has closed. The read then
        // returns at once, so the deadline still holds. Same for writes below.
        match stream.set_read_timeout(Some(remaining(deadline)?)) {
            Err(error) if error.raw_os_error() != Some(EINVAL) => return Err(error),
            _ => {}
        }
        match stream.read(buf) {
            Err(error) if retryable(&error) => {}
            result => return result,
        }
    }
}

fn write_until<S: Stream + ?Sized>(
    stream: &mut S,
    mut bytes: &[u8],
    deadline: Instant,
) -> io::Result<()> {
    while !bytes.is_empty() {
        match stream.set_write_timeout(Some(remaining(deadline)?)) {
            Err(error) if error.raw_os_error() != Some(EINVAL) => return Err(error),
            _ => {}
        }
        match stream.write(bytes) {
            Ok(0) => return Err(ErrorKind::WriteZero.into()),
            Ok(n) => bytes = &bytes[n..],
            Err(error) if retryable(&error) => {}
            Err(error) => return Err(error),
        }
    }
    stream.flush()
}

/// A response read by the client half.
pub struct ClientResponse {
    /// Status code.
    pub status: u16,
    /// Body bytes.
    pub body: Vec<u8>,
}

impl fmt::Debug for ClientResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientResponse")
            .field("status", &self.status)
            .field("body_len", &self.body.len())
            .finish()
    }
}

/// Writes one request and reads one `Content-Length` response, all before `deadline`.
/// `headers` should include `Host`. Without a `Connection` header it sends `close`.
pub fn exchange<S: Stream + ?Sized>(
    stream: &mut S,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    deadline: Instant,
    max_body: u64,
) -> io::Result<ClientResponse> {
    let mut fields =
        headers.iter().flat_map(|(name, value)| [*name, *value]).chain([method, target]);
    if fields.any(|field| field.contains(['\r', '\n'])) {
        return Err(io::Error::new(ErrorKind::InvalidInput, "line break inside a request field"));
    }
    let mut head = format!("{method} {target} HTTP/1.1\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    if method != "GET" || !body.is_empty() {
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    if !headers.iter().any(|(name, _)| name.eq_ignore_ascii_case("connection")) {
        head.push_str("Connection: close\r\n");
    }
    head.push_str("\r\n");
    let mut bytes = head.into_bytes();
    bytes.extend_from_slice(body);
    write_until(stream, &bytes, deadline)?;

    let limits = Limits::default();
    let invalid = |message: &str| io::Error::new(ErrorKind::InvalidData, message);
    let mut buf = Vec::new();
    let mut chunk = [0; 8192];
    let (status, length, used) = loop {
        match read_until(stream, &mut chunk, deadline)? {
            0 => return Err(ErrorKind::UnexpectedEof.into()),
            n => buf.extend_from_slice(&chunk[..n]),
        }
        let mut headers = vec![httparse::EMPTY_HEADER; limits.header_lines];
        let mut response = httparse::Response::new(&mut headers);
        match response.parse(&buf) {
            Ok(httparse::Status::Complete(used)) if used <= limits.head_bytes => {
                let mut lengths = response
                    .headers
                    .iter()
                    .filter(|header| header.name.eq_ignore_ascii_case("content-length"));
                let length = match (lengths.next(), lengths.next()) {
                    (Some(header), None) => parse_length(header.value),
                    _ => None,
                };
                let chunked = response
                    .headers
                    .iter()
                    .any(|header| header.name.eq_ignore_ascii_case("transfer-encoding"));
                match (response.code, length) {
                    (Some(status), Some(length)) if !chunked => break (status, length, used),
                    _ => return Err(invalid("response without one valid Content-Length")),
                }
            }
            Ok(httparse::Status::Partial) if buf.len() < limits.head_bytes => {}
            _ => return Err(invalid("malformed response head")),
        }
    };
    if length > max_body {
        return Err(invalid("response body too large"));
    }
    let length = usize::try_from(length).map_err(|_| invalid("response body too large"))?;
    let mut body = buf.split_off(used);
    body.truncate(length);
    let mut filled = body.len();
    body.resize(length, 0);
    while filled < length {
        match read_until(stream, &mut body[filled..], deadline)? {
            0 => return Err(ErrorKind::UnexpectedEof.into()),
            n => filled += n,
        }
    }
    Ok(ClientResponse { status, body })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::type_complexity)]
    use super::*;
    use proptest::prelude::*;
    use std::sync::{Arc, Mutex};
    use std::thread;

    enum Step {
        Send(Vec<u8>),
        Sleep(u64),
        /// Waits until the client has read this text, as a real client waits for
        /// `100 Continue` before it sends the body.
        Await(&'static str),
    }

    struct Run {
        bodies: Vec<Vec<u8>>,
        wire: String,
        elapsed: Duration,
        write_errors: usize,
    }

    /// Refuses a request with an `X-Deny` header, as a failed auth check does.
    fn check(head: &Head) -> Result<(), Response> {
        match head.header("x-deny") {
            Some(_) => Err(Response::error(401, "unauthorized", None)),
            None => Ok(()),
        }
    }

    /// Drives one server connection against a scripted client over a socket pair. With
    /// `unread`, each reply is 4 MiB and the client reads nothing until the server is done.
    fn run(limits: Limits, steps: Vec<Step>, unread: bool) -> Run {
        let (server, client) = UnixStream::pair().unwrap();
        let wire = Arc::new(Mutex::new(Vec::new()));
        let (mut writer, seen) = (client.try_clone().unwrap(), wire.clone());
        let writer = thread::spawn(move || {
            for step in steps {
                match step {
                    Step::Send(bytes) if writer.write_all(&bytes).is_err() => return,
                    Step::Send(_) => {}
                    Step::Sleep(ms) => thread::sleep(Duration::from_millis(ms)),
                    Step::Await(text) => {
                        // Bounded, so a server that never sends it fails the case, not hangs.
                        let until = Instant::now() + Duration::from_secs(10);
                        while !String::from_utf8_lossy(&seen.lock().unwrap()).contains(text)
                            && Instant::now() < until
                        {
                            thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
            }
            let _ = writer.shutdown(Shutdown::Write);
        });
        let read_all = |mut stream: UnixStream, wire: Arc<Mutex<Vec<u8>>>| {
            let mut chunk = [0; 8192];
            while let Ok(n @ 1..) = stream.read(&mut chunk) {
                wire.lock().unwrap().extend_from_slice(&chunk[..n]);
            }
        };
        let reader = (!unread).then(|| {
            let (stream, wire) = (client.try_clone().unwrap(), wire.clone());
            thread::spawn(move || read_all(stream, wire))
        });
        let (start, mut conn) = (Instant::now(), ServerConn::new(server, limits));
        let (mut bodies, mut write_errors) = (Vec::new(), 0);
        while let Some((request, ())) = conn.next_request(check) {
            assert_eq!(request.body.len() as u64, request.head.content_length);
            bodies.push(request.body);
            let reply = "x".repeat(if unread { 4 << 20 } else { 2 });
            write_errors +=
                conn.respond(&Response::text(200, "text/plain", reply)).is_err() as usize;
        }
        let elapsed = start.elapsed();
        drop(conn);
        match reader {
            Some(reader) => reader.join().unwrap(),
            None => read_all(client, wire.clone()),
        }
        writer.join().unwrap();
        let wire = String::from_utf8_lossy(&wire.lock().unwrap()).into_owned();
        Run { bodies, wire, elapsed, write_errors }
    }

    /// Each response on the wire as its status, `Connection` header and error name, if any.
    fn responses(wire: &str) -> Vec<String> {
        let response = |text: &str| {
            let (head, body) = text.split_once("\r\n\r\n").unwrap_or((text, ""));
            let connection = head.lines().find_map(|line| line.strip_prefix("Connection: "));
            let error =
                body.split_once(r#""error":""#).and_then(|(_, rest)| rest.split('"').next());
            let fields = [text.get(..3), connection, error];
            fields.into_iter().flatten().collect::<Vec<_>>().join(" ")
        };
        wire.split("HTTP/1.1 ").skip(1).map(response).collect()
    }

    fn send(text: &str) -> Vec<Step> {
        vec![Step::Send(text.as_bytes().to_vec())]
    }

    fn trickle(text: &str, ms: u64) -> Vec<Step> {
        text.bytes().flat_map(|byte| [Step::Send(vec![byte]), Step::Sleep(ms)]).collect()
    }

    fn header_lines(n: usize) -> String {
        let lines: String = (0..n).map(|i| format!("X-{i}: v\r\n")).collect();
        format!("GET / HTTP/1.1\r\n{lines}\r\n")
    }

    /// The published deadlines. A case about one deadline shortens only that one to
    /// [`SHORT`], so a thread delayed under load cannot fail it on another deadline.
    fn relaxed() -> Limits {
        Limits { body_bytes: 64, linger: Duration::from_millis(20), ..Limits::default() }
    }

    const SHORT: Duration = Duration::from_millis(150);

    #[test]
    fn framing_accepts_and_rejects() {
        let get = |rest: &str| send(&format!("GET / HTTP/1.1\r\n{rest}"));
        let post = |rest: &str| send(&format!("POST / HTTP/1.1\r\n{rest}"));
        let mut continued = post("Expect: 100-continue\r\nContent-Length: 4\r\n\r\n");
        continued.extend([Step::Await("100 Continue"), Step::Send(b"body".to_vec())]);
        let mut split = get("X: a\r");
        split.extend([Step::Sleep(10), Step::Send(b"\n\r".to_vec())]);
        split.extend([Step::Sleep(10), Step::Send(b"\n".to_vec())]);
        let pipelined = "Content-Length: 3\r\n\r\noneGET / HTTP/1.1\r\n\r\nPOST / HTTP/1.1\r\n\
                         Content-Length: 3\r\n\r\ntwo";
        let one_by_one = "\r\n\r\nPOST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello";
        let http10 = send("POST / HTTP/1.0\r\nContent-Length: 2\r\n\r\nhiGET / HTTP/1.1\r\n\r\n");
        let over_cap = "Expect: 100-continue\r\nContent-Length: 65\r\n\r\n";
        let denied = format!("X-Deny: 1\r\n{}", over_cap.replace("65", "64"));
        let (closing, again) = ("Connection: keep-alive, close\r\n\r\n", "GET / HTTP/1.1\r\n\r\n");
        let (chunked, expect) = (&["501 close not_implemented"], &["417 close expectation_failed"]);
        let (pad, large_head) = ("a".repeat(16 * 1024), &["431 close headers_too_large"]);
        let at_cap = post(&format!("Content-Length: 64\r\n\r\n{}", "b".repeat(64)));
        let (close, keep, bad) = (&["200 close"], &["200 keep-alive"], &["400 close bad_request"]);
        // (case, client steps, each response as `<status> <Connection> <error>`, request bodies)
        let cases: Vec<(&str, Vec<Step>, &[&str], &[&[u8]])> = vec![
            ("get keeps the connection", get("Host: a\r\n\r\n"), keep, &[b""]),
            ("post with body", post("Content-Length: 5\r\n\r\nhello"), keep, &[b"hello"]),
            ("pipelined requests", post(pipelined), &["200 keep-alive"; 3], &[b"one", b"", b"two"]),
            ("http/1.0 closes after one request", http10, close, &[b"hi"]),
            ("connection close is honored", get(&format!("{closing}{again}")), close, &[b""]),
            (
                "100-continue after the checks pass",
                continued,
                &["100", "200 keep-alive"],
                &[b"body"],
            ),
            ("get with zero length", get("Content-Length: 0\r\n\r\n"), keep, &[b""]),
            (
                "19 digit length",
                post("Content-Length: 0000000000000000003\r\n\r\nabc"),
                keep,
                &[b"abc"],
            ),
            ("body at the cap", at_cap, keep, &[&[b'b'; 64]]),
            ("one byte at a time", trickle(one_by_one, 0), keep, &[b"hello"]),
            ("head end split across reads", split, keep, &[b""]),
            ("64 header lines", send(&header_lines(64)), keep, &[b""]),
            ("transfer-encoding", post("Transfer-Encoding: chunked\r\n\r\n"), chunked, &[]),
            ("two lengths", post("Content-Length: 1\r\nContent-Length: 1\r\n\r\nx"), bad, &[]),
            ("duplicate host", get("Host: localhost\r\nHost: evil.example\r\n\r\n"), bad, &[]),
            ("obs-fold", get("X-A: a\r\n b\r\n\r\n"), bad, &[]),
            ("body on get", get("Content-Length: 3\r\n\r\nabc"), bad, &[]),
            ("length with a sign", post("Content-Length: +3\r\n\r\nabc"), bad, &[]),
            ("negative length", post("Content-Length: -1\r\n\r\n"), bad, &[]),
            ("length list", post("Content-Length: 3, 3\r\n\r\nabc"), bad, &[]),
            ("length with inner space", post("Content-Length: 1 2\r\n\r\nabc"), bad, &[]),
            ("20 digit length", post("Content-Length: 00000000000000000003\r\n\r\n"), bad, &[]),
            ("head over 16 KiB", get(&format!("X-Pad: {pad}\r\n\r\n")), large_head, &[]),
            ("16 KiB without a line break", send(&format!("GET /{pad}")), large_head, &[]),
            ("65 header lines", send(&header_lines(65)), large_head, &[]),
            ("body over the cap skips 100-continue", post(over_cap), &["413 close too_large"], &[]),
            ("failed check skips 100-continue", post(&denied), &["401 close unauthorized"], &[]),
            ("malformed request line", send("GARBAGE\r\n\r\n"), bad, &[]),
            ("unsupported version", send("GET / HTTP/2.0\r\n\r\n"), bad, &[]),
            ("unknown expectation", post("Expect: x\r\nContent-Length: 1\r\n\r\nx"), expect, &[]),
            (
                "no keep-alive after an error",
                get(&format!("Content-Length: 1\r\n\r\nx{again}")),
                bad,
                &[],
            ),
            ("truncated head", get("Host"), &[], &[]),
            ("truncated body", post("Content-Length: 10\r\n\r\nabc"), &[], &[]),
        ];
        for (case, steps, expected, bodies) in cases {
            let run = run(relaxed(), steps, false);
            assert_eq!(responses(&run.wire), expected, "{case}: responses");
            assert_eq!(run.bodies, bodies, "{case}: bodies");
            // Each error answers before the body and says nothing was written.
            let errors = run.wire.matches(r#""error":"#).count();
            assert_eq!(run.wire.matches(r#""outcome":"not_committed""#).count(), errors, "{case}");
            assert!(run.elapsed < Duration::from_secs(10), "{case}: took {:?}", run.elapsed);
        }
        // The client refuses a line break inside a field before it writes anything.
        let injected = [("Host", "localhost\r\nX-Evil: 1")];
        let deadline = Instant::now() + Duration::from_secs(5);
        let error =
            exchange(&mut UnixStream::pair().unwrap().0, "GET", "/", &injected, b"", deadline, 1)
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "header injection is refused");
    }

    #[test]
    fn deadlines_cut_off_slow_peers() {
        let defaults = Limits::default();
        let sizes = (defaults.head_bytes, defaults.header_lines, defaults.body_bytes);
        assert_eq!(sizes, (16 * 1024, 64, 8 << 20), "published limits");
        let deadlines = [
            defaults.head_timeout,
            defaults.body_timeout,
            defaults.idle_timeout,
            defaults.write_timeout,
        ];
        assert_eq!(deadlines, [10, 30, 30, 30].map(Duration::from_secs), "published deadlines");
        let head =
            trickle("GET / HTTP/1.1\r\nHost: localhost\r\nX-Slow: aaaaaaaaaaaaaaaaaa\r\n\r\n", 10);
        let mut body = send("POST / HTTP/1.1\r\nContent-Length: 60\r\n\r\n");
        body.extend(trickle(&"b".repeat(60), 10));
        let mut idle = send("GET / HTTP/1.1\r\n\r\n");
        idle.push(Step::Sleep(800));
        let (get, keep) = (send("GET / HTTP/1.1\r\n\r\n"), &["200 keep-alive"]);
        // (case, the deadline set to SHORT, client steps, responses, reply left unread)
        let cases: [(&str, fn(&mut Limits), Vec<Step>, &[&str], bool); 4] = [
            ("slow head", |l| l.head_timeout = SHORT, head, &[], false),
            ("slow body", |l| l.body_timeout = SHORT, body, &[], false),
            ("idle keep-alive", |l| l.idle_timeout = SHORT, idle, keep, false),
            ("unread response", |l| l.write_timeout = SHORT, get, keep, true),
        ];
        for (case, shorten, steps, expected, unread) in cases {
            let mut limits = relaxed();
            shorten(&mut limits);
            let run = run(limits, steps, unread);
            assert_eq!(responses(&run.wire), expected, "{case}");
            let counts = (run.bodies.len(), run.write_errors);
            assert_eq!(
                counts,
                (expected.len(), usize::from(unread)),
                "{case}: bodies, write errors"
            );
            assert!(run.elapsed < Duration::from_millis(500), "{case}: took {:?}", run.elapsed);
        }
    }

    #[test]
    fn budget_waits_and_lets_an_oversized_taker_go_alone() {
        static BUDGET: Budget = Budget::new(10);
        let short = Duration::from_millis(20);
        let first = BUDGET.take(6, short).unwrap();
        assert!(BUDGET.take(6, short).is_none(), "6 + 6 is over 10");
        let second = BUDGET.take(4, short).unwrap();
        drop(first);
        assert!(BUDGET.take(7, short).is_none(), "4 + 7 is over 10");
        drop(second);
        let alone = BUDGET.take(25, short).unwrap();
        assert!(BUDGET.take(1, short).is_none(), "nothing joins an oversized taker");
        let waiter = thread::spawn(|| BUDGET.take(5, Duration::from_secs(10)).is_some());
        thread::sleep(short);
        drop(alone);
        assert!(waiter.join().unwrap(), "a waiter gets room once it is freed");
    }

    fn vocabulary() -> impl Strategy<Value = Vec<u8>> {
        let pieces = prop::sample::select(vec![
            "GET ",
            "POST ",
            "/ ",
            "HTTP/1.1",
            "HTTP/1.0",
            "\r\n",
            "\n",
            " ",
            ":",
            ",",
            "Content-Length: ",
            "Transfer-Encoding: chunked",
            "Host: a",
            "Expect: 100-continue",
            "Connection: close",
            "0",
            "1",
            "7",
            "64",
            "65",
            "99999999999999999999",
            "abc",
            "\0",
        ]);
        prop::collection::vec(pieces, 0..40).prop_map(|pieces| pieces.concat().into_bytes())
    }

    fn framed() -> impl Strategy<Value = Vec<u8>> {
        (0u64..100, prop::collection::vec(any::<u8>(), 0..200)).prop_map(|(length, tail)| {
            let mut bytes =
                format!("POST / HTTP/1.1\r\nContent-Length: {length}\r\n\r\n").into_bytes();
            bytes.extend(tail);
            bytes
        })
    }

    proptest! {
        #[test]
        fn random_bytes_never_panic_or_pass_the_cap(
            input in prop_oneof![prop::collection::vec(any::<u8>(), 0..2048), vocabulary(), framed()],
        ) {
            let limits = Limits { body_bytes: 32, linger: Duration::ZERO, ..Limits::default() };
            let run = run(limits, vec![Step::Send(input)], false);
            for body in &run.bodies {
                prop_assert!(body.len() <= 32, "body of {} bytes", body.len());
            }
        }
    }
}

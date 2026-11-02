use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::Logger;
use crate::rpc::RpcClient;

/// One speed sample covering `span` seconds ending `t` seconds after the
/// poller started. `d`/`u` are the average rates over that span.
struct SpeedSample {
    t: u64,
    span: u64,
    d: f64,
    u: f64,
}

/// Bounded speed history for the whole session, kept at two resolutions:
///
/// - `recent` — the last ~15 minutes at full 1 s granularity, covering the
///   chart's 10-minute window with margin.
/// - `archive` — everything older, at a uniform `hist_span` granularity that
///   doubles whenever the archive exceeds [`ARCHIVE_CAP`]. The beginning of
///   the session always keeps roughly session/600-second resolution instead
///   of collapsing into one giant average.
///
/// `pending` accumulates 1 s samples rolling out of `recent` until they fill
/// one `hist_span` bucket. Total memory stays under ~1500 samples.
pub struct SpeedHistory {
    start: Instant,
    recent: VecDeque<SpeedSample>,
    archive: VecDeque<SpeedSample>,
    pending: Option<SpeedSample>,
    hist_span: u64,
    peak_d: f64,
    peak_u: f64,
}

/// Recent window at full resolution (15 min > the chart's 10 min view).
const RECENT_CAP: usize = 900;
/// Long-term samples; granularity doubles when this fills, so even after a
/// 12 h run the archive still holds ~340 points at ~2 min resolution.
const ARCHIVE_CAP: usize = 600;

/// Merges two chronologically adjacent samples into a span-weighted
/// average stamped with the newer sample's end time.
fn combine(older: SpeedSample, newer: SpeedSample) -> SpeedSample {
    let span = older.span + newer.span;
    SpeedSample {
        t: newer.t,
        d: (older.d * older.span as f64 + newer.d * newer.span as f64) / span as f64,
        u: (older.u * older.span as f64 + newer.u * newer.span as f64) / span as f64,
        span,
    }
}

impl SpeedHistory {
    /// Records one second's rates. Returns the sample's end timestamp and
    /// whether the compacted view changed in a way raw 1 s samples can't
    /// replicate — i.e. the archive emitted a bucket coarser than a second.
    /// While `hist_span` is still 1 the archive holds raw samples identical
    /// to what clients append themselves, so no resend is needed; once it
    /// coarsens, subscribers get a fresh `history` snapshot at each flush
    /// (every `hist_span` seconds) so their local copy stays bounded.
    fn push(&mut self, d: f64, u: f64) -> (u64, bool) {
        let t = self.start.elapsed().as_secs();
        self.peak_d = self.peak_d.max(d);
        self.peak_u = self.peak_u.max(u);
        self.recent.push_back(SpeedSample { t, span: 1, d, u });
        let mut flushed = false;
        if self.recent.len() > RECENT_CAP {
            let aged = self.recent.pop_front().unwrap();
            flushed = self.archive_sample(aged);
        }
        (t, flushed && self.hist_span > 1)
    }

    /// Folds a sample into the pending bucket; returns true when a bucket
    /// was emitted into the archive (or the archive was re-paired).
    fn archive_sample(&mut self, s: SpeedSample) -> bool {
        self.pending = Some(match self.pending.take() {
            Some(p) => combine(p, s),
            None => s,
        });
        if self.pending.as_ref().unwrap().span < self.hist_span {
            return false;
        }
        self.archive.push_back(self.pending.take().unwrap());
        if self.archive.len() > ARCHIVE_CAP {
            self.hist_span *= 2;
            let mut merged = VecDeque::with_capacity(ARCHIVE_CAP / 2 + 1);
            while self.archive.len() >= 2 {
                let a = self.archive.pop_front().unwrap();
                let b = self.archive.pop_front().unwrap();
                merged.push_back(combine(a, b));
            }
            // An odd tail sample is older than `pending` — fold it in so
            // no coverage is lost at the boundary.
            if let Some(last) = self.archive.pop_front() {
                self.pending = Some(match self.pending.take() {
                    Some(p) => combine(last, p),
                    None => last,
                });
            }
            self.archive = merged;
        }
        true
    }

    fn to_json(&self) -> Value {
        json!({
            "elapsed": self.start.elapsed().as_secs(),
            "peak_d": self.peak_d,
            "peak_u": self.peak_u,
            "points": self.archive.iter()
                .chain(self.pending.iter())
                .chain(self.recent.iter())
                .map(|s| json!([s.t, s.d, s.u])).collect::<Vec<_>>(),
        })
    }
}

impl Default for SpeedHistory {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            recent: VecDeque::new(),
            archive: VecDeque::new(),
            pending: None,
            hist_span: 1,
            peak_d: 0.0,
            peak_u: 0.0,
        }
    }
}

/// Shared dashboard state: the latest result of each RPC poll plus the set of
/// SSE client streams subscribed to updates. The poller is the only writer;
/// connection handler threads push newly subscribed streams and replay the
/// snapshots to them.
#[derive(Default)]
pub struct Hub {
    /// Latest `session-stats` arguments — also read at shutdown to log
    /// session totals.
    pub session: Option<Value>,
    torrents: Option<Value>,
    port: Option<Value>,
    speed: SpeedHistory,
    clients: Vec<TcpStream>,
}

impl Hub {
    /// Sends one SSE frame to every subscribed client, dropping any whose
    /// write fails (closed tab, dead socket).
    pub fn broadcast(&mut self, event: &str, data: &Value) {
        let frame = sse_frame(event, data);
        self.clients
            .retain_mut(|s| s.write_all(frame.as_bytes()).is_ok());
    }

    /// Writes the cached snapshot of each event stream so a freshly opened
    /// page renders immediately instead of waiting for the next poll tick.
    /// The speed history goes first so its baseline precedes live updates.
    fn replay(&self, stream: &mut TcpStream) -> io::Result<()> {
        stream.write_all(sse_frame("history", &self.speed.to_json()).as_bytes())?;
        for (event, value) in [
            ("status", &self.session),
            ("torrents", &self.torrents),
            ("port", &self.port),
        ] {
            if let Some(v) = value {
                stream.write_all(sse_frame(event, v).as_bytes())?;
            }
        }
        Ok(())
    }
}

/// A single SSE frame. `Value::to_string` emits single-line JSON, so `data`
/// never needs splitting into multiple `data:` lines.
fn sse_frame(event: &str, data: &Value) -> String {
    format!("event: {}\ndata: {}\n\n", event, data)
}

/// Polls Transmission on the dashboard's schedule and pushes results to all
/// SSE clients: `session-stats` every tick (1s), `torrent-get` every third
/// tick (3s), and `port-test` once when the RPC server first answers.
/// Transmission does not re-test the port during a session, so neither do we.
struct Poller {
    client: Arc<RpcClient>,
    hub: Arc<Mutex<Hub>>,
    baselines: Mutex<Option<HashMap<String, u64>>>,
    port_tested: bool,
    rpc_announced: bool,
    tick: u64,
}

impl Poller {
    fn new(client: Arc<RpcClient>, hub: Arc<Mutex<Hub>>) -> Self {
        Self {
            client,
            hub,
            baselines: Mutex::new(None),
            port_tested: false,
            rpc_announced: false,
            tick: 0,
        }
    }

    fn run(&mut self, logger: &Logger) {
        loop {
            self.tick_once(logger);
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    fn tick_once(&mut self, logger: &Logger) {
        match self.client.session_stats() {
            Ok(args) => {
                if !self.rpc_announced {
                    logger.log("Connected to Transmission RPC");
                    self.rpc_announced = true;
                }
                let mut hub = self.hub.lock().unwrap();
                hub.session = Some(args.clone());
                hub.broadcast("status", &args);
                let d = args["downloadSpeed"].as_f64().unwrap_or(0.0);
                let u = args["uploadSpeed"].as_f64().unwrap_or(0.0);
                let (t, compacted) = hub.speed.push(d, u);
                let (peak_d, peak_u) = (hub.speed.peak_d, hub.speed.peak_u);
                hub.broadcast(
                    "speed",
                    &json!({ "t": t, "d": d, "u": u, "peak_d": peak_d, "peak_u": peak_u }),
                );
                // The archive just merged raw seconds into a coarser bucket;
                // push the new snapshot so clients can drop their raw copies.
                if compacted {
                    let snapshot = hub.speed.to_json();
                    hub.broadcast("history", &snapshot);
                }
            }
            Err(e) => self
                .hub
                .lock()
                .unwrap()
                .broadcast("status", &json!({ "error": e })),
        }

        // The port test fires once, gated on the first successful RPC call —
        // Transmission's RPC server needs a moment to come up after launch,
        // and a test attempted before then only reports a connection error.
        if self.rpc_announced && !self.port_tested {
            self.port_tested = true;
            let mut result = json!({});
            for proto in ["ipv4", "ipv6"] {
                result[proto] = match self.client.port_test(proto) {
                    Ok(true) => json!("open"),
                    Ok(false) => json!("closed"),
                    Err(_) => json!("unknown"),
                };
            }
            let mut hub = self.hub.lock().unwrap();
            hub.port = Some(result.clone());
            hub.broadcast("port", &result);
        }

        if self.tick.is_multiple_of(3) {
            match self.client.torrent_get() {
                Ok(mut args) => {
                    inject_session_upload(&mut args, &self.baselines);
                    let mut hub = self.hub.lock().unwrap();
                    hub.torrents = Some(args.clone());
                    hub.broadcast("torrents", &args);
                }
                Err(e) => self
                    .hub
                    .lock()
                    .unwrap()
                    .broadcast("torrents", &json!({ "error": e })),
            }
        }
        self.tick += 1;
    }
}

/// Spawns the local dashboard HTTP server and the RPC poller on background
/// threads. Bound to localhost only; the page receives every poll result over
/// a server-sent events stream. Returns the dashboard URL, or None if the
/// port could not be bound.
pub fn spawn(
    port: u16,
    client: Arc<RpcClient>,
    hub: Arc<Mutex<Hub>>,
    logger: &Logger,
) -> Option<String> {
    let listener = match TcpListener::bind(("127.0.0.1", port)) {
        Ok(l) => l,
        Err(e) => {
            logger.log(&format!(
                "Dashboard disabled: cannot bind 127.0.0.1:{}: {}",
                port, e
            ));
            return None;
        }
    };

    let hostname = detect_hostname();
    let accept_hub = Arc::clone(&hub);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let hub = Arc::clone(&accept_hub);
                    let hostname = hostname.clone();
                    std::thread::spawn(move || handle_conn(stream, &hub, &hostname));
                }
                // Transient accept errors (e.g. fd exhaustion) would
                // otherwise spin the accept loop.
                Err(_) => std::thread::sleep(Duration::from_millis(50)),
            }
        }
    });

    let poll_logger = logger.clone();
    std::thread::spawn(move || Poller::new(client, hub).run(&poll_logger));

    Some(format!("http://127.0.0.1:{}/", port))
}

pub fn open_in_browser(url: &str, logger: &Logger) {
    let result = if cfg!(target_os = "windows") {
        Command::new("cmd").args(["/C", "start", "", url]).spawn()
    } else if cfg!(target_os = "macos") {
        Command::new("open").arg(url).spawn()
    } else {
        Command::new("xdg-open").arg(url).spawn()
    };
    match result {
        Ok(_) => logger.log(&format!("Dashboard available at {}", url)),
        Err(e) => logger.log(&format!(
            "Could not open browser ({}); dashboard is at {}",
            e, url
        )),
    }
}

fn detect_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

fn handle_conn(mut stream: TcpStream, hub: &Mutex<Hub>, hostname: &str) {
    // Idle keep-alive connections are dropped after this. SSE streams are
    // unaffected — they leave this function as soon as they're registered.
    let _ = stream.set_read_timeout(Some(Duration::from_secs(75)));
    let mut buf: Vec<u8> = Vec::with_capacity(2048);
    loop {
        let Some((method, path, version, headers, _body)) = read_request(&mut stream, &mut buf)
        else {
            return;
        };
        let keep_alive = wants_keep_alive(&version, &headers);

        // Reject requests that don't come from a loopback context: a browser
        // page on another origin can't read the response anyway (no CORS
        // headers), and a DNS-rebinding page must not be able to subscribe
        // to the event stream either.
        if !request_is_local(&headers) {
            write_response(
                &mut stream,
                403,
                "application/json",
                &json!({ "error": "forbidden origin" }).to_string(),
                false,
            );
            return;
        }

        match (method.as_str(), path.as_str()) {
            ("GET", "/") => {
                if !write_response(
                    &mut stream,
                    200,
                    "text/html; charset=utf-8",
                    &INDEX_HTML.replace("[hostname]", hostname),
                    keep_alive,
                ) {
                    return;
                }
                if !keep_alive {
                    return;
                }
            }
            ("GET", "/icon.svg") => {
                if !write_response(&mut stream, 200, "image/svg+xml", ICON_SVG, keep_alive) {
                    return;
                }
                if !keep_alive {
                    return;
                }
            }
            ("GET", "/events") => {
                // The stream now lives in the hub; nothing more to do here.
                subscribe_events(stream, hub);
                return;
            }
            _ => {
                write_response(&mut stream, 404, "text/plain", "not found", false);
                return;
            }
        }
    }
}

/// Upgrades the connection to an SSE stream and hands it to the hub.
/// Returns false (connection dies) when the handshake or snapshot replay
/// fails.
fn subscribe_events(mut stream: TcpStream, hub: &Mutex<Hub>) -> bool {
    const HEADER: &str = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n";
    if stream.write_all(HEADER.as_bytes()).is_err() {
        return false;
    }
    let mut hub = hub.lock().unwrap();
    if hub.replay(&mut stream).is_err() {
        return false;
    }
    hub.clients.push(stream);
    true
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    payload: &str,
    keep_alive: bool,
) -> bool {
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: {}\r\n\r\n",
        status,
        reason(status),
        content_type,
        payload.len(),
        if keep_alive { "keep-alive" } else { "close" }
    );
    stream.write_all(header.as_bytes()).is_ok() && stream.write_all(payload.as_bytes()).is_ok()
}

/// Adds `uploadedSeedingSession` to each seeding torrent: bytes uploaded
/// since the first torrent-get of this process. Transmission only exposes
/// the all-time `uploadedEver`, so on that first call we snapshot it as
/// the baseline — but only for torrents already seeding; torrents that
/// start seeding later never get a baseline and show no badge.
fn inject_session_upload(args: &mut Value, baselines: &Mutex<Option<HashMap<String, u64>>>) {
    let Some(torrents) = args.get_mut("torrents").and_then(|t| t.as_array_mut()) else {
        return;
    };
    let mut guard = baselines.lock().unwrap();
    if guard.is_none() {
        let mut map = HashMap::new();
        for t in torrents.iter() {
            if t["status"].as_u64() == Some(6)
                && let (Some(h), Some(u)) = (t["hashString"].as_str(), t["uploadedEver"].as_u64())
            {
                map.insert(h.to_string(), u);
            }
        }
        *guard = Some(map);
    }
    let map = guard.as_ref().unwrap();
    for t in torrents.iter_mut() {
        if t["status"].as_u64() != Some(6) {
            continue;
        }
        if let (Some(h), Some(u)) = (t["hashString"].as_str(), t["uploadedEver"].as_u64())
            && let Some(base) = map.get(h)
        {
            t["uploadedSeedingSession"] = json!(u.saturating_sub(*base));
        }
    }
}

/// Parses one request out of `buf`, refilling it from the stream as needed.
/// Bytes past the end of the request (e.g. a pipelined next request) are
/// left in `buf` for the following call. Returns (method, path, HTTP
/// version, headers, body), or None when the connection should be closed.
fn read_request(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> Option<(String, String, String, Vec<(String, String)>, Vec<u8>)> {
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > 16 * 1024 {
            return None;
        }
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let head = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = head.lines();
    let mut parts = lines.next()?.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let version = parts.next().unwrap_or("HTTP/1.0").to_string();

    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();

    let content_length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(0);

    let need = header_end + content_length;
    while buf.len() < need {
        let n = stream.read(&mut chunk).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body = buf[header_end..need].to_vec();
    buf.drain(..need);

    Some((method, path, version, headers, body))
}

/// HTTP/1.1 connections stay open unless the client asks to close;
/// HTTP/1.0 closes unless it explicitly asks for keep-alive.
fn wants_keep_alive(version: &str, headers: &[(String, String)]) -> bool {
    let conn = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("connection"))
        .map(|(_, v)| v.to_ascii_lowercase());
    match conn.as_deref() {
        Some(v) if v.contains("close") => false,
        Some(v) if v.contains("keep-alive") => true,
        _ => version == "HTTP/1.1",
    }
}

fn request_is_local(headers: &[(String, String)]) -> bool {
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    };
    match get("host") {
        Some(h) if host_is_loopback(h) => {}
        _ => return false,
    }
    // Browsers always send Origin on cross-site POSTs; it must point back
    // at the dashboard itself. No header at all means curl or a
    // same-origin request, which is fine.
    if let Some(origin) = get("origin") {
        let origin = origin
            .strip_prefix("http://")
            .or_else(|| origin.strip_prefix("https://"))
            .unwrap_or(origin);
        let host = origin.split('/').next().unwrap_or("");
        if !host_is_loopback(host) {
            return false;
        }
    }
    true
}

fn host_is_loopback(host: &str) -> bool {
    let name = if let Some(rest) = host.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        host.split(':').next().unwrap_or("")
    };
    matches!(name.trim_end_matches('.'), "127.0.0.1" | "localhost" | "::1")
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        403 => "Forbidden",
        404 => "Not Found",
        _ => "Internal Server Error",
    }
}

const INDEX_HTML: &str = include_str!("dashboard.html");
const ICON_SVG: &str = include_str!("../icon.svg");

#[cfg(test)]
mod tests {
    use super::*;

    fn read_http_request(stream: &mut TcpStream) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk).ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..end]);
                let len = head
                    .lines()
                    .filter_map(|l| l.split_once(':'))
                    .find(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, v)| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if buf.len() >= end + 4 + len {
                    return Some(buf);
                }
            }
        }
    }

    /// Mock Transmission RPC: replies 409 with a session id until the client
    /// echoes the session id back, then returns success.
    fn mock_rpc_server() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                std::thread::spawn(move || {
                    let mut stream = stream;
                    while let Some(buf) = read_http_request(&mut stream) {
                        let req = String::from_utf8_lossy(&buf);
                        if req.contains("X-Transmission-Session-Id: sid") {
                            let body = r#"{"result":"success","arguments":{"ok":true}}"#;
                            let _ = stream.write_all(
                                format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                                    body.len(),
                                    body
                                )
                                .as_bytes(),
                            );
                        } else {
                            let _ = stream.write_all(
                                b"HTTP/1.1 409 Conflict\r\nX-Transmission-Session-Id: sid\r\nContent-Length: 0\r\n\r\n",
                            );
                        }
                    }
                });
            }
        });
        port
    }

    fn spawn_dashboard() -> (u16, Arc<Mutex<Hub>>) {
        let dir = tempfile::tempdir().unwrap();
        let logger = Logger::new(dir.path().join("log.txt"), false);
        let client = Arc::new(RpcClient::new(mock_rpc_server(), None, None));
        let hub = Arc::new(Mutex::new(Hub::default()));

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let dash_port = listener.local_addr().unwrap().port();
        drop(listener);
        spawn(dash_port, client, hub.clone(), &logger).unwrap();
        (dash_port, hub)
    }

    fn get_with_headers(port: u16, path: &str, headers: &str) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let req = format!(
            "GET {} HTTP/1.1\r\n{}Connection: close\r\n\r\n",
            path, headers
        );
        stream.write_all(req.as_bytes()).unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).unwrap();
        resp
    }

    fn get(port: u16, path: &str) -> String {
        get_with_headers(port, path, "Host: 127.0.0.1\r\n")
    }

    /// Reads one HTTP response (headers plus Content-Length body) without
    /// waiting for the connection to close.
    fn read_http_response(stream: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
            let n = stream.read(&mut chunk).unwrap();
            buf.extend_from_slice(&chunk[..n]);
        };
        let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
        let len = head
            .lines()
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
            .and_then(|(_, v)| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        while buf.len() < header_end + len {
            let n = stream.read(&mut chunk).unwrap();
            buf.extend_from_slice(&chunk[..n]);
        }
        String::from_utf8_lossy(&buf).to_string()
    }

    #[test]
    fn test_dashboard_serves_index() {
        let (dash_port, _hub) = spawn_dashboard();

        let resp = get(dash_port, "/");
        assert!(resp.contains("200 OK"));
        assert!(resp.contains("<title>Transmission Tower</title>"));
        assert!(!resp.contains("[hostname]"));
    }

    #[test]
    fn test_dashboard_serves_icon() {
        let (dash_port, _hub) = spawn_dashboard();

        let resp = get(dash_port, "/icon.svg");
        assert!(resp.contains("200 OK"));
        assert!(resp.contains("Content-Type: image/svg+xml"));
        assert!(resp.contains("<svg"));
    }

    /// Subscribing to /events yields an SSE stream that carries the
    /// poller's first-tick broadcasts (status + torrents + the one-time
    /// port event) plus anything broadcast through the hub later.
    #[test]
    fn test_dashboard_events_stream_receives_poll_events() {
        let (dash_port, hub) = spawn_dashboard();

        let mut stream = TcpStream::connect(("127.0.0.1", dash_port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
            .write_all(b"GET /events HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
            .unwrap();

        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !(String::from_utf8_lossy(&buf).contains("event: port")
            && String::from_utf8_lossy(&buf).contains("event: torrents"))
        {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for poll events; got: {}",
                String::from_utf8_lossy(&buf)
            );
            let n = stream.read(&mut chunk).unwrap();
            buf.extend_from_slice(&chunk[..n]);
        }
        let text = String::from_utf8_lossy(&buf).to_string();
        assert!(text.contains("200 OK"), "{}", text);
        assert!(text.contains("text/event-stream"), "{}", text);
        assert!(text.contains("event: status"), "{}", text);
        assert!(text.contains("event: torrents"), "{}", text);
        assert!(text.contains("event: port"), "{}", text);

        // A broadcast through the hub reaches the subscribed stream.
        hub.lock()
            .unwrap()
            .broadcast("status", &json!({"marker": 42}));
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !String::from_utf8_lossy(&buf).contains("\"marker\":42") {
            assert!(std::time::Instant::now() < deadline, "timed out waiting for broadcast");
            let n = stream.read(&mut chunk).unwrap();
            buf.extend_from_slice(&chunk[..n]);
        }
    }

    #[test]
    fn test_hub_replay_sends_cached_snapshots() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut writer = TcpStream::connect(addr).unwrap();
        let mut reader = listener.accept().unwrap().0;
        reader
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let hub = Hub {
            session: Some(json!({"a": 1})),
            torrents: None,
            port: Some(json!({"p": 2})),
            speed: SpeedHistory::default(),
            clients: Vec::new(),
        };
        hub.replay(&mut writer).unwrap();
        drop(writer);

        let mut buf = String::new();
        reader.read_to_string(&mut buf).unwrap();
        assert!(buf.contains("event: history"), "{}", buf);
        assert!(buf.contains("event: status\ndata: {\"a\":1}"), "{}", buf);
        assert!(buf.contains("event: port\ndata: {\"p\":2}"), "{}", buf);
        assert!(!buf.contains("event: torrents"), "{}", buf);
    }

    /// Pushing past the cap merges the oldest samples into span-weighted
    /// averages while the tail stays at full 1 s resolution — memory stays
    /// bounded but coverage reaches back to session start.
    #[test]
    fn test_speed_history_decimates_oldest_samples() {
        let mut hist = SpeedHistory::default();
        // Artificially age the clock so timestamps are nonzero.
        hist.start = Instant::now() - Duration::from_secs(2000);
        let pushes = RECENT_CAP + ARCHIVE_CAP + 5;
        for _ in 0..pushes {
            hist.push(100.0, 50.0);
        }
        assert_eq!(hist.recent.len(), RECENT_CAP);
        assert!(hist.recent.iter().all(|s| s.span == 1));
        assert!(!hist.archive.is_empty());
        // Archive buckets are all at the current uniform granularity.
        assert!(hist.archive.iter().all(|s| s.span == hist.hist_span));
        // Nothing is dropped: archive + pending + recent cover every push.
        let coverage: u64 = hist
            .archive
            .iter()
            .chain(hist.pending.iter())
            .chain(hist.recent.iter())
            .map(|s| s.span)
            .sum();
        assert_eq!(coverage, pushes as u64);
        // Constant input rates survive averaging exactly.
        assert_eq!(hist.archive[0].d, 100.0);
        assert_eq!(hist.archive[0].u, 50.0);
        assert_eq!(hist.peak_d, 100.0);

        // Serialized points are non-decreasing in time.
        let j = hist.to_json();
        let pts = j["points"].as_array().unwrap();
        assert!(
            pts.windows(2)
                .all(|w| w[0][0].as_u64().unwrap() <= w[1][0].as_u64().unwrap())
        );
    }

    /// Once the archive fills, granularity doubles and the sample count
    /// halves — long sessions stay bounded at uniform resolution.
    #[test]
    fn test_speed_history_archive_granularity_doubles() {
        let mut hist = SpeedHistory::default();
        hist.start = Instant::now() - Duration::from_secs(10_000);
        for _ in 0..RECENT_CAP + ARCHIVE_CAP * 2 {
            hist.push(100.0, 50.0);
        }
        assert!(hist.archive.len() <= ARCHIVE_CAP);
        assert_eq!(hist.hist_span, 2);
        assert!(hist.archive.iter().all(|s| s.span == 2));
    }

    /// The dirty flag that triggers a `history` resend only fires once
    /// buckets are coarser than a second — before that, client-side raw
    /// appends replicate the server's view exactly.
    #[test]
    fn test_speed_history_dirty_only_after_coarsening() {
        let mut hist = SpeedHistory::default();
        // Aged samples flush as 1 s archive buckets — never dirty.
        for _ in 0..RECENT_CAP + 5 {
            assert!(!hist.push(1.0, 1.0).1);
        }
        // Pushing past ARCHIVE_CAP doubles hist_span; some later push must
        // report dirty once flushes emit coarser buckets.
        let mut saw_dirty = false;
        for _ in 0..ARCHIVE_CAP * 2 {
            saw_dirty |= hist.push(1.0, 1.0).1;
        }
        assert!(saw_dirty);
        assert!(hist.hist_span > 1);
    }

    #[test]
    fn test_dashboard_keep_alive_serves_multiple_requests() {
        let (dash_port, _hub) = spawn_dashboard();

        let mut stream = TcpStream::connect(("127.0.0.1", dash_port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // HTTP/1.1 defaults to keep-alive: two requests on one connection.
        for _ in 0..2 {
            stream
                .write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
                .unwrap();
            let resp = read_http_response(&mut stream);
            assert!(resp.contains("200 OK"), "{}", resp);
            assert!(resp.contains("keep-alive"));
        }

        // Connection: close is honored and ends the exchange.
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            .unwrap();
        let resp = read_http_response(&mut stream);
        assert!(resp.contains("200 OK"), "{}", resp);
        let mut rest = String::new();
        stream.read_to_string(&mut rest).unwrap();
    }

    #[test]
    fn test_dashboard_rejects_non_loopback_host_and_origin() {
        let (dash_port, _hub) = spawn_dashboard();

        // DNS-rebinding style request: wrong Host
        let resp = get_with_headers(dash_port, "/", "Host: evil.example\r\n");
        assert!(resp.contains("403"), "{}", resp);

        // Cross-origin page fetching localhost: right Host, wrong Origin
        let resp = get_with_headers(
            dash_port,
            "/events",
            "Host: 127.0.0.1\r\nOrigin: https://evil.example\r\n",
        );
        assert!(resp.contains("403"), "{}", resp);
    }

    /// One tick against the mock RPC pushes status, torrents, and the
    /// one-time port event to every subscribed stream — and stores the
    /// session snapshot for the exit log.
    #[test]
    fn test_poller_tick_broadcasts_poll_results() {
        let dir = tempfile::tempdir().unwrap();
        let logger = Logger::new(dir.path().join("log.txt"), false);
        let client = Arc::new(RpcClient::new(mock_rpc_server(), None, None));
        let hub = Arc::new(Mutex::new(Hub::default()));

        // Socket pair standing in for an SSE subscriber.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client_end = TcpStream::connect(addr).unwrap();
        let mut server_end = listener.accept().unwrap().0;
        server_end
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        hub.lock().unwrap().clients.push(client_end);

        let mut poller = Poller::new(client, hub.clone());
        poller.tick_once(&logger);

        // Read until the port frame (written last this tick) arrives.
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !String::from_utf8_lossy(&buf).contains("event: port") {
            assert!(std::time::Instant::now() < deadline, "timed out");
            let n = server_end.read(&mut chunk).unwrap();
            buf.extend_from_slice(&chunk[..n]);
        }
        let text = String::from_utf8_lossy(&buf).to_string();
        assert!(text.contains("event: status\ndata: {\"ok\":true}"), "{}", text);
        assert!(text.contains("event: speed"), "{}", text);
        assert!(text.contains("event: torrents"), "{}", text);
        assert!(text.contains("event: port"), "{}", text);
        assert!(text.contains("\"ipv4\":\"closed\""), "{}", text);

        assert!(hub.lock().unwrap().session.is_some());
        assert!(poller.port_tested);
    }

    #[test]
    fn test_session_upload_baseline_injection() {
        let baselines = Mutex::new(None);

        // First call: only the seeding torrent gets a baseline; nothing
        // is injected for a still-downloading one.
        let mut first = json!({"torrents": [
            {"id": 1, "status": 6, "hashString": "aaa", "uploadedEver": 1000},
            {"id": 2, "status": 4, "hashString": "bbb", "uploadedEver": 2000}
        ]});
        inject_session_upload(&mut first, &baselines);
        assert_eq!(baselines.lock().unwrap().as_ref().unwrap().len(), 1);
        assert_eq!(
            first["torrents"][0]["uploadedSeedingSession"].as_u64(),
            Some(0)
        );
        assert!(first["torrents"][1].get("uploadedSeedingSession").is_none());

        // Second call: delta injected from the baseline; the now-seeding
        // "bbb" still has no baseline because it wasn't seeding on the
        // process's first call.
        let mut second = json!({"torrents": [
            {"id": 1, "status": 6, "hashString": "aaa", "uploadedEver": 1750},
            {"id": 2, "status": 6, "hashString": "bbb", "uploadedEver": 2200}
        ]});
        inject_session_upload(&mut second, &baselines);
        assert_eq!(
            second["torrents"][0]["uploadedSeedingSession"].as_u64(),
            Some(750)
        );
        assert!(second["torrents"][1].get("uploadedSeedingSession").is_none());
    }
}

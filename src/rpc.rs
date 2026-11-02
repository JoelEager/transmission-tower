use std::sync::Mutex;
use std::time::Duration;

use serde_json::{Value, json};

/// Fields requested on every torrent-get poll; everything the dashboard
/// renders comes from this list.
const TORRENT_FIELDS: [&str; 21] = [
    "id",
    "name",
    "status",
    "percentDone",
    "rateDownload",
    "rateUpload",
    "uploadedEver",
    "downloadedEver",
    "uploadRatio",
    "eta",
    "totalSize",
    "addedDate",
    "pieceCount",
    "pieces",
    "trackerStats",
    "hashString",
    "peers",
    "peersFrom",
    "peersConnected",
    "peersSendingToUs",
    "peersGettingFromUs",
];

pub struct RpcClient {
    agent: ureq::Agent,
    url: String,
    auth_header: Option<String>,
    session_id: Mutex<Option<String>>,
}

impl RpcClient {
    pub fn new(port: u16, username: Option<&str>, password: Option<&str>) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(3))
            .timeout(Duration::from_secs(10))
            .build();
        let auth_header = username.map(|u| {
            format!(
                "Basic {}",
                base64_encode(format!("{}:{}", u, password.unwrap_or("")).as_bytes())
            )
        });
        Self {
            agent,
            url: format!("http://127.0.0.1:{}/transmission/rpc", port),
            auth_header,
            session_id: Mutex::new(None),
        }
    }

    /// Performs a Transmission RPC call and returns the response `arguments` object.
    /// Handles the X-Transmission-Session-Id CSRF handshake transparently.
    pub fn call(&self, method: &str, arguments: Value) -> Result<Value, String> {
        let body = json!({ "method": method, "arguments": arguments });
        match self.post(&body) {
            Ok(resp) => parse_response(resp),
            Err(ureq::Error::Status(409, resp)) => {
                let new_id = resp
                    .header("X-Transmission-Session-Id")
                    .ok_or_else(|| "RPC returned 409 without a session id".to_string())?;
                *self.session_id.lock().unwrap() = Some(new_id.to_string());
                parse_response(self.post(&body).map_err(rpc_err)?)
            }
            Err(e) => Err(rpc_err(e)),
        }
    }

    #[allow(clippy::result_large_err)]
    fn post(&self, body: &Value) -> Result<ureq::Response, ureq::Error> {
        let mut req = self.agent.post(&self.url);
        if let Some(auth) = &self.auth_header {
            req = req.set("Authorization", auth);
        }
        if let Some(id) = self.session_id.lock().unwrap().as_deref() {
            req = req.set("X-Transmission-Session-Id", id);
        }
        req.send_json(body)
    }

    pub fn session_stats(&self) -> Result<Value, String> {
        self.call("session-stats", json!({}))
    }

    pub fn torrent_get(&self) -> Result<Value, String> {
        self.call("torrent-get", json!({ "fields": TORRENT_FIELDS }))
    }

    /// Asks Transmission's port-check service whether the forwarded peer
    /// port is reachable. `ip_protocol` is "ipv4" or "ipv6".
    pub fn port_test(&self, ip_protocol: &str) -> Result<bool, String> {
        let args = self.call("port-test", json!({ "ip_protocol": ip_protocol }))?;
        Ok(args["port-is-open"].as_bool().unwrap_or(false))
    }
}

fn rpc_err(e: ureq::Error) -> String {
    match e {
        ureq::Error::Status(401, _) => {
            "Transmission RPC requires authentication; pass --rpc-username and --rpc-password"
                .to_string()
        }
        other => format!("RPC request failed: {}", other),
    }
}

fn parse_response(resp: ureq::Response) -> Result<Value, String> {
    let parsed: Value = resp
        .into_json()
        .map_err(|e| format!("Invalid RPC response: {}", e))?;
    if parsed["result"].as_str() != Some("success") {
        return Err(format!(
            "RPC error: {}",
            parsed["result"].as_str().unwrap_or("unknown")
        ));
    }
    Ok(parsed["arguments"].clone())
}

fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = (chunk[0] as u32) << 16
            | (*chunk.get(1).unwrap_or(&0) as u32) << 8
            | (*chunk.get(2).unwrap_or(&0) as u32);
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"user:pass"), "dXNlcjpwYXNz");
    }

    /// Reads one full HTTP request (headers plus Content-Length body) from the
    /// stream. Returns None when the connection closes.
    fn read_http_request(stream: &mut std::net::TcpStream) -> Option<Vec<u8>> {
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

    /// Mock RPC server: requests without a session id get a 409 carrying the
    /// session id; requests carrying it get a success response. Handles retries
    /// arriving on the same or a fresh connection.
    #[test]
    fn test_session_id_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests2 = Arc::clone(&requests);

        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let requests = Arc::clone(&requests2);
                std::thread::spawn(move || {
                    let mut stream = stream;
                    while let Some(buf) = read_http_request(&mut stream) {
                        let req = String::from_utf8_lossy(&buf).to_string();
                        requests.lock().unwrap().push(req.clone());
                        if req.contains("X-Transmission-Session-Id: test-id-42") {
                            let body = r#"{"result":"success","arguments":{"uploadSpeed":123}}"#;
                            let _ = stream.write_all(
                                format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                                    body.len(),
                                    body
                                )
                                .as_bytes(),
                            );
                            return;
                        }
                        let _ = stream.write_all(
                            b"HTTP/1.1 409 Conflict\r\nX-Transmission-Session-Id: test-id-42\r\nContent-Length: 0\r\n\r\n",
                        );
                    }
                });
            }
        });

        let client = RpcClient::new(port, None, None);
        let args = client.session_stats().unwrap();
        assert_eq!(args["uploadSpeed"], 123);

        let seen = requests.lock().unwrap();
        assert!(!seen[0].contains("X-Transmission-Session-Id: test-id-42"));
        assert!(
            seen.iter()
                .any(|r| r.contains("X-Transmission-Session-Id: test-id-42"))
        );
    }

    #[test]
    fn test_rpc_error_result() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        std::thread::spawn(move || {
            let mut stream = listener.incoming().next().unwrap().unwrap();
            let _ = read_http_request(&mut stream);
            let body = r#"{"result":"invalid method"}"#;
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                )
                .as_bytes(),
            );
        });

        let client = RpcClient::new(port, None, None);
        let err = client.call("bogus", json!({})).unwrap_err();
        assert!(err.contains("invalid method"));
    }
}

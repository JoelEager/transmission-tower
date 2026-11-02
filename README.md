# Visualization Feature
This branch adds a live visualization dashboard rendered in the browser; vibe coded using [Devin](https://devin.ai/).

While Transmission runs, the tool enables Transmission's built-in RPC server and serves a live dashboard at `http://127.0.0.1:9191/` (opened automatically in your browser). The page updates live over server-sent events and shows:

- Session download/upload totals, live transfer speeds, active time, and torrent counts
- Connected peer counts (total, sending, receiving) plus a deduplicated per-peer list with client name and transfer rates
- Per-torrent piece maps, ETAs, and tracker-reported swarm health
- Whether the forwarded peer port is reachable from the internet

Session totals are written to the log file when Transmission exits.

## How it works

The tool writes `"rpc-enabled": true` into `settings.json` alongside the usual `peer-port` update, then polls a small set of read-only RPC calls itself — `session-stats` once a second, `torrent-get` every three seconds, and `port-test` once at startup — pushing each result to the dashboard over a server-sent events stream. Poll results are also kept internally so the final session totals can be logged after Transmission exits.

Note that `rpc-enabled` persists in `settings.json`, so Transmission's localhost RPC stays enabled on future launches even without this tool.

## Security posture

- Both the Transmission RPC server and the dashboard server bind to `127.0.0.1` only — nothing is reachable from other machines or the internet. The `rpc-whitelist` is also pinned to `127.0.0.1,::1` as defense in depth.
- The dashboard endpoint accepts no RPC requests at all — it only serves the page and its event stream. The server issues just three read-only calls (`session-stats`, `torrent-get`, `port-test`) on its own schedule, so nothing a browser sends can drive Transmission.
- Requests are rejected unless the `Host` header is a loopback name, and any `Origin` header must be loopback too — this prevents other web pages (or a DNS-rebinding attack) from reading the event stream.
- Your existing RPC authentication settings are left untouched. If you've enabled RPC authentication in Transmission, pass credentials via `--rpc-username`/`--rpc-password`.
- `port-test` makes a one-time outbound request to Transmission's official port-check service (the same one the client's "Test Port" button uses) to report whether the forwarded port is open.

Disable the whole feature with `--no-dashboard`; change the dashboard port with `--dashboard-port`.

# Transmission Tower
Session companion for [Transmission](https://github.com/transmission/transmission): applies the VPN forwarded port to `settings.json`, enables a localhost-only RPC server, launches the client, and serves a live dashboard (see below) while it runs. Session totals are appended to a log file for troubleshooting and history.

Designed to run from a VPN's "launch an app after connecting" hook (e.g. Proton VPN's "Connect and Go" profile) — copy the forwarded port to the clipboard and it takes care of the rest. The port can also be passed directly with `--port`.

Mostly vibe coded using [Google Jules](https://jules.google.com/).

## Build Instructions
### Prerequisites
- [Rust & Cargo](https://www.rust-lang.org/tools/install)

### Building Release Binary
```bash
cargo build --release
```

The compiled executable will be available at `target/release/transmission-tower` (or `transmission-tower.exe` on Windows).

## Usage
Simply run the binary:

```bash
./target/release/transmission-tower
```

It should automatically detect the Transmission configuration path and executable for your platform. If not, the defaults can be overridden with command line options. After launching the binary just copy the port number and it should take care of the rest. (Run with `--help` for usage.)

<p align="center">
  <img src="icon.svg" width="96" alt="Transmission Tower icon">
</p>

# Transmission Tower

Session companion for [Transmission](https://github.com/transmission/transmission): applies your VPN's forwarded port to `settings.json`, enables a localhost-only RPC server, launches Transmission, and serves a live dashboard in your browser while it runs.

Designed to run from a VPN's "launch an app after connecting" hook (e.g. Proton VPN's "Connect and Go" profile) — copy the forwarded port to the clipboard and it takes care of the rest. The port can also be passed directly with `--port`.

## Features

- **Port forwarding** — reads the forwarded port from the clipboard (or `--port`) and writes it into `settings.json` before launch
- **Live dashboard** — served at `http://127.0.0.1:9191/` (opened automatically) and updated over server-sent events:
  - Animated swarm of connected peers, plus a deduplicated peer table with client names and rates
  - Upload/download speed graph over time with range toggle and session averages
  - Session totals, live speeds, active time, and torrent/peer counts
  - Per-torrent piece maps, ETAs, and tracker-reported swarm health
  - Whether the forwarded peer port is reachable from the internet
  - History view of per-day transfer totals, with drill-down into individual sessions
- **Session history** — one JSON line per Transmission session appended to `history.jsonl` with start/end times, exit code, runtime, and bytes transferred
- **Logging** — human-readable `transmission-tower.log`, with session totals written when Transmission exits

All state lives in the tower directory — `~/.transmission-tower` by default, override with `--tower-dir`.

## How it works

The tool writes `"rpc-enabled": true` into Transmission's `settings.json` alongside the `peer-port` update, then polls a small set of read-only RPC endpoints (`session-stats` once a second, `torrent-get` every three seconds, and `port-test` once at startup) and pushes each result to the dashboard over an SSE stream. Poll results are kept internally so final session totals can be logged after Transmission exits and new dashboard clients can see past data.

Note that `rpc-enabled` persists in `settings.json`, so Transmission's localhost RPC stays enabled on future launches even without this tool.

## Security

- Both the Transmission RPC server and the dashboard bind to `127.0.0.1` only — nothing is reachable from other machines or the internet. `rpc-whitelist` is pinned to `127.0.0.1,::1` as defense in depth.
- Requests are rejected unless the `Host` header is a loopback name, and any `Origin` header must be loopback too — preventing other web pages (or a DNS-rebinding attack) from reading the event stream.
- Existing RPC authentication settings are left untouched. If you've enabled RPC authentication in Transmission, pass credentials via `--rpc-username`/`--rpc-password`.
- `port-test` makes a one-time outbound request to Transmission's official port-check service (the same one Transmission's "Test Port" button uses).

Disable the dashboard with `--no-dashboard`; change its port with `--dashboard-port`; keep it running but skip auto-opening it with `--no-browser`.

## Build

Prerequisite: [Rust & Cargo](https://www.rust-lang.org/tools/install)

```bash
cargo build --release
```

The binary is at `target/release/transmission-tower` (`transmission-tower.exe` on Windows).

## Usage

```bash
transmission-tower
```

It auto-detects the Transmission config path and executable for your platform (overridable with `--transmission-config-dir`/`--transmission-path`). After launch, copy the forwarded port to the clipboard and it handles the rest (it waits up to 30s, configurable with `--port-timeout`). Transmission is launched minimized by default — override with `--transmission-args`.

Every option also lives in `config.json` in the data directory, created on first start with each option's default — values set there apply on future runs, and CLI flags override the file per-run. Run `transmission-tower --help` for all options.

---

Mostly vibe coded using [Google Jules](https://jules.google.com/) and [Devin](https://devin.ai/).

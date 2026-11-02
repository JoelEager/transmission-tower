#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

mod dashboard;
mod rpc;

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::sync::{Arc, Mutex};

use arboard::Clipboard;
use chrono::{DateTime, Local};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Transmission session companion: applies the forwarded port, launches the client, and serves a live dashboard."
)]
pub struct Args {
    /// Directory containing the `transmission/settings.json` file.
    /// Defaults to %LOCALAPPDATA% on Windows, or ~/.config on Linux/macOS.
    #[arg(long)]
    pub config_dir: Option<PathBuf>,

    /// Path to the Transmission executable.
    /// Defaults to `C:\Program Files\Transmission\transmission-qt.exe` on Windows, or `transmission-qt` on Linux/macOS.
    #[arg(long)]
    pub transmission_path: Option<PathBuf>,

    /// Directory holding the config file, session history, and log file.
    /// Defaults to `~/.transmission-tower`.
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// Override port value directly instead of reading from clipboard.
    #[arg(long)]
    pub port: Option<u16>,

    /// Skip launching Transmission (useful for dry runs and testing).
    #[arg(long)]
    pub skip_launch: bool,

    /// Output log messages to standard output in addition to the log file.
    #[arg(short, long)]
    pub verbose: bool,

    /// Disable the local session dashboard shown in the browser.
    #[arg(long)]
    pub no_dashboard: bool,

    /// Localhost port for the session dashboard server. Defaults to 9191.
    #[arg(long)]
    pub dashboard_port: Option<u16>,

    /// Username for Transmission RPC authentication (only needed if authentication is enabled in Transmission).
    #[arg(long)]
    pub rpc_username: Option<String>,

    /// Password for Transmission RPC authentication (only needed if authentication is enabled in Transmission).
    #[arg(long)]
    pub rpc_password: Option<String>,
}

#[derive(Clone)]
pub struct Logger {
    log_path: PathBuf,
    verbose: bool,
}

impl Logger {
    pub fn new(log_path: PathBuf, verbose: bool) -> Self {
        Self { log_path, verbose }
    }

    pub fn writeln_file(&self, msg: &str) {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
        {
            let _ = writeln!(file, "{}", msg);
        }
    }

    pub fn log(&self, msg: &str) {
        if self.verbose {
            println!("{}", msg);
        }
        self.writeln_file(msg);
    }
}

fn default_data_dir() -> Result<PathBuf, String> {
    dirs::home_dir()
        .map(|home| home.join(".transmission-tower"))
        .ok_or_else(|| "Could not determine user home directory".to_string())
}

/// Values read from `<data-dir>/config.json`. Every field is optional:
/// absent or null falls back to the built-in default. Keys not listed
/// here are collected in `extra` so they can be called out in the log
/// rather than silently ignored (a typo'd key would otherwise look like
/// a working setting).
#[derive(Debug, Default, Deserialize)]
struct FileConfig {
    config_dir: Option<PathBuf>,
    transmission_path: Option<PathBuf>,
    port: Option<u16>,
    skip_launch: Option<bool>,
    verbose: Option<bool>,
    no_dashboard: Option<bool>,
    dashboard_port: Option<u16>,
    rpc_username: Option<String>,
    rpc_password: Option<String>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

/// Effective settings after merging CLI arguments over the config file.
/// Anything the CLI explicitly supplies wins; everything else comes from
/// the config file, falling back to the built-in defaults.
struct Settings {
    config_dir: PathBuf,
    transmission_path: PathBuf,
    port: Option<u16>,
    skip_launch: bool,
    verbose: bool,
    no_dashboard: bool,
    dashboard_port: u16,
    rpc_username: Option<String>,
    rpc_password: Option<String>,
}

fn resolve_settings(args: &Args, cfg: &FileConfig) -> Result<Settings, String> {
    Ok(Settings {
        config_dir: match args.config_dir.clone().or_else(|| cfg.config_dir.clone()) {
            Some(dir) => dir,
            None => default_config_dir()?,
        },
        transmission_path: args
            .transmission_path
            .clone()
            .or_else(|| cfg.transmission_path.clone())
            .unwrap_or_else(default_transmission_path),
        port: args.port.or(cfg.port),
        skip_launch: args.skip_launch || cfg.skip_launch.unwrap_or(false),
        verbose: args.verbose || cfg.verbose.unwrap_or(false),
        no_dashboard: args.no_dashboard || cfg.no_dashboard.unwrap_or(false),
        dashboard_port: args.dashboard_port.or(cfg.dashboard_port).unwrap_or(9191),
        rpc_username: args.rpc_username.clone().or(cfg.rpc_username.clone()),
        rpc_password: args.rpc_password.clone().or(cfg.rpc_password.clone()),
    })
}

/// Writes the initial config file populated with the built-in defaults.
/// `port` stays null to mean "read the forwarded port from the clipboard".
fn write_default_config(path: &Path) -> Result<(), String> {
    let defaults = json!({
        "config_dir": default_config_dir().ok(),
        "transmission_path": default_transmission_path(),
        "port": null,
        "skip_launch": false,
        "verbose": false,
        "no_dashboard": false,
        "dashboard_port": 9191,
        "rpc_username": null,
        "rpc_password": null
    });
    let mut body = serde_json::to_string_pretty(&defaults)
        .map_err(|e| format!("Failed to serialize default config: {}", e))?;
    body.push('\n');
    fs::write(path, body).map_err(|e| format!("Failed to write {}: {}", path.display(), e))
}

fn default_config_dir() -> Result<PathBuf, String> {
    if cfg!(target_os = "windows") {
        std::env::var("LOCALAPPDATA")
            .map(PathBuf::from)
            .map_err(|_| "LOCALAPPDATA environment variable is not set".to_string())
    } else {
        dirs::config_dir().ok_or_else(|| "Could not determine user config directory".to_string())
    }
}

fn default_transmission_path() -> PathBuf {
    if cfg!(target_os = "windows") {
        PathBuf::from(r"C:\Program Files\Transmission\transmission-qt.exe")
    } else {
        PathBuf::from("transmission-qt")
    }
}

fn get_port_from_clipboard() -> Result<u16, String> {
    let mut clipboard =
        Clipboard::new().map_err(|e| format!("Failed to access clipboard: {}", e))?;
    let content = clipboard
        .get_text()
        .map_err(|e| format!("Failed to get clipboard text: {}", e))?;

    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Err("Clipboard is empty".to_string());
    }

    // Never echo the clipboard contents into the log — it usually holds
    // something unrelated when this check fails.
    let port: u16 = trimmed
        .parse()
        .map_err(|_| "Clipboard content is not a valid port number".to_string())?;

    if port == 0 {
        return Err("Port number cannot be 0".to_string());
    }

    Ok(port)
}

fn clear_clipboard(logger: &Logger) {
    match Clipboard::new() {
        Ok(mut clipboard) => {
            if clipboard.clear().is_err() {
                // If clear fails, try setting empty text as fallback
                let _ = clipboard.set_text("");
            }
        }
        Err(e) => {
            logger.log(&format!("Failed to access clipboard to reset: {}", e));
        }
    }
}

fn poll_port_from_clipboard_with_interval(
    logger: &Logger,
    total_attempts: u32,
    sleep_duration: std::time::Duration,
) -> Result<u16, String> {
    logger.log("Copy VPN forwarding port to continue...");

    let mut last_err = String::new();
    for _ in 0..total_attempts {
        std::thread::sleep(sleep_duration);
        match get_port_from_clipboard() {
            Ok(port) => {
                clear_clipboard(logger);
                return Ok(port);
            }
            Err(err_msg) => last_err = err_msg,
        }
    }

    let failure_msg = if last_err.is_empty() {
        "Failed to find a valid port on clipboard after 30 seconds.".to_string()
    } else {
        format!(
            "Failed to find a valid port on clipboard after 30 seconds: {}",
            last_err
        )
    };
    logger.log(&failure_msg);
    Err(failure_msg)
}

fn poll_port_from_clipboard(logger: &Logger) -> Result<u16, String> {
    poll_port_from_clipboard_with_interval(logger, 6, std::time::Duration::from_secs(5))
}

/// Updates `settings.json` with the new peer port and returns the configured
/// RPC port. When `enable_rpc` is true, also enables the RPC server restricted
/// to localhost — Transmission ignores all other remote-access settings when
/// binding to a loopback address.
fn update_settings_file(
    config_dir: &Path,
    port: u16,
    enable_rpc: bool,
    logger: &Logger,
) -> Result<u16, String> {
    let settings_path = config_dir.join("transmission").join("settings.json");
    let settings_path_str = settings_path.to_string_lossy();
    logger.log(&format!("Opening {}", settings_path_str));

    if !settings_path.exists() {
        return Err(format!(
            "Settings file does not exist: {}",
            settings_path_str
        ));
    }

    let content = fs::read_to_string(&settings_path)
        .map_err(|e| format!("Failed to read {}: {}", settings_path_str, e))?;

    let mut settings: Value = serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse JSON from {}: {}", settings_path_str, e))?;

    if !settings.is_object() {
        return Err(format!(
            "Settings file JSON root is not an object: {}",
            settings_path_str
        ));
    }

    let rpc_port = settings["rpc-port"].as_u64().unwrap_or(9091) as u16;

    settings["peer-port"] = Value::from(port);

    if enable_rpc {
        settings["rpc-enabled"] = Value::from(true);
        settings["rpc-bind-address"] = Value::from("127.0.0.1");
        settings["rpc-whitelist-enabled"] = Value::from(true);
        settings["rpc-whitelist"] = Value::from("127.0.0.1,::1");
        logger.log("Enabled Transmission RPC bound to localhost only (127.0.0.1)");
    }

    // Format JSON with 4 spaces indentation
    let buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(b"    ");
    let mut ser = serde_json::Serializer::with_formatter(buf, formatter);
    settings
        .serialize(&mut ser)
        .map_err(|e| format!("Failed to serialize settings JSON: {}", e))?;

    let mut updated_json = String::from_utf8(ser.into_inner())
        .map_err(|e| format!("Failed to convert serialized JSON to string: {}", e))?;
    updated_json.push('\n');

    fs::write(&settings_path, updated_json).map_err(|e| {
        format!(
            "Failed to write updated settings to {}: {}",
            settings_path_str, e
        )
    })?;

    logger.log(&format!("Successfully updated port to {}", port));
    Ok(rpc_port)
}

fn run_app_logic(
    settings: &Settings,
    history_path: &Path,
    logger: &Logger,
) -> Result<(), String> {
    let port = match settings.port {
        Some(p) => {
            if p == 0 {
                return Err("Port number cannot be 0".to_string());
            }
            p
        }
        None => {
            let p = poll_port_from_clipboard(logger)?;
            logger.log(&format!("Using port {} from clipboard", p));
            p
        }
    };

    let dashboard_enabled = !settings.no_dashboard && !settings.skip_launch;
    let rpc_port = update_settings_file(&settings.config_dir, port, dashboard_enabled, logger)?;

    if settings.skip_launch {
        logger.log("Transmission launch skipped per --skip-launch flag");
        return Ok(());
    }

    let trans_path = settings.transmission_path.clone();
    let started_at = Local::now();
    logger.log(&format!(
        "Starting Transmission at {} ({})",
        started_at.format("%Y-%m-%d %H:%M:%S"),
        trans_path.display()
    ));

    let hub: Arc<Mutex<dashboard::Hub>> = Arc::new(Mutex::new(dashboard::Hub::default()));
    if dashboard_enabled {
        let client = Arc::new(rpc::RpcClient::new(
            rpc_port,
            settings.rpc_username.as_deref(),
            settings.rpc_password.as_deref(),
        ));
        if let Some(url) = dashboard::spawn(
            settings.dashboard_port,
            client,
            hub.clone(),
            history_path,
            logger,
        )
        {
            dashboard::open_in_browser(&url, logger);
        }
    }

    let result = run_transmission(&trans_path, &hub, logger);
    if let Ok(status) = &result {
        append_history(history_path, started_at, Local::now(), *status, &hub, logger);
    }
    // Tell any connected dashboards the host is going away. write_all lands
    // the frame in the kernel buffer, so it is delivered before the FIN when
    // process exit closes the sockets.
    let event = match &result {
        Ok(status) => json!({ "reason": "transmission exited", "code": status.code() }),
        Err(e) => json!({ "reason": e }),
    };
    hub.lock().unwrap().broadcast("exit", &event);
    result.map(|_| ())
}

/// Appends one machine-readable JSON line per Transmission session to
/// `history.jsonl`. Byte counts and runtime come from the last
/// session-stats poll; they are null when RPC was never reachable.
fn append_history(
    path: &Path,
    started_at: DateTime<Local>,
    exited_at: DateTime<Local>,
    exit_status: std::process::ExitStatus,
    hub: &Mutex<dashboard::Hub>,
    logger: &Logger,
) {
    let hub = hub.lock().unwrap();
    let current = hub.session.as_ref().and_then(|s| s.get("current-stats"));
    let stat = |key: &str| current.and_then(|c| c[key].as_u64());
    let record = json!({
        "started_at": started_at.format("%Y-%m-%d %H:%M:%S").to_string(),
        "exited_at": exited_at.format("%Y-%m-%d %H:%M:%S").to_string(),
        "exit_code": exit_status.code(),
        "runtime_seconds": stat("secondsActive"),
        "downloaded_bytes": stat("downloadedBytes"),
        "uploaded_bytes": stat("uploadedBytes"),
    });
    drop(hub);

    match OpenOptions::new().create(true).append(true).open(path) {
        Ok(mut file) => {
            if let Err(e) = writeln!(file, "{}", record) {
                logger.log(&format!(
                    "Failed to write session history to {}: {}",
                    path.display(),
                    e
                ));
            }
        }
        Err(e) => logger.log(&format!(
            "Failed to open session history {}: {}",
            path.display(),
            e
        )),
    }
}

fn run_transmission(
    trans_path: &Path,
    hub: &Mutex<dashboard::Hub>,
    logger: &Logger,
) -> Result<std::process::ExitStatus, String> {
    let mut child = Command::new(trans_path).spawn().map_err(|e| {
        format!(
            "Failed to launch Transmission at '{}': {}",
            trans_path.display(),
            e
        )
    })?;

    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(e) => {
                logger.log(&format!("Error waiting for Transmission: {}", e));
                return Err(format!("Error waiting for Transmission: {}", e));
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(5));
    };

    let finish_time = Local::now().format("%Y-%m-%d %H:%M:%S");
    logger.log(&format!(
        "Transmission exited at {} with {}",
        finish_time, status
    ));
    log_session_totals(logger, hub);

    Ok(status)
}

fn log_session_totals(logger: &Logger, hub: &Mutex<dashboard::Hub>) {
    let hub = hub.lock().unwrap();
    match hub.session.as_ref().and_then(|s| s.get("current-stats")) {
        Some(current) => {
            let downloaded = current["downloadedBytes"].as_f64().unwrap_or(0.0);
            let uploaded = current["uploadedBytes"].as_f64().unwrap_or(0.0);
            let seconds = current["secondsActive"].as_f64().unwrap_or(0.0);
            logger.log(&format!(
                "Session totals: downloaded {}, uploaded {} over {}",
                format_bytes(downloaded),
                format_bytes(uploaded),
                format_duration(seconds)
            ));
        }
        None => {
            logger.log("Session totals unavailable (Transmission RPC was not reachable)");
        }
    }
}

fn format_bytes(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{:.1} {}", value, UNITS[unit])
}

fn format_duration(seconds: f64) -> String {
    let secs = seconds as u64;
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    if hours > 0 {
        format!("{}h {}m {}s", hours, mins, secs % 60)
    } else {
        format!("{}m {}s", mins, secs % 60)
    }
}

fn run_app(args: Args) -> Result<(), String> {
    let data_dir = match &args.data_dir {
        Some(dir) => dir.clone(),
        None => default_data_dir()?,
    };
    fs::create_dir_all(&data_dir).map_err(|e| {
        format!(
            "Failed to create data directory {}: {}",
            data_dir.display(),
            e
        )
    })?;

    let config_path = data_dir.join("config.json");
    let history_path = data_dir.join("history.jsonl");
    let log_path = data_dir.join("transmission-tower.log");

    // Messages buffered until the logger exists (the verbose setting can
    // itself come from the config file being read here).
    let mut pending_logs: Vec<String> = Vec::new();
    if !config_path.exists() {
        match write_default_config(&config_path) {
            Ok(()) => pending_logs.push(format!("Created default config at {}", config_path.display())),
            Err(e) => pending_logs.push(format!("WARNING: {}", e)),
        }
    }
    let cfg: FileConfig = match fs::read_to_string(&config_path) {
        Ok(content) => match serde_json::from_str(&content) {
            Ok(cfg) => cfg,
            Err(e) => {
                pending_logs.push(format!(
                    "WARNING: could not parse {}, falling back to defaults: {}",
                    config_path.display(),
                    e
                ));
                FileConfig::default()
            }
        },
        // Unreadable after the create attempt above: defaults it is.
        Err(e) => {
            pending_logs.push(format!(
                "WARNING: could not read {}, falling back to defaults: {}",
                config_path.display(),
                e
            ));
            FileConfig::default()
        }
    };

    let settings = resolve_settings(&args, &cfg)?;
    #[cfg(target_os = "windows")]
    if settings.verbose {
        setup_verbose_console();
    }

    let logger = Logger::new(log_path, settings.verbose);
    logger.writeln_file("=== transmission-tower invoked ===");
    for msg in pending_logs {
        logger.log(&msg);
    }
    for key in cfg.extra.keys() {
        logger.log(&format!(
            "WARNING: ignoring unknown config key '{}' in {}",
            key,
            config_path.display()
        ));
    }

    if let Err(err) = run_app_logic(&settings, &history_path, &logger) {
        logger.log(&format!("ERROR: {}", err));
        return Err(err);
    }

    Ok(())
}

#[cfg(target_os = "windows")]
fn setup_verbose_console() {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn AttachConsole(dwProcessId: u32) -> i32;
        fn AllocConsole() -> i32;
    }
    const ATTACH_PARENT_PROCESS: u32 = 0xFFFF_FFFF;
    unsafe {
        if AttachConsole(ATTACH_PARENT_PROCESS) == 0 {
            AllocConsole();
        }
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    if let Err(err) = run_app(args) {
        eprintln!("Error: {}", err);
        if io::stdin().is_terminal() {
            eprintln!("Press Enter to exit...");
            let mut buffer = [0u8; 1];
            let _ = io::stdin().read(&mut buffer);
        }
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Serializes tests that mutate the shared system clipboard.
    static CLIPBOARD_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_update_settings_file() {
        let dir = tempdir().unwrap();
        let trans_dir = dir.path().join("transmission");
        fs::create_dir_all(&trans_dir).unwrap();
        let settings_file = trans_dir.join("settings.json");

        let initial_json = r#"{
    "peer-port": 50000,
    "download-dir": "/downloads"
}"#;
        fs::write(&settings_file, initial_json).unwrap();

        let log_file = dir.path().join("log.txt");
        let logger = Logger::new(log_file.clone(), false);

        let rpc_port = update_settings_file(dir.path(), 51413, false, &logger).unwrap();
        assert_eq!(rpc_port, 9091);

        let updated_content = fs::read_to_string(&settings_file).unwrap();
        let parsed: Value = serde_json::from_str(&updated_content).unwrap();

        assert_eq!(parsed["peer-port"], 51413);
        assert_eq!(parsed["download-dir"], "/downloads");
        assert!(parsed.get("rpc-enabled").is_none());

        let log_content = fs::read_to_string(&log_file).unwrap();
        assert!(log_content.contains("Successfully updated port to 51413"));
    }

    #[test]
    fn test_update_settings_file_enables_rpc_localhost_only() {
        let dir = tempdir().unwrap();
        let trans_dir = dir.path().join("transmission");
        fs::create_dir_all(&trans_dir).unwrap();
        let settings_file = trans_dir.join("settings.json");

        fs::write(&settings_file, r#"{"peer-port": 50000, "rpc-port": 8888}"#).unwrap();

        let log_file = dir.path().join("log.txt");
        let logger = Logger::new(log_file.clone(), false);

        let rpc_port = update_settings_file(dir.path(), 51413, true, &logger).unwrap();
        assert_eq!(rpc_port, 8888);

        let parsed: Value =
            serde_json::from_str(&fs::read_to_string(&settings_file).unwrap()).unwrap();
        assert_eq!(parsed["rpc-enabled"], true);
        assert_eq!(parsed["rpc-bind-address"], "127.0.0.1");
        assert_eq!(parsed["rpc-whitelist-enabled"], true);
        assert_eq!(parsed["rpc-whitelist"], "127.0.0.1,::1");
    }

    fn test_args(
        config_dir: &Path,
        data_dir: &Path,
        port: Option<u16>,
        skip_launch: bool,
    ) -> Args {
        Args {
            config_dir: Some(config_dir.to_path_buf()),
            transmission_path: None,
            data_dir: Some(data_dir.to_path_buf()),
            port,
            skip_launch,
            verbose: false,
            no_dashboard: false,
            dashboard_port: None,
            rpc_username: None,
            rpc_password: None,
        }
    }

    #[test]
    fn test_default_data_dir() {
        let home = dirs::home_dir().expect("Home dir should be present in test env");
        let expected = home.join(".transmission-tower");
        assert_eq!(default_data_dir().unwrap(), expected);
    }

    #[test]
    fn test_run_app_with_port_override_and_skip_launch() {
        let dir = tempdir().unwrap();
        let data_dir = tempdir().unwrap();
        let trans_dir = dir.path().join("transmission");
        fs::create_dir_all(&trans_dir).unwrap();
        let settings_file = trans_dir.join("settings.json");

        fs::write(&settings_file, r#"{"peer-port": 12345}"#).unwrap();

        let args = test_args(dir.path(), data_dir.path(), Some(60000), true);
        assert!(run_app(args).is_ok());

        let updated_content = fs::read_to_string(&settings_file).unwrap();
        let parsed: Value = serde_json::from_str(&updated_content).unwrap();
        assert_eq!(parsed["peer-port"], 60000);

        let log_content =
            fs::read_to_string(data_dir.path().join("transmission-tower.log")).unwrap();
        assert!(log_content.contains("Successfully updated port to 60000"));
        assert!(log_content.contains("Transmission launch skipped"));
    }

    /// First start writes config.json populated with the defaults; on the
    /// next run its values apply, and CLI arguments override them.
    #[test]
    fn test_config_created_then_merged_with_cli_overrides() {
        let dir = tempdir().unwrap();
        let data_dir = tempdir().unwrap();
        let trans_dir = dir.path().join("transmission");
        fs::create_dir_all(&trans_dir).unwrap();
        fs::write(trans_dir.join("settings.json"), r#"{"peer-port": 1}"#).unwrap();

        let args = test_args(dir.path(), data_dir.path(), Some(60000), true);
        assert!(run_app(args).is_ok());

        let config_path = data_dir.path().join("config.json");
        let cfg_text = fs::read_to_string(&config_path).unwrap();
        let cfg_json: Value = serde_json::from_str(&cfg_text).unwrap();
        assert_eq!(cfg_json["dashboard_port"], 9191);
        assert_eq!(cfg_json["verbose"], false);
        assert!(cfg_json["port"].is_null());
        assert!(cfg_json["transmission_path"].is_string());

        // A config-provided value is used when the CLI doesn't supply it…
        fs::write(&config_path, cfg_text.replace("9191", "4242")).unwrap();
        let cfg: FileConfig =
            serde_json::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
        let args = test_args(dir.path(), data_dir.path(), Some(60000), true);
        let settings = resolve_settings(&args, &cfg).unwrap();
        assert_eq!(settings.dashboard_port, 4242);
        assert_eq!(settings.port, Some(60000));

        // …and the CLI wins when both are present.
        let mut args = test_args(dir.path(), data_dir.path(), Some(60000), true);
        args.dashboard_port = Some(1111);
        let settings = resolve_settings(&args, &cfg).unwrap();
        assert_eq!(settings.dashboard_port, 1111);
    }

    #[test]
    fn test_run_app_missing_settings_file() {
        let dir = tempdir().unwrap();
        let data_dir = tempdir().unwrap();

        let args = test_args(dir.path(), data_dir.path(), Some(60000), true);
        let result = run_app(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Settings file does not exist"));

        let log_content =
            fs::read_to_string(data_dir.path().join("transmission-tower.log")).unwrap();
        assert!(log_content.contains("ERROR: Settings file does not exist"));
    }

    #[test]
    fn test_port_zero_validation() {
        let dir = tempdir().unwrap();
        let data_dir = tempdir().unwrap();

        let args = test_args(dir.path(), data_dir.path(), Some(0), true);
        let result = run_app(args);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Port number cannot be 0");

        let log_content =
            fs::read_to_string(data_dir.path().join("transmission-tower.log")).unwrap();
        assert!(log_content.contains("ERROR: Port number cannot be 0"));
    }

    #[test]
    fn test_poll_port_from_clipboard_timeout() {
        let _guard = CLIPBOARD_LOCK.lock().unwrap();
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("log.txt");
        let logger = Logger::new(log_file.clone(), false);

        // Ensure clipboard is empty or non-numeric for timeout test
        if let Ok(mut cb) = Clipboard::new() {
            let _ = cb.set_text("not_a_port");
        }

        // Test with short duration and 2 attempts for fast execution
        let result = poll_port_from_clipboard_with_interval(
            &logger,
            2,
            std::time::Duration::from_millis(10),
        );
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Failed to find a valid port on clipboard after 30 seconds: Clipboard content is not a valid port number"
        );

        let log_content = fs::read_to_string(&log_file).unwrap();
        assert!(log_content.contains("Copy VPN forwarding port to continue"));
        assert!(log_content.contains("Failed to find a valid port on clipboard after 30 seconds"));
        // The failure line carries the reason but not the clipboard value.
        assert!(log_content.contains("Clipboard content is not a valid port number"));
        assert!(!log_content.contains("not_a_port"));
        // Polling attempts are no longer logged one per line.
        assert!(!log_content.contains("Attempt"));
    }

    #[test]
    fn test_poll_port_from_clipboard_success_and_clear() {
        let _guard = CLIPBOARD_LOCK.lock().unwrap();
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("log.txt");
        let logger = Logger::new(log_file.clone(), false);

        if let Ok(mut cb) = Clipboard::new() {
            let _ = cb.set_text("54321");
            let result = poll_port_from_clipboard_with_interval(
                &logger,
                2,
                std::time::Duration::from_millis(10),
            );
            assert_eq!(result.unwrap(), 54321);

            // Verify clipboard was reset
            let current_cb_text = cb.get_text().unwrap_or_default();
            assert!(current_cb_text.trim().is_empty());
        }
    }

    #[test]
    fn test_args_verbose_flag_parsing() {
        let args_short = Args::parse_from(["transmission-tower", "-v"]);
        assert!(args_short.verbose);

        let args_long = Args::parse_from(["transmission-tower", "--verbose"]);
        assert!(args_long.verbose);

        let args_default = Args::parse_from(["transmission-tower"]);
        assert!(!args_default.verbose);
    }

    #[test]
    fn test_logger_file_only_when_not_verbose() {
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("log.txt");
        let logger = Logger::new(log_file.clone(), false);

        logger.log("Hello log file");

        let log_content = fs::read_to_string(&log_file).unwrap();
        assert!(log_content.contains("Hello log file"));
    }
}

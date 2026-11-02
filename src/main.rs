#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

mod dashboard;
mod rpc;

use std::fs::{self, OpenOptions};
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::sync::{Arc, Mutex};

use arboard::Clipboard;
use chrono::Local;
use clap::Parser;
use serde::Serialize;
use serde_json::Value;

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

    /// Path to the log file. Defaults to `~/.transmission-tower.log`.
    #[arg(long)]
    pub log_file: Option<PathBuf>,

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

    /// Localhost port for the session dashboard server.
    #[arg(long, default_value_t = 9191)]
    pub dashboard_port: u16,

    /// Username for Transmission RPC authentication (only needed if authentication is enabled in Transmission).
    #[arg(long)]
    pub rpc_username: Option<String>,

    /// Password for Transmission RPC authentication (only needed if authentication is enabled in Transmission).
    #[arg(long)]
    pub rpc_password: Option<String>,
}

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

fn default_log_file() -> Result<PathBuf, String> {
    dirs::home_dir()
        .map(|home| home.join(".transmission-tower.log"))
        .ok_or_else(|| "Could not determine user home directory".to_string())
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

    let port: u16 = trimmed
        .parse()
        .map_err(|_| format!("Invalid port number in clipboard: '{}'", trimmed))?;

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
            logger.log("Clipboard reset to empty.");
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

    for attempt in 1..=total_attempts {
        std::thread::sleep(sleep_duration);

        match get_port_from_clipboard() {
            Ok(port) => {
                logger.log(&format!(
                    "Attempt {}/{}: Found valid port {} on clipboard",
                    attempt, total_attempts, port
                ));
                clear_clipboard(logger);
                return Ok(port);
            }
            Err(err_msg) => {
                logger.log(&format!(
                    "Attempt {}/{}: {}",
                    attempt, total_attempts, err_msg
                ));
            }
        }
    }

    let failure_msg = "Failed to find a valid port on clipboard after 30 seconds.".to_string();
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

fn run_app_logic(args: &Args, logger: &Logger) -> Result<(), String> {
    let config_dir = match &args.config_dir {
        Some(dir) => dir.clone(),
        None => default_config_dir()?,
    };

    let port = match args.port {
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

    let dashboard_enabled = !args.no_dashboard && !args.skip_launch;
    let rpc_port = update_settings_file(&config_dir, port, dashboard_enabled, logger)?;

    if args.skip_launch {
        logger.log("Transmission launch skipped per --skip-launch flag");
        return Ok(());
    }

    let trans_path = args
        .transmission_path
        .clone()
        .unwrap_or_else(default_transmission_path);
    let start_time = Local::now().format("%Y-%m-%d %H:%M:%S");
    logger.log(&format!(
        "Starting Transmission at {} ({})",
        start_time,
        trans_path.display()
    ));

    let stats_cache: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let rpc_client = if dashboard_enabled {
        let client = Arc::new(rpc::RpcClient::new(
            rpc_port,
            args.rpc_username.as_deref(),
            args.rpc_password.as_deref(),
        ));
        match dashboard::spawn(
            args.dashboard_port,
            client.clone(),
            stats_cache.clone(),
            logger,
        ) {
            Some(url) => {
                dashboard::open_in_browser(&url, logger);
                Some(client)
            }
            None => None,
        }
    } else {
        None
    };

    run_transmission(&trans_path, rpc_client.as_deref(), &stats_cache, logger)
}

fn run_transmission(
    trans_path: &Path,
    rpc_client: Option<&rpc::RpcClient>,
    stats_cache: &Mutex<Option<Value>>,
    logger: &Logger,
) -> Result<(), String> {
    let mut child = Command::new(trans_path).spawn().map_err(|e| {
        format!(
            "Failed to launch Transmission at '{}': {}",
            trans_path.display(),
            e
        )
    })?;

    let mut rpc_announced = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(e) => {
                logger.log(&format!("Error waiting for Transmission: {}", e));
                return Err(format!("Error waiting for Transmission: {}", e));
            }
        }

        if let Some(client) = rpc_client
            && let Ok(stats) = client.session_stats()
        {
            *stats_cache.lock().unwrap() = Some(stats);
            if !rpc_announced {
                logger.log("Connected to Transmission RPC");
                rpc_announced = true;
            }
        }

        std::thread::sleep(std::time::Duration::from_secs(5));
    };

    let finish_time = Local::now().format("%Y-%m-%d %H:%M:%S");
    logger.log(&format!(
        "Transmission exited at {} with {}",
        finish_time, status
    ));
    log_session_totals(logger, stats_cache);

    Ok(())
}

fn log_session_totals(logger: &Logger, stats_cache: &Mutex<Option<Value>>) {
    let cache = stats_cache.lock().unwrap();
    match cache.as_ref().and_then(|s| s.get("current-stats")) {
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
    let log_file = match &args.log_file {
        Some(path) => path.clone(),
        None => default_log_file()?,
    };

    let logger = Logger::new(log_file, args.verbose);
    logger.writeln_file("=== transmission-tower invoked ===");

    if let Err(err) = run_app_logic(&args, &logger) {
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
    if args.verbose {
        #[cfg(target_os = "windows")]
        setup_verbose_console();
    }
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

    #[test]
    fn test_default_log_file() {
        let home = dirs::home_dir().expect("Home dir should be present in test env");
        let expected = home.join(".transmission-tower.log");
        assert_eq!(default_log_file().unwrap(), expected);
    }

    #[test]
    fn test_run_app_with_port_override_and_skip_launch() {
        let dir = tempdir().unwrap();
        let trans_dir = dir.path().join("transmission");
        fs::create_dir_all(&trans_dir).unwrap();
        let settings_file = trans_dir.join("settings.json");

        fs::write(&settings_file, r#"{"peer-port": 12345}"#).unwrap();

        let log_file = dir.path().join("log.txt");

        let args = Args {
            config_dir: Some(dir.path().to_path_buf()),
            transmission_path: None,
            log_file: Some(log_file.clone()),
            port: Some(60000),
            skip_launch: true,
            verbose: false,
            no_dashboard: false,
            dashboard_port: 9191,
            rpc_username: None,
            rpc_password: None,
        };

        assert!(run_app(args).is_ok());

        let updated_content = fs::read_to_string(&settings_file).unwrap();
        let parsed: Value = serde_json::from_str(&updated_content).unwrap();
        assert_eq!(parsed["peer-port"], 60000);

        let log_content = fs::read_to_string(&log_file).unwrap();
        assert!(log_content.contains("Successfully updated port to 60000"));
        assert!(log_content.contains("Transmission launch skipped"));
    }

    #[test]
    fn test_run_app_missing_settings_file() {
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("log.txt");

        let args = Args {
            config_dir: Some(dir.path().to_path_buf()),
            transmission_path: None,
            log_file: Some(log_file.clone()),
            port: Some(60000),
            skip_launch: true,
            verbose: false,
            no_dashboard: false,
            dashboard_port: 9191,
            rpc_username: None,
            rpc_password: None,
        };

        let result = run_app(args);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Settings file does not exist"));

        let log_content = fs::read_to_string(&log_file).unwrap();
        assert!(log_content.contains("ERROR: Settings file does not exist"));
    }

    #[test]
    fn test_port_zero_validation() {
        let dir = tempdir().unwrap();
        let log_file = dir.path().join("log.txt");

        let args = Args {
            config_dir: Some(dir.path().to_path_buf()),
            transmission_path: None,
            log_file: Some(log_file.clone()),
            port: Some(0),
            skip_launch: true,
            verbose: false,
            no_dashboard: false,
            dashboard_port: 9191,
            rpc_username: None,
            rpc_password: None,
        };

        let result = run_app(args);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Port number cannot be 0");

        let log_content = fs::read_to_string(&log_file).unwrap();
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
            "Failed to find a valid port on clipboard after 30 seconds."
        );

        let log_content = fs::read_to_string(&log_file).unwrap();
        assert!(log_content.contains("Attempt 1/2:"));
        assert!(log_content.contains("Attempt 2/2:"));
        assert!(log_content.contains("Failed to find a valid port on clipboard after 30 seconds."));
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

            let log_content = fs::read_to_string(&log_file).unwrap();
            assert!(log_content.contains("Found valid port 54321 on clipboard"));
            assert!(log_content.contains("Clipboard reset to empty."));

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

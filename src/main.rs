use std::{
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, header};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sysinfo::System;
use tokio::process::Command;
use tokio_tungstenite::tungstenite::Message;

mod delivery;
mod process;
mod self_update;
mod setup;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const TOOL_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const MAX_TOOL_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
const CDP_BASE: &str = "http://127.0.0.1:9222";

#[derive(Clone, Serialize, Deserialize)]
struct WorkerConfig {
    controller_url: String,
    user_id: String,
    machine_id: String,
    access_token: String,
}

#[derive(Deserialize)]
struct ToolCall {
    id: String,
    thread_id: String,
    name: String,
    arguments: Value,
}

#[derive(Deserialize)]
struct DevtoolsTarget {
    #[serde(rename = "webSocketDebuggerUrl")]
    websocket_url: Option<String>,
    #[serde(rename = "type")]
    target_type: String,
}

#[derive(Serialize)]
struct Heartbeat {
    hostname: String,
    version: &'static str,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "cybion_worker=info".to_owned()),
        )
        .compact()
        .init();
    let command = parse_command()?;
    match command {
        WorkerCommand::Run { config, background } => {
            let lock = setup::lock(&config)?;
            let loaded = setup::load_or_pair(&config).await?;
            if background {
                drop(lock);
                launch_background(config).await
            } else {
                let installed = run(loaded, &config).await?;
                drop(lock);
                if let Err(error) = launch_background_at(installed.clone(), config.clone()).await {
                    self_update::rollback(&installed)?;
                    launch_background_at(installed, config).await?;
                    return Err(
                        error.context("Upgrade startup failed; previous executable restored")
                    );
                }
                Ok(())
            }
        }
        WorkerCommand::Status(config) => setup::status(&config).await,
        WorkerCommand::Doctor(config) => setup::doctor(&config).await,
        WorkerCommand::Help => {
            println!(
                "Cybion Worker {}\n\ncybion-worker [run] [--background] [--config PATH]\n  First run opens browser authorization; existing configuration is preserved.\ncybion-worker status [--config PATH]\ncybion-worker doctor [--config PATH]\ncybion-worker config-path\n\nBackground mode does not install login/startup persistence.\nTo disconnect, remove this device in Cybion. To re-pair, stop the process and move the config aside first.",
                env!("CARGO_PKG_VERSION")
            );
            Ok(())
        }
        WorkerCommand::ConfigPath => {
            println!("{}", default_config_path()?.display());
            Ok(())
        }
    }
}

enum WorkerCommand {
    Run { config: PathBuf, background: bool },
    ConfigPath,
    Status(PathBuf),
    Doctor(PathBuf),
    Help,
}

fn parse_command() -> Result<WorkerCommand> {
    let mut arguments = std::env::args_os().skip(1);
    match arguments.next() {
        None => parse_run_arguments(arguments),
        Some(argument) if argument == "run" => parse_run_arguments(arguments),
        Some(argument) if argument == "--help" || argument == "help" || argument == "--version" => {
            Ok(WorkerCommand::Help)
        }
        Some(argument) if argument == "status" || argument == "doctor" => {
            let WorkerCommand::Run { config, background } = parse_run_arguments(arguments)? else {
                unreachable!()
            };
            ensure!(!background, "status/doctor do not accept --background");
            Ok(if argument == "status" {
                WorkerCommand::Status(config)
            } else {
                WorkerCommand::Doctor(config)
            })
        }
        Some(argument) if argument == "config-path" => {
            ensure!(
                arguments.next().is_none(),
                "config-path accepts no arguments"
            );
            Ok(WorkerCommand::ConfigPath)
        }
        Some(argument) => bail!("unknown command: {}", argument.to_string_lossy()),
    }
}

fn parse_run_arguments(
    mut arguments: impl Iterator<Item = std::ffi::OsString>,
) -> Result<WorkerCommand> {
    let mut config = default_config_path()?;
    let mut background = false;
    while let Some(argument) = arguments.next() {
        if argument == "--background" {
            background = true;
        } else if argument == "--config" {
            config = arguments
                .next()
                .map(PathBuf::from)
                .context("--config requires a path")?;
        } else {
            bail!("unknown argument: {}", argument.to_string_lossy());
        }
    }
    Ok(WorkerCommand::Run { config, background })
}

fn default_config_path() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .context("cannot determine the current user's home directory")?;
    Ok(PathBuf::from(home).join(".cybion").join("worker.toml"))
}

fn load_config(path: &Path) -> Result<WorkerConfig> {
    let content =
        fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    let config: WorkerConfig = toml::from_str(&content)
        .with_context(|| format!("{} is not valid TOML", path.display()))?;
    let controller_url = config.controller_url.trim_end_matches('/');
    let parsed = url::Url::parse(controller_url).context("controller_url must be an HTTPS URL")?;
    ensure!(
        parsed.scheme() == "https" || parsed.host_str() == Some("localhost"),
        "controller_url must use HTTPS"
    );
    ensure!(!config.user_id.trim().is_empty(), "user_id is required");
    ensure!(
        config.user_id.len() <= 128
            && config
                .user_id
                .bytes()
                .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'-' | b'.')),
        "user_id contains unsupported characters"
    );
    ensure!(
        !config.machine_id.trim().is_empty(),
        "machine_id is required"
    );
    ensure!(
        !config.access_token.trim().is_empty(),
        "access_token is required"
    );
    Ok(WorkerConfig {
        controller_url: controller_url.to_owned(),
        ..config
    })
}

async fn launch_background(config: PathBuf) -> Result<()> {
    let executable = std::env::current_exe().context("cannot determine the Worker executable")?;
    launch_background_at(executable, config).await
}

async fn launch_background_at(executable: PathBuf, config: PathBuf) -> Result<()> {
    let log_path = config
        .parent()
        .context("config path has no parent")?
        .join("worker.log");
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let error_log = log.try_clone()?;
    let mut command = std::process::Command::new(executable);
    command
        .arg("run")
        .arg("--config")
        .arg(&config)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(error_log));
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0000_0008 | 0x0800_0000);
    }
    let mut child = command
        .spawn()
        .context("could not start the background Worker")?;
    for _ in 0..150 {
        if let Some(status) = child.try_wait()? {
            bail!("Worker exited ({status}); inspect {}", log_path.display());
        }
        if let Ok(content) = fs::read(setup::ready_path(&config))
            && let Ok(status) = serde_json::from_slice::<Value>(&content)
            && status["pid"].as_u64() == Some(u64::from(child.id()))
        {
            println!(
                "Worker connected in the background (PID {}). Logs: {}\nNot configured for automatic startup; use your OS service manager if needed.",
                child.id(),
                log_path.display()
            );
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!(
        "Worker started in the background (PID {}), still connecting. Logs: {}",
        child.id(),
        log_path.display()
    );
    Ok(())
}

async fn run(config: WorkerConfig, config_path: &Path) -> Result<PathBuf> {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(600))
        .user_agent(format!("cybion-worker/{}", env!("CARGO_PKG_VERSION")))
        .build()?;
    let delivery = Arc::new(delivery::DeliveryState::new());
    let reporting_client = client.clone();
    let reporting_config = config.clone();
    let reporting_boot = delivery.boot_id.clone();
    let reporting = tokio::spawn(async move {
        loop {
            if let Err(error) =
                report_liveness_for_boot(&reporting_client, &reporting_config, &reporting_boot)
                    .await
            {
                tracing::warn!(%error, "Worker liveness report failed");
            }
            tokio::time::sleep(HEARTBEAT_INTERVAL).await;
        }
    });
    let mut failures = 0;
    let outcome = loop {
        match event_session_ready(&client, &config, Some(config_path), delivery.clone()).await {
            Ok(Some(upgrade)) => {
                delivery.wait_idle().await;
                let result = async {
                    self_update::report(
                        &client,
                        &config,
                        &delivery.boot_id,
                        &upgrade,
                        "installing",
                        None,
                    )
                    .await?;
                    let installed = self_update::install(&client, &upgrade.version).await?;
                    Ok::<_, anyhow::Error>(installed)
                }
                .await;
                match result {
                    Ok(installed) => break Ok(installed),
                    Err(error) => {
                        tracing::warn!(%error, "Worker upgrade failed; current executable retained");
                        if let Err(report_error) = self_update::report(
                            &client,
                            &config,
                            &delivery.boot_id,
                            &upgrade,
                            "failed",
                            Some(error.to_string()),
                        )
                        .await
                        {
                            break Err(report_error);
                        }
                    }
                }
                failures = 0;
            }
            Ok(None) => failures = 0,
            Err(error) => {
                if !delivery::retryable(&error) {
                    break Err(error.context(
                        "Worker connection rejected; inspect configuration or re-pair this device",
                    ));
                }
                tracing::warn!(%error, "Worker event stream ended; reconnecting");
                failures += 1;
            }
        }
        tokio::time::sleep(delivery::retry_delay(failures)).await;
    };
    reporting.abort();
    outcome
}

async fn report_liveness(client: &Client, config: &WorkerConfig) -> Result<()> {
    report_liveness_for_boot(client, config, "").await
}

async fn report_liveness_for_boot(
    client: &Client,
    config: &WorkerConfig,
    boot_id: &str,
) -> Result<()> {
    let base = worker_url(config);
    let hostname = System::host_name().unwrap_or_else(|| "cybion-worker".to_owned());
    request(
        client,
        config,
        client
            .post(format!("{base}/heartbeat"))
            .header(delivery::BOOT_HEADER, boot_id)
            .timeout(Duration::from_secs(15))
            .json(&Heartbeat {
                hostname,
                version: env!("CARGO_PKG_VERSION"),
            }),
    )
    .await?;
    request(
        client,
        config,
        client
            .post(format!("{base}/resources"))
            .header(delivery::BOOT_HEADER, boot_id)
            .timeout(Duration::from_secs(15))
            .json(&resources()),
    )
    .await?;
    Ok(())
}

fn resources() -> Value {
    let mut system = System::new_all();
    system.refresh_cpu_usage();
    system.refresh_memory();
    json!({
        "logical_cpus": system.cpus().len(),
        "cpu_usage_percent": system.global_cpu_usage(),
        "memory_used_bytes": system.used_memory(),
        "memory_total_bytes": system.total_memory(),
    })
}

#[cfg(test)]
async fn event_session(client: &Client, config: &WorkerConfig) -> Result<()> {
    event_session_ready(
        client,
        config,
        None,
        Arc::new(delivery::DeliveryState::new()),
    )
    .await
    .map(|_| ())
}

async fn event_session_ready(
    client: &Client,
    config: &WorkerConfig,
    ready: Option<&Path>,
    delivery: Arc<delivery::DeliveryState>,
) -> Result<Option<self_update::Upgrade>> {
    let response = client
        .get(format!("{}/events", worker_url(config)))
        .header(delivery::BOOT_HEADER, &delivery.boot_id)
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", config.access_token),
        )
        .send()
        .await?
        .error_for_status()?;
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut ready = ready;
    while let Some(chunk) = tokio::time::timeout(Duration::from_secs(45), stream.next())
        .await
        .context("Worker event stream idle timeout")?
    {
        buffer.extend_from_slice(&chunk?);
        while let Some((separator, width)) = delivery::frame_end(&buffer) {
            if let Some(path) = ready.take() {
                setup::mark_ready(path)?;
            }
            let event =
                String::from_utf8(buffer[..separator].to_vec()).context("invalid event UTF-8")?;
            buffer.drain(..separator + width);
            if let Some(upgrade) = self_update::parse_event(&event)? {
                ensure!(
                    upgrade.boot_id == delivery.boot_id,
                    "upgrade addressed to another Worker process"
                );
                return Ok(Some(upgrade));
            }
            if let Some(call) = parse_sse_call(&event)? {
                if !delivery.admit(&call)? {
                    continue;
                }
                let client = client.clone();
                let config = config.clone();
                let delivery = delivery.clone();
                tokio::spawn(async move {
                    let call_id = call.id.clone();
                    if let Err(error) =
                        execute_and_submit(&client, &config, call, &delivery.boot_id).await
                    {
                        tracing::warn!(%call_id, %error, "Worker result submission permanently rejected");
                    }
                    delivery.finished();
                });
            }
        }
    }
    Ok(None)
}

fn parse_sse_call(event: &str) -> Result<Option<ToolCall>> {
    let mut event_name = "message";
    let mut data = String::new();
    for line in event.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            event_name = value.trim();
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push_str(value.trim_start());
        }
    }
    if event_name != "tool_call" {
        return Ok(None);
    }
    Ok(Some(
        serde_json::from_str(&data).context("Worker received malformed tool_call")?,
    ))
}

async fn execute_and_submit(
    client: &Client,
    config: &WorkerConfig,
    call: ToolCall,
    boot_id: &str,
) -> Result<()> {
    tracing::info!(call_id = %call.id, thread_id = %call.thread_id, tool = %call.name, "Worker executing tool call");
    let result = execute_call(&call).await;
    let (failed, result) = match result {
        Ok(result) => (false, result),
        Err(error) => (true, json!({"error":error.to_string()})),
    };
    tracing::info!(call_id = %call.id, thread_id = %call.thread_id, tool = %call.name, failed, "Worker tool call finished");
    let category = if call.name == "diagnostics" {
        "checks"
    } else {
        "calls"
    };
    let url = format!("{}/{category}/{}/result", worker_url(config), call.id);
    delivery::post_until_confirmed(
        client,
        config,
        &url,
        boot_id,
        &json!({"result":result,"failed":failed}),
    )
    .await?;
    tracing::info!(call_id = %call.id, "Worker result submitted");
    Ok(())
}

async fn request(
    _client: &Client,
    config: &WorkerConfig,
    request: reqwest::RequestBuilder,
) -> Result<()> {
    request
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", config.access_token),
        )
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

fn worker_url(config: &WorkerConfig) -> String {
    format!(
        "{}/worker/v1/users/{}/workers/{}",
        config.controller_url, config.user_id, config.machine_id
    )
}

async fn execute_call(call: &ToolCall) -> Result<Value> {
    match call.name.as_str() {
        "diagnostics" => Ok(setup::diagnostics().await),
        "bash" => bash(&call.arguments).await,
        "browser_control" => browser_control(&call.arguments).await,
        "computer_use" => computer_use(&call.arguments).await,
        unknown => bail!("unsupported Worker tool: {unknown}"),
    }
}

async fn bash(arguments: &Value) -> Result<Value> {
    let command = required_string(arguments, "command")?;
    let process = if cfg!(windows) {
        let mut process = Command::new("cmd");
        process.args(["/C", command]);
        process
    } else {
        let mut process = Command::new("/bin/sh");
        process.args(["-lc", command]);
        process
    };
    let output = process::output(process, TOOL_TIMEOUT, "Bash command").await?;
    Ok(command_output(
        output.status.code(),
        &output.stdout,
        &output.stderr,
    ))
}

async fn browser_control(arguments: &Value) -> Result<Value> {
    let action = required_string(arguments, "action")?;
    ensure_browser().await?;
    match action {
        "navigate" | "open" => {
            let url = valid_url(required_string(arguments, "url")?)?;
            cdp("Page.navigate", json!({"url":url})).await
        }
        "evaluate" => {
            cdp(
                "Runtime.evaluate",
                json!({"expression":required_string(arguments, "text")?,"returnByValue":true}),
            )
            .await
        }
        "click" => {
            let selector = required_string(arguments, "selector")?;
            let expression = format!(
                "document.querySelector({})?.click() ?? false",
                serde_json::to_string(selector)?
            );
            cdp(
                "Runtime.evaluate",
                json!({"expression":expression,"returnByValue":true}),
            )
            .await
        }
        "type" => {
            let selector = required_string(arguments, "selector")?;
            let text = required_string(arguments, "text")?;
            let expression = format!(
                "(() => {{ const element = document.querySelector({}); if (!element) return false; element.focus(); element.value = {}; element.dispatchEvent(new Event('input', {{bubbles:true}})); element.dispatchEvent(new Event('change', {{bubbles:true}})); return true }})()",
                serde_json::to_string(selector)?,
                serde_json::to_string(text)?
            );
            cdp(
                "Runtime.evaluate",
                json!({"expression":expression,"returnByValue":true}),
            )
            .await
        }
        "screenshot" => cdp("Page.captureScreenshot", json!({"format":"png"})).await,
        unknown => bail!("unsupported browser action: {unknown}"),
    }
}

async fn ensure_browser() -> Result<()> {
    if devtools_target().await.is_ok() {
        return Ok(());
    }
    let executable = chromium_executable().context("could not find Chromium, Chrome, or Edge")?;
    let profile = worker_data_dir()?.join("browser");
    fs::create_dir_all(&profile)?;
    std::process::Command::new(executable)
        .arg("--remote-debugging-port=9222")
        .arg("--remote-allow-origins=*")
        .arg(format!("--user-data-dir={}", profile.display()))
        .arg("about:blank")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("could not launch Chromium")?;
    for _ in 0..20 {
        if devtools_target().await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    bail!("Chromium did not expose its DevTools endpoint")
}

async fn cdp(method: &str, params: Value) -> Result<Value> {
    let target = devtools_target().await?;
    let websocket_url = target
        .websocket_url
        .context("browser target has no DevTools socket")?;
    let (mut socket, _) = tokio_tungstenite::connect_async(websocket_url).await?;
    socket
        .send(Message::Text(
            json!({"id":1,"method":method,"params":params})
                .to_string()
                .into(),
        ))
        .await?;
    while let Some(message) = socket.next().await {
        let message = message?;
        if let Message::Text(text) = message {
            let response: Value = serde_json::from_str(&text)?;
            if response.get("id").and_then(Value::as_i64) == Some(1) {
                if let Some(error) = response.get("error") {
                    bail!("Browser Control failed: {error}");
                }
                return Ok(response.get("result").cloned().unwrap_or_else(|| json!({})));
            }
        }
    }
    bail!("Browser Control connection closed without a response")
}

async fn devtools_target() -> Result<DevtoolsTarget> {
    let targets = Client::new()
        .get(format!("{CDP_BASE}/json/list"))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<DevtoolsTarget>>()
        .await?;
    targets
        .into_iter()
        .find(|target| target.target_type == "page" && target.websocket_url.is_some())
        .context("browser has no page target")
}

fn chromium_executable() -> Option<PathBuf> {
    let candidates = if cfg!(target_os = "macos") {
        vec![
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome".into(),
            "/Applications/Chromium.app/Contents/MacOS/Chromium".into(),
            "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge".into(),
        ]
    } else if cfg!(windows) {
        let mut paths = Vec::new();
        for root in ["PROGRAMFILES", "PROGRAMFILES(X86)", "LOCALAPPDATA"] {
            if let Some(root) = std::env::var_os(root) {
                let root = PathBuf::from(root);
                paths.push(root.join("Google/Chrome/Application/chrome.exe"));
                paths.push(root.join("Microsoft/Edge/Application/msedge.exe"));
                paths.push(root.join("Chromium/Application/chrome.exe"));
            }
        }
        paths
    } else {
        vec![
            "/usr/bin/google-chrome".into(),
            "/usr/bin/google-chrome-stable".into(),
            "/usr/bin/chromium".into(),
            "/usr/bin/chromium-browser".into(),
            "/usr/bin/microsoft-edge".into(),
        ]
    };
    candidates.into_iter().find(|path| path.is_file())
}

async fn computer_use(arguments: &Value) -> Result<Value> {
    let action = required_string(arguments, "action")?;
    match action {
        "move" | "click" => {
            let x = required_number(arguments, "x")?;
            let y = required_number(arguments, "y")?;
            let command = computer_command(action, x, y, None)?;
            let output = process::output(command, TOOL_TIMEOUT, "Computer Use command").await?;
            Ok(command_output(
                output.status.code(),
                &output.stdout,
                &output.stderr,
            ))
        }
        "type" => {
            let text = required_string(arguments, "text")?;
            let command = computer_command("type", 0, 0, Some(text))?;
            let output = process::output(command, TOOL_TIMEOUT, "Computer Use command").await?;
            Ok(command_output(
                output.status.code(),
                &output.stdout,
                &output.stderr,
            ))
        }
        unknown => bail!("unsupported computer action: {unknown}"),
    }
}

fn computer_command(action: &str, x: i64, y: i64, text: Option<&str>) -> Result<Command> {
    #[cfg(target_os = "macos")]
    {
        let script = match action {
            "move" => format!(
                "tell application \"System Events\" to set the position of the mouse to {{{x}, {y}}}"
            ),
            "click" => format!("tell application \"System Events\" to click at {{{x}, {y}}}"),
            "type" => format!(
                "tell application \"System Events\" to keystroke {}",
                serde_json::to_string(text.context("text is required")?)?
            ),
            _ => bail!("unsupported computer action"),
        };
        let mut command = Command::new("/usr/bin/osascript");
        command.args(["-e", &script]);
        Ok(command)
    }
    #[cfg(target_os = "windows")]
    {
        let script = match action {
            "move" => format!(
                "Add-Type -AssemblyName System.Windows.Forms; [System.Windows.Forms.Cursor]::Position=New-Object System.Drawing.Point({x},{y})"
            ),
            "click" => "Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; public class Mouse { [DllImport(\"user32.dll\")] public static extern void mouse_event(int f,int x,int y,int d,int e); }'; [Mouse]::mouse_event(2,0,0,0,0); [Mouse]::mouse_event(4,0,0,0,0)".to_owned(),
            "type" => format!(
                "Add-Type -AssemblyName System.Windows.Forms; [System.Windows.Forms.SendKeys]::SendWait({})",
                serde_json::to_string(text.context("text is required")?)?
            ),
            _ => bail!("unsupported computer action"),
        };
        let mut command = Command::new("powershell");
        command.args(["-NoProfile", "-Command", &script]);
        Ok(command)
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let mut command = Command::new("xdotool");
        match action {
            "move" => {
                command.args(["mousemove", &x.to_string(), &y.to_string()]);
            }
            "click" => {
                command.args([
                    "mousemove",
                    "--sync",
                    &x.to_string(),
                    &y.to_string(),
                    "click",
                    "1",
                ]);
            }
            "type" => {
                command.args([
                    "type",
                    "--delay",
                    "1",
                    "--",
                    text.context("text is required")?,
                ]);
            }
            _ => bail!("unsupported computer action"),
        }
        Ok(command)
    }
}

fn required_string<'a>(arguments: &'a Value, key: &str) -> Result<&'a str> {
    arguments
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{key} is required"))
}

fn required_number(arguments: &Value, key: &str) -> Result<i64> {
    arguments
        .get(key)
        .and_then(Value::as_i64)
        .with_context(|| format!("{key} is required"))
}

fn valid_url(value: &str) -> Result<String> {
    let url = url::Url::parse(value).context("url is invalid")?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "url must use HTTP or HTTPS"
    );
    Ok(url.into())
}

fn worker_data_dir() -> Result<PathBuf> {
    let config = default_config_path()?;
    Ok(config
        .parent()
        .context("worker config path has no parent")?
        .to_path_buf())
}

fn command_output(code: Option<i32>, stdout: &[u8], stderr: &[u8]) -> Value {
    json!({
        "exit_code": code,
        "stdout": limited_output(stdout),
        "stderr": limited_output(stderr),
    })
}

fn limited_output(value: &[u8]) -> String {
    let value = &value[..value.len().min(MAX_TOOL_OUTPUT_BYTES)];
    String::from_utf8_lossy(value).to_string()
}

#[cfg(test)]
mod dispatch_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_api_config_without_any_local_database() {
        let config: WorkerConfig = toml::from_str(
            r#"controller_url = "https://cybion.ntnl.io"
user_id = "auth-user"
machine_id = "machine"
access_token = "secret""#,
        )
        .unwrap();
        assert_eq!(config.controller_url, "https://cybion.ntnl.io");
        assert!(!std::any::type_name::<WorkerConfig>().contains("sqlite"));
    }

    #[test]
    fn parses_tool_call_sse_events_only() {
        assert!(
            parse_sse_call("event: heartbeat\ndata: {}\n")
                .unwrap()
                .is_none()
        );
        let call = parse_sse_call("event: tool_call\ndata: {\"id\":\"call\",\"thread_id\":\"thread\",\"name\":\"bash\",\"arguments\":{\"command\":\"pwd\"}}\n").unwrap().unwrap();
        assert_eq!(call.name, "bash");
    }

    #[test]
    fn supports_the_cloud_worker_protocol_paths() {
        let config = WorkerConfig {
            controller_url: "https://cybion.ntnl.io".to_owned(),
            user_id: "auth-user".to_owned(),
            machine_id: "worker".to_owned(),
            access_token: "token".to_owned(),
        };
        assert_eq!(
            worker_url(&config),
            "https://cybion.ntnl.io/worker/v1/users/auth-user/workers/worker"
        );
    }
}

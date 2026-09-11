use std::{
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    process::Stdio,
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

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const TOOL_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const MAX_TOOL_OUTPUT_BYTES: usize = 2 * 1024 * 1024;
const CDP_BASE: &str = "http://127.0.0.1:9222";

#[derive(Clone, Deserialize)]
struct WorkerConfig {
    controller_url: String,
    tenant_id: String,
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
        WorkerCommand::Run {
            config,
            background: true,
        } => launch_background(config),
        WorkerCommand::Run {
            config,
            background: false,
        } => run(load_config(&config)?).await,
        WorkerCommand::ConfigPath => {
            println!("{}", default_config_path()?.display());
            Ok(())
        }
    }
}

enum WorkerCommand {
    Run { config: PathBuf, background: bool },
    ConfigPath,
}

fn parse_command() -> Result<WorkerCommand> {
    let mut arguments = std::env::args_os().skip(1);
    match arguments.next() {
        None => parse_run_arguments(arguments),
        Some(argument) if argument == "run" => parse_run_arguments(arguments),
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
    ensure!(
        config.tenant_id.len() == 64
            && config
                .tenant_id
                .bytes()
                .all(|value| value.is_ascii_hexdigit()),
        "tenant_id must be a SHA-256 hex value"
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

fn launch_background(config: PathBuf) -> Result<()> {
    let executable = std::env::current_exe().context("cannot determine the Worker executable")?;
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
        .arg(config)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(error_log));
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0000_0008 | 0x0800_0000);
    }
    command
        .spawn()
        .context("could not start the background Worker")?;
    println!(
        "Cybion Worker started in the background; logs: {}",
        log_path.display()
    );
    Ok(())
}

async fn run(config: WorkerConfig) -> Result<()> {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(600))
        .user_agent(format!("cybion-worker/{}", env!("CARGO_PKG_VERSION")))
        .build()?;
    let reporting_client = client.clone();
    let reporting_config = config.clone();
    tokio::spawn(async move {
        loop {
            if let Err(error) = report_liveness(&reporting_client, &reporting_config).await {
                tracing::warn!(%error, "Worker liveness report failed");
            }
            tokio::time::sleep(HEARTBEAT_INTERVAL).await;
        }
    });
    loop {
        if let Err(error) = event_session(&client, &config).await {
            tracing::warn!(%error, "Worker event stream ended");
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

async fn report_liveness(client: &Client, config: &WorkerConfig) -> Result<()> {
    let base = worker_url(config);
    let hostname = std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "cybion-worker".to_owned());
    request(
        client,
        config,
        client.post(format!("{base}/heartbeat")).json(&Heartbeat {
            hostname,
            version: env!("CARGO_PKG_VERSION"),
        }),
    )
    .await?;
    request(
        client,
        config,
        client.post(format!("{base}/resources")).json(&resources()),
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

async fn event_session(client: &Client, config: &WorkerConfig) -> Result<()> {
    let response = client
        .get(format!("{}/events", worker_url(config)))
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", config.access_token),
        )
        .send()
        .await?
        .error_for_status()?;
    let mut stream = response.bytes_stream();
    let mut buffer = String::new();
    while let Some(chunk) = stream.next().await {
        buffer.push_str(&String::from_utf8_lossy(&chunk?));
        while let Some(separator) = buffer.find("\n\n") {
            let event = buffer[..separator].replace('\r', "");
            buffer.drain(..separator + 2);
            if let Some(call) = parse_sse_call(&event)? {
                execute_and_submit(client, config, call).await?;
            }
        }
    }
    Ok(())
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

async fn execute_and_submit(client: &Client, config: &WorkerConfig, call: ToolCall) -> Result<()> {
    tracing::info!(call_id = %call.id, thread_id = %call.thread_id, tool = %call.name, "Worker executing tool call");
    let result = execute_call(&call).await;
    let (failed, result) = match result {
        Ok(result) => (false, result),
        Err(error) => (true, json!({"error":error.to_string()})),
    };
    let url = format!("{}/calls/{}/result", worker_url(config), call.id);
    request(
        client,
        config,
        client
            .post(url)
            .json(&json!({"result":result,"failed":failed})),
    )
    .await
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
        "{}/worker/v1/tenants/{}/workers/{}",
        config.controller_url, config.tenant_id, config.machine_id
    )
}

async fn execute_call(call: &ToolCall) -> Result<Value> {
    match call.name.as_str() {
        "bash" => bash(&call.arguments).await,
        "browser_control" => browser_control(&call.arguments).await,
        "computer_use" => computer_use(&call.arguments).await,
        unknown => bail!("unsupported Worker tool: {unknown}"),
    }
}

async fn bash(arguments: &Value) -> Result<Value> {
    let command = required_string(arguments, "command")?;
    let mut process = if cfg!(windows) {
        let mut process = Command::new("cmd");
        process.args(["/C", command]);
        process
    } else {
        let mut process = Command::new("/bin/sh");
        process.args(["-lc", command]);
        process
    };
    let output = tokio::time::timeout(TOOL_TIMEOUT, process.output())
        .await
        .context("Bash command timed out")??;
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
            let mut command = computer_command(action, x, y, None)?;
            let output = tokio::time::timeout(TOOL_TIMEOUT, command.output()).await??;
            Ok(command_output(
                output.status.code(),
                &output.stdout,
                &output.stderr,
            ))
        }
        "type" => {
            let text = required_string(arguments, "text")?;
            let mut command = computer_command("type", 0, 0, Some(text))?;
            let output = tokio::time::timeout(TOOL_TIMEOUT, command.output()).await??;
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
            "click" => format!(
                "Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; public class Mouse {{ [DllImport(\"user32.dll\")] public static extern void mouse_event(int f,int x,int y,int d,int e); }}'; [Mouse]::mouse_event(2,0,0,0,0); [Mouse]::mouse_event(4,0,0,0,0)"
            ),
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
mod tests {
    use super::*;

    #[test]
    fn parses_api_config_without_any_local_database() {
        let config: WorkerConfig = toml::from_str(
            r#"controller_url = "https://cybion.ntnl.io"
tenant_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
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
            tenant_id: "a".repeat(64),
            machine_id: "worker".to_owned(),
            access_token: "token".to_owned(),
        };
        assert_eq!(
            worker_url(&config),
            format!(
                "https://cybion.ntnl.io/worker/v1/tenants/{}/workers/worker",
                "a".repeat(64)
            )
        );
    }
}

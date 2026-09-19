use super::*;
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

const CONTROLLER: &str = "https://cybion.ntnl.io";

#[derive(Serialize, Deserialize)]
struct Pending {
    id: String,
    user_code: String,
    expires_at: u64,
    device_secret: String,
    access_token: String,
}

#[derive(Deserialize)]
struct StartReply {
    id: String,
    user_code: String,
    expires_at: u64,
}
#[derive(Deserialize)]
struct PollReply {
    status: String,
    user_id: Option<String>,
    machine_id: String,
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn lock(path: &Path) -> Result<File> {
    let parent = path.parent().context("config path has no parent")?;
    fs::create_dir_all(parent)?;
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path.with_extension("lock"))?;
    file.try_lock_exclusive().context("A Worker or setup process already owns this config. Run status instead of starting a second instance.")?;
    Ok(file)
}

pub fn save_new(path: &Path, content: &[u8]) -> Result<()> {
    let parent = path.parent().context("config path has no parent")?;
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(content)?;
    temp.as_file().sync_all()?;
    temp.persist_noclobber(path)
        .with_context(|| format!("Refusing to overwrite {}", path.display()))?;
    Ok(())
}

fn secret() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

pub async fn load_or_pair(path: &Path) -> Result<WorkerConfig> {
    if path.try_exists()? {
        return load_config(path);
    }
    pair(path, CONTROLLER).await
}

async fn pair(path: &Path, controller: &str) -> Result<WorkerConfig> {
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(15))
        .build()?;
    let pending_path = path.with_extension("pairing.json");
    let pending = if pending_path.try_exists()? {
        let pending: Pending = serde_json::from_slice(&fs::read(&pending_path)?)
            .context("Invalid pending pairing file")?;
        // The server may have approved just before the local deadline. Never
        // discard the device credential based only on the original local expiry.
        let response = client
            .get(format!("{controller}/worker/v1/pairings/{}", pending.id))
            .bearer_auth(&pending.device_secret)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::GONE {
            fs::remove_file(&pending_path)?;
            None
        } else {
            response.error_for_status()?;
            Some(pending)
        }
    } else {
        None
    };
    let pending = match pending {
        Some(pending) => pending,
        None => {
            let device_secret = secret();
            let access_token = secret();
            let reply: StartReply = client.post(format!("{controller}/worker/v1/pairings"))
                .json(&json!({"device_secret":device_secret,"token_hash":hex::encode(Sha256::digest(access_token.as_bytes())),
                    "hostname":System::host_name().unwrap_or_else(|| "My device".into()),
                    "platform":format!("{} / {}",std::env::consts::OS,std::env::consts::ARCH),"version":env!("CARGO_PKG_VERSION")}))
                .send().await.context("Could not contact Cybion. Check your network and run the Worker again.")?
                .error_for_status()?.json().await?;
            let pending = Pending {
                id: reply.id,
                user_code: reply.user_code,
                expires_at: reply.expires_at,
                device_secret,
                access_token,
            };
            save_new(&pending_path, &serde_json::to_vec(&pending)?)?;
            pending
        }
    };
    let url = format!("{controller}/#/workers?code={}", pending.user_code);
    println!(
        "\n连接设备 / Connect device\n\n  {url}\n\n配对码 / Pairing code: {}\n\n请在网页核对设备并确认授权。请勿批准他人发送的配对码。\nConfirm this device and code in Cybion. Do not approve codes sent by others.\nWaiting for approval (10 minute expiry). Ctrl+C is safe; run again to resume.\n",
        pending.user_code
    );
    #[cfg(not(test))]
    if let Err(error) = open_browser(&url) {
        eprintln!("Could not open a browser ({error}). Open the address above on any device.");
    }
    loop {
        if now() >= pending.expires_at + 630 {
            bail!(
                "Could not finish pairing within the recovery window. Run again to recover the server state; pending credentials have been preserved."
            );
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        let response = client
            .get(format!("{controller}/worker/v1/pairings/{}", pending.id))
            .bearer_auth(&pending.device_secret)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) if error.is_connect() || error.is_timeout() => {
                eprintln!("Network unavailable; retrying until pairing expires…");
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
            || response.status().is_server_error()
        {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        if response.status() == reqwest::StatusCode::GONE {
            fs::remove_file(&pending_path)?;
            bail!("Pairing expired. Run the Worker again for a new code.");
        }
        let reply: PollReply = response.error_for_status()?.json().await?;
        match reply.status.as_str() {
            "pending" | "approving" => continue,
            "cancelled" => {
                fs::remove_file(&pending_path)?;
                bail!("Pairing cancelled. Run the Worker again when ready.");
            }
            "approved" => {
                let config = WorkerConfig {
                    controller_url: controller.into(),
                    user_id: reply.user_id.context("approval has no owner")?,
                    machine_id: reply.machine_id,
                    access_token: pending.access_token,
                };
                save_new(path, toml::to_string(&config)?.as_bytes())?;
                fs::remove_file(&pending_path)?;
                println!("已配对 / Paired. Configuration saved to {}", path.display());
                return Ok(config);
            }
            other => bail!("Unexpected pairing status: {other}"),
        }
    }
}

#[cfg(not(test))]
fn open_browser(url: &str) -> Result<()> {
    let mut command;
    #[cfg(target_os = "macos")]
    {
        command = std::process::Command::new("open");
        command.arg(url);
    }
    #[cfg(windows)]
    {
        command = std::process::Command::new("rundll32.exe");
        command.args(["url.dll,FileProtocolHandler", url]);
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if std::env::var_os("DISPLAY").is_none() && std::env::var_os("WAYLAND_DISPLAY").is_none() {
            return Ok(());
        }
        command = std::process::Command::new("xdg-open");
        command.arg(url);
    }
    let status = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    ensure!(status.success(), "browser launcher failed");
    Ok(())
}

pub async fn diagnostics() -> Value {
    let shell = match tokio::time::timeout(
        Duration::from_secs(5),
        bash(&json!({"command":"echo cybion-worker-check"})),
    )
    .await
    {
        Ok(Ok(result))
            if result["exit_code"] == 0
                && result["stdout"]
                    .as_str()
                    .is_some_and(|s| s.trim() == "cybion-worker-check") =>
        {
            json!({"status":"ready","detail":"shell_ok"})
        }
        _ => json!({"status":"failed","detail":"shell_failed"}),
    };
    let browser = if chromium_executable().is_some() {
        json!({"status":"not_checked","detail":"browser_installed"})
    } else {
        json!({"status":"missing_dependency","detail":"browser_missing"})
    };
    let desktop = if cfg!(target_os = "linux") && std::env::var_os("DISPLAY").is_none() {
        json!({"status":"unsupported","detail":"no_display"})
    } else {
        json!({"status":"not_checked","detail":"desktop_permissions"})
    };
    json!({"shell":shell,"browser":browser,"desktop":desktop,"platform":std::env::consts::OS,
        "arch":std::env::consts::ARCH,"version":env!("CARGO_PKG_VERSION")})
}

pub fn ready_path(config: &Path) -> PathBuf {
    config.with_extension("status.json")
}

pub fn mark_ready(config: &Path) -> Result<()> {
    let content = json!({"pid":std::process::id(),"connected_at":now()}).to_string();
    let path = ready_path(config);
    let mut temp =
        tempfile::NamedTempFile::new_in(path.parent().context("status path has no parent")?)?;
    temp.write_all(content.as_bytes())?;
    temp.persist(path)?;
    Ok(())
}

pub async fn status(path: &Path) -> Result<()> {
    let config = load_config(path)?;
    println!(
        "Config: {}\nController: {}\nWorker: {}",
        path.display(),
        config.controller_url,
        config.machine_id
    );
    let lock = lock(path);
    if lock.is_ok() {
        println!("Local process: not running. Start: cybion-worker run --background");
    } else {
        println!("Local process: running or setup in progress");
    }
    match fs::read_to_string(ready_path(path)) {
        Ok(content) => println!(
            "Last successful task-channel connection (not a live health guarantee): {content}"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            println!("Task channel: not yet observed")
        }
        Err(error) => return Err(error.into()),
    }
    println!(
        "Logs: {}",
        path.parent()
            .context("config path has no parent")?
            .join("worker.log")
            .display()
    );
    Ok(())
}

pub async fn doctor(path: &Path) -> Result<()> {
    let config = load_config(path)?;
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    // This only authenticates a heartbeat; the web check validates the real SSE round trip.
    report_liveness(&client,&config).await.context("Controller authentication/network check failed. Check credentials, HTTPS access and device revocation.")?;
    println!(
        "Controller authentication: OK\n{}\nRun the web connection check to test the task channel. Browser and desktop permissions are NOT tested by doctor.",
        serde_json::to_string_pretty(&diagnostics().await)?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_writes_are_private_and_never_overwrite_existing_configuration() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("worker.toml");
        save_new(&path, b"original").unwrap();
        assert!(save_new(&path, b"replacement").is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), "original");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
    #[test]
    fn second_instance_is_rejected_and_lock_is_reusable_after_exit() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("worker.toml");
        let first = lock(&path).unwrap();
        assert!(lock(&path).is_err());
        drop(first);
        assert!(lock(&path).is_ok());
    }
    #[tokio::test]
    async fn diagnostics_checks_shell_but_never_claims_desktop_or_browser_ready() {
        let result = diagnostics().await;
        assert_eq!(result["shell"]["status"], "ready");
        assert_ne!(result["browser"]["status"], "ready");
        assert_ne!(result["desktop"]["status"], "ready");
    }
    #[test]
    fn random_credentials_are_distinct_and_not_printed_in_config_status() {
        let a = secret();
        let b = secret();
        assert_eq!(a.len(), 64);
        assert_ne!(a, b);
    }
}

#[cfg(test)]
mod pairing_tests {
    use super::*;
    use axum::{
        Json, Router,
        http::{HeaderMap, StatusCode},
        routing::{get, post},
    };
    use std::sync::{Arc, Mutex};
    #[tokio::test]
    async fn authorization_persists_the_device_generated_credential_and_recovers_a_pending_request()
    {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("worker.toml");
        let device_secret = "a".repeat(64);
        let access_token = "b".repeat(64);
        let pending = Pending {
            id: "session".into(),
            user_code: "ABCD-1234-EF56".into(),
            expires_at: now() - 1,
            device_secret: device_secret.clone(),
            access_token: access_token.clone(),
        };
        save_new(
            &path.with_extension("pairing.json"),
            &serde_json::to_vec(&pending).unwrap(),
        )
        .unwrap();
        let polls = Arc::new(Mutex::new(0));
        let counter = polls.clone();
        let router = Router::new()
            .route(
                "/worker/v1/pairings/session",
                get(move |headers: HeaderMap| {
                    let counter = counter.clone();
                    let secret = device_secret.clone();
                    async move {
                        assert_eq!(headers["authorization"], format!("Bearer {secret}"));
                        *counter.lock().unwrap() += 1;
                        Json(json!({"status":"approved","user_id":"owner","machine_id":"machine"}))
                    }
                }),
            )
            .route(
                "/worker/v1/pairings",
                post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://localhost:{}", listener.local_addr().unwrap().port());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let config = pair(&path, &url).await.unwrap();
        assert_eq!(config.access_token, access_token);
        assert_eq!(config.user_id, "owner");
        assert!(!path.with_extension("pairing.json").exists());
        assert_eq!(load_config(&path).unwrap().machine_id, "machine");
        assert!(*polls.lock().unwrap() >= 2);
        server.abort();
    }
    #[tokio::test]
    async fn cancellation_never_writes_a_configuration() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("worker.toml");
        let seen = Arc::new(Mutex::new(None::<String>));
        let save = seen.clone();
        let router=Router::new().route("/worker/v1/pairings",post(move |Json(input):Json<Value>| {
            let save=save.clone();async move {
                assert!(input.get("access_token").is_none());assert_eq!(input["token_hash"].as_str().unwrap().len(),64);
                *save.lock().unwrap()=Some(input["device_secret"].as_str().unwrap().into());
                Json(json!({"id":"session","user_code":"ABCD-1234-EF56","expires_at":now()+600}))
            }
        })).route("/worker/v1/pairings/session",get(move |headers:HeaderMap| {
            let seen=seen.clone();async move {
                assert_eq!(headers["authorization"],format!("Bearer {}",seen.lock().unwrap().as_ref().unwrap()));
                Json(json!({"status":"cancelled","user_id":null,"machine_id":"machine"}))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://localhost:{}", listener.local_addr().unwrap().port());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        assert!(pair(&path, &url).await.is_err());
        assert!(!path.exists());
        assert!(!path.with_extension("pairing.json").exists());
        server.abort();
    }
}

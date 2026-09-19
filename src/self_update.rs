use super::*;
use sha2::{Digest, Sha256};
use std::io::{Cursor, Read};

const RELEASES: &str = "https://github.com/zccz14/cybion-worker/releases/download";
const MAX_ARCHIVE: usize = 64 * 1024 * 1024;

#[derive(Deserialize)]
pub struct Upgrade {
    pub id: String,
    pub version: String,
    pub boot_id: String,
}

pub fn parse_event(event: &str) -> Result<Option<Upgrade>> {
    if !event.lines().any(|line| {
        line.strip_prefix("event:")
            .is_some_and(|v| v.trim() == "upgrade")
    }) {
        return Ok(None);
    }
    let data = event
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .collect::<String>();
    Ok(Some(serde_json::from_str(&data)?))
}

fn version(value: &str) -> Result<(u64, u64, u64)> {
    let parts: Vec<_> = value
        .strip_prefix('v')
        .unwrap_or(value)
        .split('.')
        .collect();
    ensure!(
        parts.len() == 3
            && parts
                .iter()
                .all(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit())),
        "invalid release version"
    );
    Ok((parts[0].parse()?, parts[1].parse()?, parts[2].parse()?))
}

fn platform(os: &str, arch: &str) -> Result<String> {
    let name = match (os, arch) {
        ("macos", "aarch64") => "macos-aarch64",
        ("macos", "x86_64") => "macos-x86_64",
        ("linux", "aarch64") => "linux-aarch64",
        ("linux", "x86_64") => "linux-x86_64",
        ("windows", "x86_64") => "windows-x86_64",
        _ => bail!("no official Worker build for this platform"),
    };
    Ok(format!("cybion-worker-{name}"))
}

async fn download(client: &Client, url: &str, limit: usize) -> Result<Vec<u8>> {
    let response = client
        .get(url)
        .timeout(Duration::from_secs(180))
        .send()
        .await?
        .error_for_status()?;
    ensure!(
        response.content_length().is_none_or(|n| n <= limit as u64),
        "release asset too large"
    );
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        ensure!(
            bytes.len() + chunk.len() <= limit,
            "release asset too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn verify(archive: &[u8], checksum: &[u8], name: &str) -> Result<()> {
    let text = std::str::from_utf8(checksum)?;
    let fields: Vec<_> = text.split_whitespace().collect();
    ensure!(
        fields.len() == 2 && fields[1].trim_start_matches('*') == name,
        "invalid release checksum manifest"
    );
    ensure!(
        hex::encode(Sha256::digest(archive)) == fields[0].to_ascii_lowercase(),
        "release checksum mismatch"
    );
    Ok(())
}

fn extract(archive: &[u8], expected: &str, target: &mut fs::File) -> Result<()> {
    let decoder = flate2::read::GzDecoder::new(Cursor::new(archive));
    let mut archive = tar::Archive::new(decoder);
    let mut found = false;
    for entry in archive.entries()? {
        let entry = entry?;
        if entry.path()?.as_ref() != Path::new(expected) {
            continue;
        }
        ensure!(
            !found && entry.header().entry_type().is_file() && entry.size() <= MAX_ARCHIVE as u64,
            "invalid executable entry"
        );
        std::io::copy(&mut entry.take(MAX_ARCHIVE as u64 + 1), target)?;
        found = true;
    }
    ensure!(found, "release archive has no expected executable");
    target.sync_all()?;
    Ok(())
}

pub async fn install(client: &Client, requested: &str) -> Result<PathBuf> {
    let target_version = version(requested)?;
    ensure!(
        target_version > version(env!("CARGO_PKG_VERSION"))?,
        "only a newer official release may be installed"
    );
    let tag = format!(
        "v{}.{}.{}",
        target_version.0, target_version.1, target_version.2
    );
    let folder = platform(std::env::consts::OS, std::env::consts::ARCH)?;
    let asset = format!("{folder}.tar.gz");
    let url = format!("{RELEASES}/{tag}/{asset}");
    let archive = download(client, &url, MAX_ARCHIVE).await?;
    let checksum = download(client, &format!("{url}.sha256"), 1024).await?;
    verify(&archive, &checksum, &asset)?;
    let installed = std::env::current_exe()?;
    let parent = installed.parent().context("executable has no parent")?;
    let binary = if cfg!(windows) {
        "cybion-worker.exe"
    } else {
        "cybion-worker"
    };
    let mut staged = tempfile::Builder::new()
        .prefix("cybion-update-")
        .suffix(if cfg!(windows) { ".exe" } else { "" })
        .tempfile_in(parent)?;
    extract(
        &archive,
        &format!("{folder}/{binary}"),
        staged.as_file_mut(),
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(staged.path(), fs::Permissions::from_mode(0o755))?;
    }
    // Windows requires closing the staging writer before executing the image.
    let staged = staged.into_temp_path();
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        Command::new(&staged)
            .arg("--help")
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    ensure!(
        output.status.success()
            && String::from_utf8_lossy(&output.stdout).contains(&format!(
                "Cybion Worker {}.{}.{}",
                target_version.0, target_version.1, target_version.2
            )),
        "downloaded executable failed version preflight"
    );
    fs::copy(&installed, backup_path(&installed))?;
    fs::OpenOptions::new()
        .write(true)
        .open(backup_path(&installed))?
        .sync_all()?;
    self_replace::self_replace(&staged)?;
    Ok(installed)
}

fn backup_path(installed: &Path) -> PathBuf {
    installed.with_extension("previous")
}

pub fn rollback(installed: &Path) -> Result<()> {
    fs::copy(backup_path(installed), installed)
        .context("could not restore previous Worker executable")?;
    Ok(())
}

pub async fn report(
    client: &Client,
    config: &WorkerConfig,
    boot_id: &str,
    upgrade: &Upgrade,
    status: &str,
    error: Option<String>,
) -> Result<()> {
    delivery::post_until_confirmed(
        client,
        config,
        &format!("{}/upgrade", worker_url(config)),
        boot_id,
        &json!({"id":upgrade.id,"status":status,"error":error}),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_numeric_versions_and_official_platforms_are_accepted() {
        assert_eq!(version("v0.2.0").unwrap(), (0, 2, 0));
        for value in ["../evil", "0.2.0/evil", "0.2", "0.2.0;sh", "0.2.-1"] {
            assert!(version(value).is_err());
        }
        assert!(platform("linux", "x86_64").is_ok());
        assert!(platform("windows", "aarch64").is_err());
    }
    #[test]
    fn checksum_must_match_both_asset_and_bytes() {
        let text = format!(
            "{}  asset.tar.gz\n",
            hex::encode(Sha256::digest(b"fixture"))
        );
        verify(b"fixture", text.as_bytes(), "asset.tar.gz").unwrap();
        assert!(verify(b"tampered", text.as_bytes(), "asset.tar.gz").is_err());
        assert!(verify(b"fixture", text.as_bytes(), "other.tar.gz").is_err());
    }
    #[test]
    fn extraction_only_writes_the_exact_regular_executable() {
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut archive = tar::Builder::new(encoder);
        let mut header = tar::Header::new_gnu();
        header.set_size(7);
        header.set_mode(0o755);
        header.set_cksum();
        archive
            .append_data(&mut header, "folder/cybion-worker", &b"fixture"[..])
            .unwrap();
        let bytes = archive.into_inner().unwrap().finish().unwrap();
        let mut target = tempfile::tempfile().unwrap();
        extract(&bytes, "folder/cybion-worker", &mut target).unwrap();
        assert!(extract(&bytes, "wrong/cybion-worker", &mut target).is_err());
        assert_eq!(target.metadata().unwrap().len(), 7);
    }
}

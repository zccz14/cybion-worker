use std::{
    process::{Output, Stdio},
    time::Duration,
};

use anyhow::{Context, Result};
use process_wrap::tokio::{CommandWrap, KillOnDrop};
use tokio::{io::AsyncReadExt, process::Command};

pub async fn output(mut command: Command, timeout: Duration, label: &str) -> Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut command = CommandWrap::from(command);
    command.wrap(KillOnDrop);
    #[cfg(unix)]
    command.wrap(process_wrap::tokio::ProcessGroup::leader());
    #[cfg(windows)]
    command.wrap(process_wrap::tokio::JobObject);
    let mut child = command
        .spawn()
        .with_context(|| format!("could not start {label}"))?;
    let mut stdout_pipe = child
        .stdout()
        .take()
        .context("command stdout is not piped")?;
    let mut stderr_pipe = child
        .stderr()
        .take()
        .context("command stderr is not piped")?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let status = tokio::time::timeout(timeout, async {
        tokio::try_join!(
            child.wait(),
            stdout_pipe.read_to_end(&mut stdout),
            stderr_pipe.read_to_end(&mut stderr),
        )
    })
    .await
    .with_context(|| format!("{label} timed out"))
    .and_then(|result| result.map(|(status, _, _)| status).map_err(Into::into));
    if status.is_err() {
        child
            .start_kill()
            .with_context(|| format!("could not terminate {label} process tree"))?;
        child
            .wait()
            .await
            .with_context(|| format!("could not reap {label}"))?;
    }
    Ok(Output {
        status: status?,
        stdout,
        stderr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sysinfo::{Pid, ProcessStatus, System};

    // This test executable also provides a portable, real three-generation process tree.
    #[test]
    #[ignore]
    #[expect(
        clippy::zombie_processes,
        reason = "The fixture deliberately exits before its descendants to test timeout cleanup"
    )]
    fn process_tree_fixture() {
        let root = std::env::var("CYBION_TEST_TREE_ROOT").unwrap();
        let depth: u32 = std::env::var("CYBION_TEST_TREE_DEPTH")
            .unwrap()
            .parse()
            .unwrap();
        std::fs::write(
            format!("{root}/{depth}.pid"),
            std::process::id().to_string(),
        )
        .unwrap();
        if depth < 2 {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "process::tests::process_tree_fixture",
                    "--ignored",
                    "--nocapture",
                ])
                .env("CYBION_TEST_TREE_DEPTH", (depth + 1).to_string())
                .spawn()
                .unwrap();
            if depth == 0 && std::env::var_os("CYBION_TEST_PARENT_EXITS").is_some() {
                return;
            }
            child.wait().unwrap();
        } else {
            std::thread::sleep(Duration::from_secs(30));
        }
    }

    async fn check_timeout_tree(parent_exits: bool) {
        let root = tempfile::tempdir().unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "process::tests::process_tree_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("CYBION_TEST_TREE_ROOT", root.path())
            .env("CYBION_TEST_TREE_DEPTH", "0");
        if parent_exits {
            command.env("CYBION_TEST_PARENT_EXITS", "1");
        }
        let error = output(command, Duration::from_secs(3), "test command")
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "test command timed out");
        let pids: Vec<_> = (0..3)
            .map(|depth| {
                let pid =
                    std::fs::read_to_string(root.path().join(format!("{depth}.pid"))).unwrap();
                Pid::from_u32(pid.parse().unwrap())
            })
            .collect();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let system = System::new_all();
                if pids.iter().all(|pid| {
                    system
                        .process(*pid)
                        .is_none_or(|p| p.status() == ProcessStatus::Zombie)
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("timed-out command left a running process behind");
    }

    #[tokio::test]
    async fn timeout_terminates_shell_and_descendants() {
        check_timeout_tree(false).await;
    }

    #[tokio::test]
    async fn timeout_terminates_descendants_after_parent_exit() {
        check_timeout_tree(true).await;
    }
}

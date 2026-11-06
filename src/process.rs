use std::{
    process::{Output, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use process_wrap::tokio::{CommandWrap, KillOnDrop};
use tokio::{io::AsyncReadExt, process::Command, sync::watch};

/// Windows flags for `CreateProcess`. The Worker itself is started detached
/// and owns no console, so a console-subsystem child would get a brand-new
/// console window allocated — the black window that flashes on screen for
/// every scheduled or interactive call. Ask for a windowless console instead.
#[cfg(windows)]
pub const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// Starts the Worker detached from any console it was launched from.
#[cfg(windows)]
pub const DETACHED_PROCESS: u32 = 0x0000_0008;

/// Hides the console of a child that is spawned directly, without a
/// `CommandWrap`. Wrapped children need the wrapper instead: `JobObject`
/// re-applies creation flags from `CreationFlags` and would otherwise drop
/// this one on the floor.
#[cfg(windows)]
pub fn hide_console(command: &mut Command) {
    command.creation_flags(CREATE_NO_WINDOW);
}

/// Wraps a child the same way every tool call does: kill-on-drop, its own
/// process group or job object, and — on Windows — a windowless console.
fn wrapped(command: Command) -> CommandWrap {
    let mut command = CommandWrap::from(command);
    command.wrap(KillOnDrop);
    #[cfg(unix)]
    command.wrap(process_wrap::tokio::ProcessGroup::leader());
    #[cfg(windows)]
    {
        // Order does not matter: each wrapper consults the other. `JobObject`
        // temporarily adds CREATE_SUSPENDED and resumes after assignment.
        command.wrap(process_wrap::tokio::JobObject);
        command.wrap(process_wrap::tokio::CreationFlags(
            windows::Win32::System::Threading::PROCESS_CREATION_FLAGS(CREATE_NO_WINDOW),
        ));
    }
    command
}

pub async fn output(
    mut command: Command,
    timeout: Duration,
    label: &str,
    cancel: &mut watch::Receiver<bool>,
) -> Result<Output> {
    if *cancel.borrow() {
        bail!("{label} cancelled by the Controller");
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut command = wrapped(command);
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
    let status = tokio::select! {
        result = tokio::time::timeout(timeout, async {
            tokio::try_join!(
                child.wait(),
                stdout_pipe.read_to_end(&mut stdout),
                stderr_pipe.read_to_end(&mut stderr),
            )
        }) => result
            .with_context(|| format!("{label} timed out"))
            .and_then(|result| result.map(|(status, _, _)| status).map_err(Into::into)),
        _ = cancel.changed() => Err(anyhow::anyhow!("{label} cancelled by the Controller")),
    };
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

    // The Worker runs detached, so Windows hands every child its own console
    // window unless the child is asked for a windowless console. The request
    // has to travel through process-wrap's `CreationFlags`: `JobObject`
    // re-applies creation flags and silently drops anything set directly on
    // the command. That regression is the black window users see on every call.
    #[cfg(windows)]
    #[test]
    fn windows_tool_children_ask_for_a_windowless_console() {
        let command = wrapped(Command::new("cmd"));
        let flags = command
            .get_wrap::<process_wrap::tokio::CreationFlags>()
            .expect("tool children must register CreationFlags next to JobObject");
        assert_ne!(
            flags.0.0 & CREATE_NO_WINDOW,
            0,
            "tool children must keep CREATE_NO_WINDOW through process-wrap"
        );
    }

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
        let (_cancel, mut receiver) = watch::channel(false);
        let error = output(
            command,
            Duration::from_secs(3),
            "test command",
            &mut receiver,
        )
        .await
        .unwrap_err();
        assert_eq!(error.to_string(), "test command timed out");
        assert!(!*receiver.borrow());
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

    #[tokio::test]
    async fn cancellation_terminates_shell_and_descendants() {
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
        let (sender, mut receiver) = watch::channel(false);
        let running = tokio::spawn(async move {
            output(
                command,
                Duration::from_secs(60),
                "test command",
                &mut receiver,
            )
            .await
            .map(|_| ())
        });
        let pids: Vec<Pid> = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let pids: Vec<_> = (0..3)
                    .filter_map(|depth| {
                        std::fs::read_to_string(root.path().join(format!("{depth}.pid")))
                            .ok()
                            .and_then(|pid| pid.parse().ok())
                            .map(Pid::from_u32)
                    })
                    .collect();
                if pids.len() == 3 {
                    break pids;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("process tree did not start");
        sender.send(true).unwrap();
        let error = running.await.unwrap().unwrap_err();
        assert_eq!(
            error.to_string(),
            "test command cancelled by the Controller"
        );
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
        .expect("cancelled command left a running process behind");
    }
}

# Cybion Worker

A single, SQLite-free execution binary for hosted Cybion. It receives Bash,
Browser Control and Computer Use calls over an outbound SSE connection and
returns results over HTTPS. Run it as the OS account whose files and desktop
you intend Cybion to access; it is not a sandbox or per-command approval system.

## Connect a device

1. Open **Workers → Connect a device** at `https://cybion.ntnl.io`.
2. Choose the **target** OS/architecture, download and extract the recommended
   release. Windows also provides a ZIP archive. Checksums accompany every asset.
3. Open a terminal in the extracted directory and run:

   ```sh
   ./cybion-worker run --background
   ```

   On Windows PowerShell: `.\cybion-worker.exe run --background`.

4. With no configuration, Worker prints a 12-character pairing code and opens
   browser authorization. On a headless server, open the printed address on
   another device. Check the code, device details and signed-in account before
   authorizing. Never approve a code someone else sends you.
5. Cybion saves the configuration on the device automatically. The web guide
   tests task delivery, a fixed `echo` command and result upload before marking
   command execution ready. Browser presence is **not** proof of browser control;
   desktop permissions are reported as unchecked, never silently granted.

Running the binary without arguments follows the same initialization flow and
then stays in the foreground. Existing valid configuration is reused. Interrupted
setup resumes from a private pending file; expiry/rejection gives an actionable
retry. Pairing codes expire after ten minutes. Approval grants another ten-minute
claim window, including when approval arrives near the original deadline.

## Lifecycle and diagnostics

```sh
cybion-worker run                         # foreground, suitable for a service manager
cybion-worker run --background            # wait for actual task-channel startup
cybion-worker status                      # config path, local lock, last connection
cybion-worker doctor                      # authentication, shell, local dependencies
cybion-worker config-path
cybion-worker --help
```

`run`, `status` and `doctor` accept `--config PATH`. One process owns each config;
starting a duplicate is rejected. Background startup reports an early process failure. If connection takes longer
than 15 seconds, the process remains alive and the CLI reports that it is still
connecting, not that it is ready.
Logs are in `worker.log` beside the config. `status`'s last connection timestamp
is historical, not a live health guarantee. The web connection check validates
the full task round trip. Neither command prints the access token.

**Background mode is not automatic startup.** This release does not install an
OS service. Restart after reboot, or explicitly configure your OS service manager.
Desktop automation needs the user's interactive session, not an arbitrary system
service account.

Removing a device in Cybion revokes future access; a v0.1.4 Worker exits after
its credential is rejected. Removal does not undo or terminate commands that
already started. To re-pair, stop the old process, remove the old device in
Cybion, and move `worker.toml` to a safe location before restarting. Do not share
configuration or pending pairing files.

## Manual configuration

The web guide's advanced section retains manual pairing. Save `worker.toml` to
`~/.cybion/worker.toml` (Windows: `%USERPROFILE%\.cybion\worker.toml`):

```toml
controller_url = "https://cybion.ntnl.io"
user_id = "..."
machine_id = "..."
access_token = "..."
```

Auto-setup uses atomic, no-clobber writes with owner-only permissions on Unix;
Windows uses the current user's directory ACL. A malformed existing config is
reported, never overwritten. Worker generates the long-lived credential locally
and submits only its hash; neither browser nor pairing store sees plaintext.
The device secret used to poll pairing is separate from that credential.

## Platforms and capability limits

Releases target macOS arm64/x86_64, Linux x86_64/aarch64 and Windows x86_64.
Browser Control discovers Chrome, Chromium or Edge. Desktop Control depends on
OS permissions and an interactive desktop. Linux without an X11 display is
reported as not applicable; macOS Accessibility/Automation and Windows desktop
permissions must be configured explicitly. Diagnostics never click/type on the
user's current desktop or launch a browser to test it.

## Reliability and remote upgrades (0.2.0)

Each process reports its version and a new in-memory boot ID. Reconnecting keeps
that boot ID and the call-ID deduplication map. Completed results stay in memory
and retry upload on transport failures, HTTP 408/429 and 5xx. Acknowledged results
release their payload; call IDs remain remembered for the process lifetime.
The Worker also posts a delivery receipt for every received call, duplicates
included, and retries it like a result; the Controller records the receipt and
stops replaying calls this process already acknowledged. Receipt posts are
in-flight work, so remote upgrades wait for them before replacing the executable.
There is no task database, journal or disk outbox. Restarting Worker may lose
results; the Controller must not replay old-process commands in the new process.

From 0.2.0 onward, the owning user can request the Controller's recommended
Worker release from the device page. The Controller pauses new task delivery
and waits for delivered work; Worker also waits for executing/uploading tasks.
Worker accepts only a newer numeric release version from the official
`zccz14/cybion-worker` repository, verifies the SHA-256 manifest and executable
version, replaces its executable and restarts with the same configuration.
The old executable is retained as `cybion-worker.previous`; an early replacement
startup failure restores it. Upgrades require write access to the executable
directory. Download, checksum or preflight failures retain the current Worker.
Installation archives and the executable backup are not task persistence.

A 0.1.x Worker has no remote-upgrade handler. Install 0.2.0 manually once;
subsequent upgrades can be requested through Controller. No arbitrary download
URL or model-invokable upgrade tool is accepted.

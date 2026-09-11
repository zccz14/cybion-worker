# Cybion Worker

`cybion-worker` is the independent, SQLite-free execution binary for hosted
Cybion. It receives Bash, Browser Control, and Computer Use calls through an
outbound SSE connection, then returns results through HTTPS.

Create a pairing from `https://cybion.ntnl.io`, then save the returned config as
`~/.cybion/worker.toml`:

```toml
controller_url = "https://cybion.ntnl.io"
tenant_id = "..."
machine_id = "..."
access_token = "..."
```

Run in the foreground (appropriate for a service manager):

```sh
cybion-worker run
```

Or start a detached background process with logs at `~/.cybion/worker.log`:

```sh
cybion-worker run --background
```

Release assets are published for macOS arm64/x86_64, Linux x86_64/aarch64, and
Windows x86_64. Browser Control discovers Chrome, Chromium, or Edge on each
platform. Computer Use relies on the operating system's accessibility permission
and the native desktop automation facility available on the device.

# Scrutiny Server in Rust

This is a Rust implementation of a simple RPV (Runtime Published Values) Scrutiny Server.  

Scrutiny is a GUI tool for tuning and observing data on a target system.  
The server collects data from individual sources and serves the API to the Scrutiny GUI, which visualizes the collected data. The server also allows to modify data on the target system, which is useful for tuning and testing purposes. 

For more information about Scrutiny, please visit the official Scrutiny GitHub repository:
https://github.com/scrutinydebugger/scrutiny-main

and

https://scrutinydebugger.com



Run the server example application sine_source:

cargo run --example sine_source


Run the Scrutiny GUI:

```
scrutiny gui --auto-connect
```

---

## Using scrutiny-server-rs as a library

### Add the dependency

In your `Cargo.toml`:

```toml
[dependencies]
scrutiny-server-rs = { path = "../scrutiny-server-rs" }
tokio = { version = "1", features = ["full"] }
```

### Implement `DataSource`

The entire integration point is the `DataSource` trait. Implement it on a struct that holds your application state:

```rust
use std::collections::HashSet;
use scrutiny_server_rs::{
    ConnectionStatus, DataSource, DataType, PollResult,
    WatchableDefinition, WatchableKind, WatchableUpdate, WatchableValue,
};

struct MySource {
    temperature: f64,
    setpoint: f64,
}

impl DataSource for MySource {
    /// Called once at startup. Return every watchable your source exposes.
    /// Use WatchableKind::Rpv so values appear in the GUI without a loaded
    /// firmware description file (.sfd).
    fn watchables(&self) -> Vec<WatchableDefinition> {
        vec![
            WatchableDefinition {
                path: "/sensors/temperature".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
            },
            WatchableDefinition {
                path: "/control/setpoint".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
            },
        ]
    }

    /// Called periodically (every poll_interval_ms). Return only changed values.
    /// `subscribed` contains the paths the GUI is currently watching — you may
    /// skip computing values for paths that are absent to save CPU.
    fn poll(&mut self, server_time_us: f64, subscribed: &HashSet<String>) -> PollResult {
        let mut updates = Vec::new();

        if subscribed.contains("/sensors/temperature") {
            updates.push(WatchableUpdate {
                path: "/sensors/temperature".into(),
                value: WatchableValue::Float(self.temperature),
            });
        }

        PollResult { timestamp_us: server_time_us, updates }
    }

    /// Called when the GUI writes to a watchable. Return Err for read-only paths.
    fn write(&mut self, path: &str, value: WatchableValue) -> Result<(), String> {
        if path == "/control/setpoint" {
            if let WatchableValue::Float(v) = value {
                self.setpoint = v;
                return Ok(());
            }
        }
        Err(format!("unknown or read-only path: {path}"))
    }
}
```

### Start the server

`run()` is an async function that blocks until the shutdown signal fires.
Pass `std::future::pending()` to run until the process exits, or a
`tokio::sync::oneshot` receiver to support programmatic shutdown:

```rust
use scrutiny_server_rs::run;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let source = MySource { temperature: 20.0, setpoint: 25.0 };

    // Run until Ctrl-C / process exit — no programmatic shutdown needed.
    run(source, "0.0.0.0:8765", 100, std::future::pending()).await
}
```

### Programmatic start and stop

Wrap `run()` in `tokio::spawn` and send on a oneshot channel to stop it:

```rust
use scrutiny_server_rs::run;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let source = MySource { temperature: 20.0, setpoint: 25.0 };
    let server = tokio::spawn(
        run(source, "0.0.0.0:8765", 100, async { let _ = shutdown_rx.await; })
    );

    // … do other work …

    // Stop the server gracefully.
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}
```

`run()` signature:

```rust
pub async fn run<DS: DataSource>(
    datasource: DS,
    addr: impl Into<String>,   // e.g. "0.0.0.0:8765"
    poll_interval_ms: u64,     // how often poll() is called, e.g. 100 = 10 Hz
    shutdown: impl Future<Output = ()> + Send,
) -> anyhow::Result<()>
```

---

## DataSource trait reference

| Method | When called | Notes |
|---|---|---|
| `watchables() -> Vec<WatchableDefinition>` | Once at startup | Defines the full list of observable values. Wire IDs are assigned automatically. |
| `poll(server_time_us, subscribed) -> PollResult` | Every `poll_interval_ms` | Return only values that changed. Skip paths absent from `subscribed` to save work. |
| `write(path, value) -> Result<(), String>` | On GUI write request | Return `Err` for read-only paths. |
| `connection_status() -> ConnectionStatus` | Every poll tick | Drives the "Device" and "Link" indicators in the GUI status bar. Default: `Connected`. |
| `on_subscribed(paths: &[&str])` | Before `poll()` when new paths are subscribed | Use to start data acquisition for those paths. |
| `on_unsubscribed(paths: &[&str])` | Before `poll()` when paths are unsubscribed | Use to stop data acquisition for those paths. |

### Types

**`WatchableDefinition`** — describes one observable value:
- `path: String` — hierarchical path shown in the GUI, e.g. `/sensors/temp`. Acts as the unique key.
- `dtype: DataType` — one of `Sint8/16/32/64`, `Uint8/16/32/64`, `Float32`, `Float64`, `Boolean`.
- `kind: WatchableKind` — `Rpv` (recommended; visible without an SFD file), `Var`, or `Alias`.

**`WatchableValue`** — the runtime value of a watchable:
- `Float(f64)` · `Int(i64)` · `Uint(u64)` · `Bool(bool)`

**`PollResult`**:
- `timestamp_us: f64` — microseconds since server start. Becomes the time-axis value in GUI charts. Use `server_time_us` directly, or back-date by your actual sample latency.
- `updates: Vec<WatchableUpdate>` — the values that changed this tick. Returning an empty `Vec` is valid and sends no update.

**`ConnectionStatus`**:
- `Connected` — green indicator in the GUI.
- `Connecting` — yellow device / red link indicator.
- `Disconnected` — red indicators.



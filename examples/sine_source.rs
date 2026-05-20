/// Example: sine-wave datasource
///
/// Run with:
///   cargo run --example sine_source -- [host:port]
///
/// Then point the Scrutiny GUI at the same port.
use std::f64::consts::TAU;
use std::time::Instant;

use scrutiny_server_rs::{
    run, ConnectionStatus, DataSource, DataType, PollResult, WatchableDefinition, WatchableKind,
    WatchableUpdate, WatchableValue,
};

struct SineSource {
    start: Instant,
    amplitude: f64, // Tunable amplitude
    poll_counter: u32,
}

impl SineSource {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            amplitude: 1.0,
            poll_counter: 0,
        }
    }
}

impl DataSource for SineSource {
    fn watchables(&self) -> Vec<WatchableDefinition> {
        // Use WatchableKind::Rpv so entries appear in the GUI without needing
        // a loaded SFD firmware description file.
        // The server automatically assigns the 16-bit wire IDs — no need to specify them.
        vec![
            WatchableDefinition {
                path: "/signals/sine".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
            },
            WatchableDefinition {
                path: "/signals/cosine".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
            },
            WatchableDefinition {
                path: "/params/amplitude".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
            },
            WatchableDefinition {
                path: "/meta/server_time".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
            },
            WatchableDefinition {
                path: "/meta/poll_time".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
            },
            WatchableDefinition {
                path: "/meta/poll_counter".into(),
                dtype: DataType::Uint32,
                kind: WatchableKind::Rpv,
            },
        ]
    }

    fn poll(&mut self, server_time_us: f64, subscribed: &std::collections::HashSet<String>) -> PollResult {
        let t = self.start.elapsed().as_secs_f64();
        // Use the datasource's own elapsed time (sub-millisecond precision) as
        // the timestamp so the graph x-axis reflects actual sample timing.
        let timestamp_us = t * 1_000_000.0;
        let mut updates = Vec::new();

        if subscribed.contains("/signals/sine") {
            updates.push(WatchableUpdate {
                path: "/signals/sine".into(),
                value: WatchableValue::Float(self.amplitude * (TAU * 1.0 * t).sin()),
            });
        }

        if subscribed.contains("/signals/cosine") && self.poll_counter % 2 == 0 {
            updates.push(WatchableUpdate {
                path: "/signals/cosine".into(),
                value: WatchableValue::Float(self.amplitude * (TAU * 1.0 * t).cos()),
            });
        }
        if subscribed.contains("/params/amplitude") && self.poll_counter % 10 == 0 {
            updates.push(WatchableUpdate {
                path: "/params/amplitude".into(),
                value: WatchableValue::Float(self.amplitude),
            });
        }

        if subscribed.contains("/meta/server_time") {
            updates.push(WatchableUpdate {
                path: "/meta/server_time".into(),
                value: WatchableValue::Float(server_time_us / 1_000_000.0), // convert µs → s
            });
        }

        if subscribed.contains("/meta/poll_time") {
            updates.push(WatchableUpdate {
                path: "/meta/poll_time".into(),
                value: WatchableValue::Float(t),
            });
        }

        if subscribed.contains("/meta/poll_counter") {
            updates.push(WatchableUpdate {
                path: "/meta/poll_counter".into(),
                value: WatchableValue::Uint(self.poll_counter as u64),
            });
        }

        self.poll_counter += 1;

        PollResult {
            timestamp_us,
            updates,
        }
    }

    fn write(&mut self, path: &str, value: WatchableValue) -> Result<(), String> {
        if path == "/params/amplitude" {
            match value {
                WatchableValue::Float(f) => {
                    self.amplitude = f;
                    log::info!("Amplitude set to {f}");
                    Ok(())
                }
                WatchableValue::Int(i) => {
                    self.amplitude = i as f64;
                    log::info!("Amplitude set to {}", self.amplitude);
                    Ok(())
                }
                _ => Err("amplitude expects a numeric value".into()),
            }
        } else {
            Err(format!("Read-only watchable: {path}"))
        }
    }

    fn connection_status(&self) -> ConnectionStatus {
        ConnectionStatus::Connected
        //    ConnectionStatus::Connecting
        //    ConnectionStatus::Disconnected
    }

    fn on_subscribed(&mut self, paths: &[&str]) {
        log::info!("GUI started watching: {:?}", paths);
    }

    fn on_unsubscribed(&mut self, paths: &[&str]) {
        log::info!("GUI stopped watching: {:?}", paths);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    // --log-level <level>  (overrides RUST_LOG when given)
    let log_level_arg = args.windows(2)
        .find(|w| w[0] == "--log-level")
        .and_then(|w| w[1].parse::<log::LevelFilter>().ok());
    match log_level_arg {
        Some(level) => env_logger::Builder::new().filter_level(level).init(),
        None => env_logger::Builder::from_env(
            env_logger::Env::default().default_filter_or("info")
        ).init(),
    }

    // First positional arg (skipping flag pairs) is the bind address.
    let addr = {
        let mut skip = false;
        args.iter()
            .find(|a| {
                if skip { skip = false; return false; }
                if a.as_str() == "--log-level" { skip = true; return false; }
                !a.starts_with('-')
            })
            .cloned()
            .unwrap_or_else(|| "0.0.0.0:8765".into())
    };

    // ── Start the server as a background task ────────────────────────────────
    // `shutdown_tx` / `shutdown_rx` form a one-shot channel: sending on
    // `shutdown_tx` tells the server to stop accepting connections, close the
    // active client (if any), abort the poller, and return.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    log::info!("Starting sine-wave datasource on {addr}");
    let server = tokio::spawn(
        run(SineSource::new(), addr, 20, async { let _ = shutdown_rx.await; })
    );

    // ── Wait for Ctrl-C, then stop the server gracefully ─────────────────────
    tokio::signal::ctrl_c().await?;
    log::info!("Ctrl-C received — stopping server…");

    let _ = shutdown_tx.send(());
    server.await??;
    log::info!("Goodbye.");
    Ok(())
}

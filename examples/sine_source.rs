/// Example: sine-wave datasource
///
/// Run with:
///   cargo run --example sine_source -- [host:port]
///
/// Then point the Scrutiny GUI at the same port.
use std::f64::consts::TAU;
use std::time::Instant;

use scrutiny_server_rs::{
    run, DataSource, DataType, WatchableDefinition, WatchableKind, WatchableUpdate, WatchableValue,
};

struct SineSource {
    start: Instant,
    /// Overridable amplitude for "sine_a".
    amplitude: f64,
}

impl SineSource {
    fn new() -> Self {
        Self { start: Instant::now(), amplitude: 1.0 }
    }
}

impl DataSource for SineSource {
    fn watchables(&self) -> Vec<WatchableDefinition> {
        // Use WatchableKind::Rpv so entries appear in the GUI without needing
        // a loaded SFD firmware description file. RPV IDs must be unique u16 values.
        vec![
            WatchableDefinition {
                id: "waves/sine_1hz".into(),
                path: "/waves/sine_1hz".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
                rpv_id: 0x0001,
            },
            WatchableDefinition {
                id: "waves/sine_5hz".into(),
                path: "/waves/sine_5hz".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
                rpv_id: 0x0002,
            },
            WatchableDefinition {
                id: "waves/cosine_1hz".into(),
                path: "/waves/cosine_1hz".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
                rpv_id: 0x0003,
            },
            WatchableDefinition {
                id: "config/amplitude".into(),
                path: "/config/amplitude".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
                rpv_id: 0x0004,
            },
            WatchableDefinition {
                id: "meta/elapsed_s".into(),
                path: "/meta/elapsed_s".into(),
                dtype: DataType::Float64,
                kind: WatchableKind::Rpv,
                rpv_id: 0x0005,
            },
        ]
    }

    fn poll(&mut self) -> Vec<WatchableUpdate> {
        let t = self.start.elapsed().as_secs_f64();
        vec![
            WatchableUpdate {
                id: "waves/sine_1hz".into(),
                value: WatchableValue::Float(self.amplitude * (TAU * 1.0 * t).sin()),
            },
            WatchableUpdate {
                id: "waves/sine_5hz".into(),
                value: WatchableValue::Float(self.amplitude * (TAU * 5.0 * t).sin()),
            },
            WatchableUpdate {
                id: "waves/cosine_1hz".into(),
                value: WatchableValue::Float(self.amplitude * (TAU * 1.0 * t).cos()),
            },
            WatchableUpdate {
                id: "config/amplitude".into(),
                value: WatchableValue::Float(self.amplitude),
            },
            WatchableUpdate {
                id: "meta/elapsed_s".into(),
                value: WatchableValue::Float(t),
            },
        ]
    }

    fn write(&mut self, id: &str, value: WatchableValue) -> Result<(), String> {
        if id == "config/amplitude" {
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
            Err(format!("Read-only watchable: {id}"))
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let addr = std::env::args().nth(1).unwrap_or_else(|| "0.0.0.0:8765".into());
    log::info!("Starting sine-wave datasource on {addr}");

    run(SineSource::new(), &addr).await
}

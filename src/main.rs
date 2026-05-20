use scrutiny_server_rs::{
    run, ConnectionStatus, DataSource, DataType, PollResult, WatchableDefinition, WatchableKind,
    WatchableUpdate, WatchableValue,
};

/// A minimal built-in demo datasource that exposes a single counter variable.
/// Replace this with your own implementation of `DataSource`.
struct DemoSource {
    counter: i64,
}

impl DataSource for DemoSource {
    fn watchables(&self) -> Vec<WatchableDefinition> {
        vec![WatchableDefinition {
            path: "/demo/counter".into(),
            dtype: DataType::Sint64,
            kind: WatchableKind::Rpv,
        }]
    }

    fn poll(&mut self, server_time_us: f64) -> PollResult {
        self.counter += 1;
        PollResult {
            timestamp_us: server_time_us,
            updates: vec![WatchableUpdate {
                path: "/demo/counter".into(),
                value: WatchableValue::Int(self.counter),
            }],
        }
    }

    fn write(&mut self, path: &str, value: WatchableValue) -> Result<(), String> {
        if path == "/demo/counter" {
            if let WatchableValue::Int(v) = value {
                self.counter = v;
                return Ok(());
            }
        }
        Err(format!("Unknown path or wrong type: {path}"))
    }

    fn connection_status(&self) -> ConnectionStatus {
        ConnectionStatus::Connected
        //    ConnectionStatus::Connecting
        //    ConnectionStatus::Disconnected
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

    let source = DemoSource { counter: 0 };
    run(source, &addr, 100).await
}

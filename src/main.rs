use scrutiny_server_rs::{run, DataSource, DataType, WatchableDefinition, WatchableKind, WatchableUpdate, WatchableValue};

/// A minimal built-in demo datasource that exposes a single counter variable.
/// Replace this with your own implementation of `DataSource`.
struct DemoSource {
    counter: i64,
}

impl DataSource for DemoSource {
    fn watchables(&self) -> Vec<WatchableDefinition> {
        vec![
            WatchableDefinition {
                id: "demo/counter".into(),
                path: "/demo/counter".into(),
                dtype: DataType::Sint64,
                kind: WatchableKind::Rpv,
                rpv_id: 0x0001,
            },
        ]
    }

    fn poll(&mut self) -> Vec<WatchableUpdate> {
        self.counter += 1;
        vec![WatchableUpdate {
            id: "demo/counter".into(),
            value: WatchableValue::Int(self.counter),
        }]
    }

    fn write(&mut self, id: &str, value: WatchableValue) -> Result<(), String> {
        if id == "demo/counter" {
            if let WatchableValue::Int(v) = value {
                self.counter = v;
                return Ok(());
            }
        }
        Err(format!("Unknown id or wrong type: {id}"))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let addr = std::env::args().nth(1).unwrap_or_else(|| "0.0.0.0:8765".into());
    let source = DemoSource { counter: 0 };

    run(source, &addr).await
}

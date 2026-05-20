/// A watchable data type, matching the Scrutiny API type strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Sint8,
    Sint16,
    Sint32,
    Sint64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Float32,
    Float64,
    Boolean,
}

impl DataType {
    pub fn as_api_str(&self) -> &'static str {
        match self {
            DataType::Sint8 => "sint8",
            DataType::Sint16 => "sint16",
            DataType::Sint32 => "sint32",
            DataType::Sint64 => "sint64",
            DataType::Uint8 => "uint8",
            DataType::Uint16 => "uint16",
            DataType::Uint32 => "uint32",
            DataType::Uint64 => "uint64",
            DataType::Float32 => "float32",
            DataType::Float64 => "float64",
            DataType::Boolean => "boolean",
        }
    }
}

/// Which Scrutiny watchable kind this entry is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchableKind {
    /// Regular variable (maps to "var" in the API).
    Var,
    /// Runtime Published Value (maps to "rpv" in the API).
    Rpv,
    /// Alias of another watchable (maps to "alias" in the API).
    Alias,
}

impl WatchableKind {
    pub fn as_api_str(&self) -> &'static str {
        match self {
            WatchableKind::Var => "var",
            WatchableKind::Rpv => "rpv",
            WatchableKind::Alias => "alias",
        }
    }
}

/// Metadata describing a single watchable exposed by a datasource.
///
/// The 16-bit RPV wire ID required by the Scrutiny protocol is assigned
/// automatically by the server at startup — users do not need to manage it.
#[derive(Debug, Clone)]
pub struct WatchableDefinition {
    /// Hierarchical display path shown in the UI, e.g. `/sensors/temperature`.
    /// This is the unique key for the watchable — also used in subscriptions
    /// and write requests.
    pub path: String,
    /// Numeric data type of this watchable.
    pub dtype: DataType,
    /// Which Scrutiny kind this maps to.
    pub kind: WatchableKind,
}

/// A single value update emitted by a datasource on each poll.
#[derive(Debug, Clone)]
pub struct WatchableUpdate {
    pub path: String,
    pub value: WatchableValue,
}

/// Returned by [`DataSource::poll`].
///
/// `timestamp_us` is the server-relative timestamp (microseconds since server
/// start) that will be attached to every update in this batch on the wire.
/// The server passes its current time as `server_time_us` into `poll()` — use
/// that value directly if you have no better source, or substitute a precise
/// hardware / sample timestamp expressed in the same microsecond scale:
///
/// ```
/// // Use the server clock as-is (default behaviour):
/// PollResult { timestamp_us: server_time_us, updates: vec![...] }
///
/// // Back-date by 5 ms to reflect actual sample acquisition time:
/// PollResult { timestamp_us: server_time_us - 5_000.0, updates: vec![...] }
/// ```
#[derive(Debug)]
pub struct PollResult {
    /// Timestamp in microseconds since server start. Becomes the `t` field in
    /// every `watchable_update` message sent for this batch.
    pub timestamp_us: f64,
    /// The watchable values that changed since the last poll. May be empty.
    pub updates: Vec<WatchableUpdate>,
}

/// The runtime value of a watchable.
#[derive(Debug, Clone)]
pub enum WatchableValue {
    Float(f64),
    Int(i64),
    Uint(u64),
    Bool(bool),
}

impl WatchableValue {
    /// Convert to the JSON value sent in `watchable_update.updates[].v`.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            WatchableValue::Float(f) => serde_json::Value::from(*f),
            WatchableValue::Int(i) => serde_json::Value::from(*i),
            WatchableValue::Uint(u) => serde_json::Value::from(*u),
            WatchableValue::Bool(b) => serde_json::Value::Bool(*b),
        }
    }
}

/// A write request forwarded from a connected client to the datasource.
#[derive(Debug)]
pub struct WriteCommand {
    pub path: String,
    pub value: WatchableValue,
    /// Sent back in `inform_write_completion`.
    pub request_token: String,
    pub batch_index: i64,
}

/// Connection state reported by a datasource to the GUI status bar.
///
/// Maps to the "Device:" and "Link:" indicators in the Scrutiny GUI:
///
/// | Variant        | Device label (color)       | Link label (color) |
/// |----------------|----------------------------|--------------------|
/// | `Connected`    | "Device: Connected" (green)  | green              |
/// | `Connecting`   | "Device: Connecting" (yellow)| red                |
/// | `Disconnected` | "Device: Disconnected" (red) | red                |
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// Datasource has an active connection to its data source.
    Connected,
    /// Datasource is in the process of establishing a connection.
    Connecting,
    /// Datasource has no connection to its data source.
    Disconnected,
}

/// The trait datasource implementors must fulfill.
///
/// # Contract
/// - `watchables()` is called once at startup to populate the datastore.
/// - `poll(server_time_us)` is called periodically. Return a [`PollResult`]
///   whose `timestamp_us` will be used as the time-axis value for every update
///   in this batch. Use `server_time_us` directly if you have no better source.
///   Return only values that changed since the last poll to reduce traffic.
/// - `write()` is called when a client writes a value. Return `Err` with a
///   human-readable message if the write is rejected.
/// - `connection_status()` is polled periodically and drives the "Device:" and
///   "Link:" indicators in the GUI status bar. The default is `Connected`.
pub trait DataSource: Send + 'static {
    /// Return the full list of watchables this source exposes.
    fn watchables(&self) -> Vec<WatchableDefinition>;

    /// Return changed values since the last call.
    /// `server_time_us` is the current server clock in microseconds since
    /// server start — use it as `timestamp_us` in the returned [`PollResult`]
    /// unless you have a more precise sample timestamp.
    fn poll(&mut self, server_time_us: f64) -> PollResult;

    /// Apply a write to the datasource. `path` is the watchable's path as
    /// returned from `watchables()`.
    fn write(&mut self, path: &str, value: WatchableValue) -> Result<(), String>;

    /// Return the current connection state of this datasource.
    /// Called periodically; the result drives the GUI status bar indicators.
    /// Default: always `Connected`.
    fn connection_status(&self) -> ConnectionStatus {
        ConnectionStatus::Connected
    }
}

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
#[derive(Debug, Clone)]
pub struct WatchableDefinition {
    /// Unique ID (used internally and as the subscription key).
    pub id: String,
    /// Display path shown in the UI, e.g. `/sensors/temperature`.
    pub path: String,
    /// Numeric data type of this watchable.
    pub dtype: DataType,
    /// Which Scrutiny kind this maps to.
    pub kind: WatchableKind,
    /// For `WatchableKind::Rpv` entries: the 16-bit RPV identifier sent to the
    /// client in the detailed definition. Must be unique within a datasource.
    /// Ignored for non-RPV entries.
    pub rpv_id: u32,
}

/// A single value update emitted by a datasource on each poll.
#[derive(Debug, Clone)]
pub struct WatchableUpdate {
    pub id: String,
    pub value: WatchableValue,
}

/// The runtime value of a watchable.
#[derive(Debug, Clone)]
pub enum WatchableValue {
    Float(f64),
    Int(i64),
    Bool(bool),
}

impl WatchableValue {
    /// Convert to the JSON value sent in `watchable_update.updates[].v`.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            WatchableValue::Float(f) => serde_json::Value::from(*f),
            WatchableValue::Int(i) => serde_json::Value::from(*i),
            WatchableValue::Bool(b) => serde_json::Value::Bool(*b),
        }
    }
}

/// A write request forwarded from a connected client to the datasource.
#[derive(Debug)]
pub struct WriteCommand {
    pub id: String,
    pub value: WatchableValue,
    /// Sent back in `inform_write_completion`.
    pub request_token: String,
    pub batch_index: i64,
}

/// The trait datasource implementors must fulfill.
///
/// # Contract
/// - `watchables()` is called once at startup to populate the datastore.
/// - `poll()` is called periodically (every ~100 ms) to get updated values.
///   Return only values that have changed since the last poll to reduce traffic.
/// - `write()` is called when a client writes a value. Return `Err` with a
///   human-readable message if the write is rejected.
pub trait DataSource: Send + 'static {
    /// Return the full list of watchables this source exposes.
    fn watchables(&self) -> Vec<WatchableDefinition>;

    /// Return changed values since the last call. May return all values on the
    /// first call to populate clients immediately after subscription.
    fn poll(&mut self) -> Vec<WatchableUpdate>;

    /// Apply a write to the datasource.
    fn write(&mut self, id: &str, value: WatchableValue) -> Result<(), String>;
}

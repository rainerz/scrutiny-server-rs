/// JSON message types exchanged with Scrutiny clients.
///
/// These structs map 1-to-1 onto the Python `api_typing.py` TypedDicts.
/// `serde_json::Value` is used for fields whose shape is dynamic or not
/// needed at the Rust side (e.g. `link_config`).
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ── shared base ──────────────────────────────────────────────────────────────

/// Every client→server message has at minimum these two fields.
#[derive(Debug, Deserialize)]
pub struct BaseC2S {
    pub cmd: String,
    pub reqid: Option<i64>,
}

// ── client → server (C2S) ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct C2sEcho {
    pub reqid: Option<i64>,
    pub payload: String,
}

#[derive(Debug, Deserialize)]
pub struct C2sGetWatchableList {
    pub reqid: Option<i64>,
    pub max_per_response: Option<usize>,
    pub filter: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct C2sGetWatchableCount {
    pub reqid: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct C2sGetWatchableInfo {
    pub reqid: Option<i64>,
    pub watchables: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct C2sSubscribeWatchable {
    pub reqid: Option<i64>,
    pub watchables: Vec<String>,
    pub rate: Option<Value>, // ignored – we don't throttle
}

#[derive(Debug, Deserialize)]
pub struct C2sUnsubscribeWatchable {
    pub reqid: Option<i64>,
    pub watchables: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct C2sChangeSubscriptionUpdateRate {
    pub reqid: Option<i64>,
    pub changes: Vec<SubscriptionRateChange>,
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionRateChange {
    pub id: String,
    pub rate: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct C2sSetLinkConfig {
    pub reqid: Option<i64>,
    pub link_type: String,
    pub link_config: Value,
}

#[derive(Debug, Deserialize)]
pub struct C2sWriteWatchable {
    pub reqid: Option<i64>,
    pub updates: Vec<WriteUpdate>,
}

#[derive(Debug, Deserialize)]
pub struct WriteUpdate {
    pub batch_index: i64,
    pub watchable: String, // watchable ID
    pub value: Value,
}

#[derive(Debug, Deserialize)]
pub struct C2sWriteSingleWatchable {
    pub reqid: Option<i64>,
    pub server_path: String,
    pub value: Value,
}

// ── server → client (S2C) ───────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct S2cWelcome {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub server_time_zero_timestamp: f64,
}

#[derive(Debug, Serialize)]
pub struct S2cError {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub request_cmd: String,
    pub msg: String,
}

#[derive(Debug, Serialize)]
pub struct S2cEcho {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub payload: String,
}

#[derive(Debug, Serialize)]
pub struct S2cEmpty {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct S2cInformServerStatus {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub device_status: &'static str,
    pub device_session_id: Option<String>,
    pub loaded_sfd_firmware_id: Option<String>,
    pub datalogging_status: DataloggingStatus,
    pub device_comm_link: DeviceCommLink,
}

#[derive(Debug, Serialize)]
pub struct DataloggingStatus {
    pub datalogging_state: &'static str,
    pub completion_ratio: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct DeviceCommLink {
    pub link_type: &'static str,
    pub link_operational: bool,
    pub link_config: Value,
    pub demo_mode: bool,
}

#[derive(Debug, Serialize)]
pub struct S2cGetDeviceInfo {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub available: bool,
    pub device_info: Option<DeviceInfo>,
}

#[derive(Debug, Serialize)]
pub struct DeviceInfo {
    pub session_id: String,
    pub device_id: &'static str,
    pub display_name: &'static str,
    pub max_tx_data_size: u32,
    pub max_rx_data_size: u32,
    pub max_bitrate_bps: Option<u32>,
    pub rx_timeout_us: u32,
    pub heartbeat_timeout_us: u32,
    pub address_size_bits: u32,
    pub protocol_major: u32,
    pub protocol_minor: u32,
    pub supported_feature_map: SupportedFeatureMap,
    pub forbidden_memory_regions: Vec<Value>,
    pub readonly_memory_regions: Vec<Value>,
    pub datalogging_capabilities: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct SupportedFeatureMap {
    pub memory_write: bool,
    pub datalogging: bool,
    pub user_command: bool,
    pub _64bits: bool,
}

#[derive(Debug, Serialize)]
pub struct S2cGetWatchableCount {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub qty: WatchableQty,
}

#[derive(Debug, Serialize)]
pub struct WatchableQty {
    pub var: usize,
    pub alias: usize,
    pub rpv: usize,
    pub var_factory: usize,
}

#[derive(Debug, Serialize)]
pub struct S2cGetWatchableList {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub qty: WatchableQty,
    pub content: WatchableListContent,
    pub done: bool,
}

#[derive(Debug, Serialize)]
pub struct WatchableListContent {
    #[serde(rename = "var")]
    pub vars: Vec<WatchableBrief>,
    pub alias: Vec<WatchableBrief>,
    pub rpv: Vec<WatchableBrief>,
    pub var_factory: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct WatchableBrief {
    pub path: String,
    pub dtype: String,
    #[serde(rename = "type")]
    pub wtype: String,
}

#[derive(Debug, Serialize)]
pub struct S2cGetWatchableInfo {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub info: std::collections::HashMap<String, Value>,
}

/// Detailed watchable definition returned on subscribe or get_watchable_info.
#[derive(Debug, Serialize)]
pub struct WatchableDetailed {
    pub id: String,
    pub path: String,
    pub dtype: String,
    #[serde(rename = "type")]
    pub wtype: String,
    pub enum_def: Option<Value>,
    // var-only fields (null for rpv/alias)
    pub address: Option<u64>,
    pub bitoffset: Option<u32>,
    pub bitsize: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct S2cSubscribeWatchable {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub subscribed: std::collections::HashMap<String, Value>,
}

#[derive(Debug, Serialize)]
pub struct S2cUnsubscribeWatchable {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub unsubscribed: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct S2cChangeSubscriptionUpdateRate {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub effective_rates: std::collections::HashMap<String, Option<f64>>,
}

#[derive(Debug, Serialize)]
pub struct S2cWatchableUpdate {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub updates: Vec<WatchableUpdateRecord>,
}

#[derive(Debug, Serialize)]
pub struct WatchableUpdateRecord {
    pub id: String,
    pub v: Value,
    pub t: f64,
}

#[derive(Debug, Serialize)]
pub struct S2cWriteWatchable {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub request_token: String,
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct S2cWriteCompletion {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub batch_index: i64,
    pub watchable: String,
    pub success: bool,
    pub request_token: String,
    pub completion_server_time_us: f64,
}

#[derive(Debug, Serialize)]
pub struct S2cWriteSingleWatchable {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub success: bool,
}

#[derive(Debug, Serialize)]
pub struct S2cGetInstalledSfd {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub sfd_list: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct S2cGetLoadedSfd {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub sfd: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct S2cGetServerStats {
    pub cmd: &'static str,
    pub reqid: Option<i64>,
    pub uptime: f64,
    pub invalid_request_count: u64,
    pub unexpected_error_count: u64,
    pub client_count: usize,
}

// ── command name constants ───────────────────────────────────────────────────

pub mod cmd {
    // client → server
    pub const ECHO: &str = "echo";
    pub const GET_WATCHABLE_LIST: &str = "get_watchable_list";
    pub const GET_WATCHABLE_COUNT: &str = "get_watchable_count";
    pub const GET_WATCHABLE_INFO: &str = "get_watchable_info";
    pub const SUBSCRIBE_WATCHABLE: &str = "subscribe_watchable";
    pub const UNSUBSCRIBE_WATCHABLE: &str = "unsubscribe_watchable";
    pub const CHANGE_SUBSCRIPTION_UPDATE_RATE: &str = "change_subscription_update_rate";
    pub const GET_INSTALLED_SFD: &str = "get_installed_sfd";
    pub const GET_LOADED_SFD: &str = "get_loaded_sfd";
    pub const GET_SERVER_STATUS: &str = "get_server_status";
    pub const GET_DEVICE_INFO: &str = "get_device_info";
    pub const SET_LINK_CONFIG: &str = "set_link_config";
    pub const WRITE_WATCHABLE: &str = "write_watchable";
    pub const WRITE_SINGLE_WATCHABLE: &str = "write_single_watchable";
    pub const GET_SERVER_STATS: &str = "get_server_stats";
    pub const SET_THROTTLING: &str = "set_throttling";

    // server → client
    pub const WELCOME: &str = "welcome";
    pub const ECHO_RESPONSE: &str = "response_echo";
    pub const ERROR_RESPONSE: &str = "error";
    pub const INFORM_SERVER_STATUS: &str = "inform_server_status";
    pub const RESPONSE_GET_DEVICE_INFO: &str = "response_get_device_info";
    pub const RESPONSE_GET_WATCHABLE_COUNT: &str = "response_get_watchable_count";
    pub const RESPONSE_GET_WATCHABLE_LIST: &str = "response_get_watchable_list";
    pub const RESPONSE_GET_WATCHABLE_INFO: &str = "response_get_watchable_info";
    pub const RESPONSE_SUBSCRIBE_WATCHABLE: &str = "response_subscribe_watchable";
    pub const RESPONSE_UNSUBSCRIBE_WATCHABLE: &str = "response_unsubscribe_watchable";
    pub const RESPONSE_CHANGE_SUBSCRIPTION_UPDATE_RATE: &str =
        "response_change_subscription_update_rate";
    pub const WATCHABLE_UPDATE: &str = "watchable_update";
    pub const RESPONSE_WRITE_WATCHABLE: &str = "response_write_watchable";
    pub const INFORM_WRITE_COMPLETION: &str = "inform_write_completion";
    pub const RESPONSE_WRITE_SINGLE_WATCHABLE: &str = "response_write_single_watchable";
    pub const RESPONSE_GET_INSTALLED_SFD: &str = "response_get_installed_sfd";
    pub const RESPONSE_GET_LOADED_SFD: &str = "response_get_loaded_sfd";
    pub const RESPONSE_SET_LINK_CONFIG: &str = "response_set_link_config";
    pub const RESPONSE_GET_SERVER_STATS: &str = "response_get_server_stats";
    pub const RESPONSE_SET_THROTTLING: &str = "response_set_throttling";

    // datalogging – we return empty/unavailable responses for these
    pub const REQUEST_DATALOGGING_ACQUISITION: &str = "request_datalogging_acquisition";
    pub const LIST_DATALOGGING_ACQUISITIONS: &str = "list_datalogging_acquisitions";
    pub const READ_DATALOGGING_ACQUISITION_CONTENT: &str = "read_datalogging_acquisition_content";
    pub const UPDATE_DATALOGGING_ACQUISITION: &str = "update_datalogging_acquisition";
    pub const DELETE_DATALOGGING_ACQUISITION: &str = "delete_datalogging_acquisition";
    pub const DELETE_ALL_DATALOGGING_ACQUISITION: &str = "delete_all_datalogging_acquisition";
}

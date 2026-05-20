/// Main TCP server – accepts connections and drives each connection's lifecycle.
///
/// # Architecture
///
/// ```text
///  ┌──────────────────────────────────────────────────────────────┐
///  │                       ScrutinyServer                        │
///  │                                                              │
///  │  Arc<Datastore>  (read-only, shared)                        │
///  │  Arc<Mutex<ValueStore>>  (latest values, written by poller) │
///  │  broadcast::Sender<ValueEvent>  (changed values)            │
///  │  mpsc::Sender<WriteCommand>  (writes → datasource)          │
///  │  start_time: Instant                                         │
///  │  session_id: String                                          │
///  │                                                              │
///  │  ┌────────────┐   ┌────────────┐   ┌────────────────────┐   │
///  │  │  Listener  │   │  Poller    │   │  ConnectionTask×N  │   │
///  │  │  task      │   │  task      │   │                    │   │
///  │  │            │   │  (100 ms)  │   │  framing codec     │   │
///  │  │  accept()  │   │  polls DS  │   │  request handler   │   │
///  │  │  → spawn   │   │  broadcasts│   │  subscription state│   │
///  │  └────────────┘   └────────────┘   └────────────────────┘   │
///  └──────────────────────────────────────────────────────────────┘
/// ```
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc},
    time,
};
use tokio_util::codec::Framed;
use uuid::Uuid;

use crate::{
    api_types::{cmd, *},
    datasource::{ConnectionStatus, DataSource, WatchableKind, WatchableValue, WriteCommand},
    datastore::{Datastore, ValueStore},
    framing::ScrutinyCodec,
};

// ── event types ──────────────────────────────────────────────────────────────

/// Broadcast from the poller task to every connection task.
#[derive(Clone, Debug)]
pub struct ValueEvent {
    pub updates: Arc<Vec<(String, WatchableValue)>>,
    /// Timestamp from the datasource (microseconds since server start).
    /// Used as the `t` field in every `watchable_update` message.
    pub timestamp_us: f64,
}

// ── shared server state ──────────────────────────────────────────────────────

struct ServerShared {
    datastore: Arc<Datastore>,
    value_store: Arc<Mutex<ValueStore>>,
    write_tx: mpsc::Sender<WriteCommand>,
    start_time: Instant,
    session_id: String,
    /// Approximate count of active connections (best-effort).
    conn_count: Arc<Mutex<usize>>,
    /// Latest connection status reported by the datasource.
    /// Updated by the poller task; read by `make_server_status`.
    conn_status: Arc<Mutex<ConnectionStatus>>,
}

// ── public entry point ───────────────────────────────────────────────────────

/// Start the Scrutiny-compatible server.
///
/// `poll_interval_ms` controls how often the datasource's `poll()` method is
/// called and how frequently value updates are pushed to connected clients.
/// Typical values: 100 ms (10 Hz), 20 ms (50 Hz), 10 ms (100 Hz).
///
/// Blocks until the process is interrupted.
pub async fn run<DS: DataSource>(
    datasource: DS,
    addr: &str,
    poll_interval_ms: u64,
) -> anyhow::Result<()> {
    // --- build datastore from datasource metadata ---------------------------
    let mut datastore = Datastore::default();
    datastore.populate(datasource.watchables());
    let datastore = Arc::new(datastore);

    let value_store = Arc::new(Mutex::new(ValueStore::default()));

    // channels
    let (value_tx, _) = broadcast::channel::<ValueEvent>(512);
    let (write_tx, write_rx) = mpsc::channel::<WriteCommand>(256);

    let session_id = Uuid::new_v4().to_string();
    let conn_count = Arc::new(Mutex::new(0usize));
    let conn_status = Arc::new(Mutex::new(ConnectionStatus::Connected));

    let shared = Arc::new(ServerShared {
        datastore: datastore.clone(),
        value_store: value_store.clone(),
        write_tx,
        start_time: Instant::now(),
        session_id,
        conn_count: conn_count.clone(),
        conn_status: conn_status.clone(),
    });

    // --- start background tasks ---------------------------------------------
    spawn_poller(
        datasource,
        value_store.clone(),
        value_tx.clone(),
        write_rx,
        poll_interval_ms,
        shared.start_time,
        conn_status,
    );

    // --- listen for connections ---------------------------------------------
    let listener = TcpListener::bind(addr).await?;
    log::info!("Scrutiny server listening on {}", addr);

    loop {
        let (stream, peer) = listener.accept().await?;
        log::info!("New connection from {}", peer);

        let conn_id = Uuid::new_v4().to_string();
        let shared = shared.clone();
        let value_rx = value_tx.subscribe();

        *conn_count.lock().unwrap() += 1;

        tokio::spawn(async move {
            if let Err(e) =
                handle_connection(stream, conn_id.clone(), shared.clone(), value_rx).await
            {
                log::warn!("[{}] connection error: {}", conn_id, e);
            }
            *shared.conn_count.lock().unwrap() -= 1;
            log::info!("[{}] connection closed", conn_id);
        });
    }
}

// ── datasource poller ────────────────────────────────────────────────────────

fn spawn_poller<DS: DataSource>(
    mut ds: DS,
    value_store: Arc<Mutex<ValueStore>>,
    tx: broadcast::Sender<ValueEvent>,
    mut write_rx: mpsc::Receiver<WriteCommand>,
    poll_interval_ms: u64,
    start: Instant,
    conn_status: Arc<Mutex<ConnectionStatus>>,
) {
    tokio::spawn(async move {
        let mut poll_interval = time::interval(Duration::from_millis(poll_interval_ms));
        loop {
            tokio::select! {
                _ = poll_interval.tick() => {
                    let now_us = start.elapsed().as_secs_f64() * 1_000_000.0;
                    // Update connection status before polling values.
                    *conn_status.lock().unwrap() = ds.connection_status();
                    let result = ds.poll(now_us);
                    if !result.updates.is_empty() {
                        let ts = result.timestamp_us;
                        let mut store = value_store.lock().unwrap();
                        let pairs: Vec<(String, WatchableValue)> = result.updates
                            .into_iter()
                            .map(|u| {
                                store.set(&u.path, u.value.clone());
                                (u.path, u.value)
                            })
                            .collect();
                        drop(store);
                        log::trace!("poll: {} updates, ts={:.0} µs", pairs.len(), ts);
                        let _ = tx.send(ValueEvent { updates: Arc::new(pairs), timestamp_us: ts });
                    }
                }
                Some(cmd) = write_rx.recv() => {
                    match ds.write(&cmd.path, cmd.value) {
                        Ok(_) => log::debug!("write ok: {}", cmd.path),
                        Err(e) => log::warn!("write failed: {}: {}", cmd.path, e),
                    }
                }
            }
        }
    });
}

// ── per-connection handler ───────────────────────────────────────────────────

/// State that lives only within a single connection task.
struct ConnState {
    /// watchable ID → () (just tracking which IDs are subscribed)
    subscriptions: HashMap<String, ()>,
    invalid_request_count: u64,
}

async fn handle_connection(
    stream: TcpStream,
    conn_id: String,
    shared: Arc<ServerShared>,
    mut value_rx: broadcast::Receiver<ValueEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut framed = Framed::new(stream, ScrutinyCodec::new());
    let mut state = ConnState {
        subscriptions: HashMap::new(),
        invalid_request_count: 0,
    };

    // send welcome on connect
    send(&mut framed, make_welcome(shared.start_time)).await?;
    // immediately inform the client of server status
    send(&mut framed, make_server_status(&shared, None)).await?;

    let mut status_interval = time::interval(Duration::from_secs(2));
    status_interval.reset(); // don't fire immediately (we just sent it above)

    loop {
        tokio::select! {
            // ── incoming request from client ──────────────────────────────
            frame = framed.next() => {
                match frame {
                    None => break,          // connection closed
                    Some(Err(e)) => {
                        log::warn!("[{}] framing error: {}", conn_id, e);
                        break;
                    }
                    Some(Ok(msg)) => {
                        let cmd_name = msg.get("cmd").and_then(Value::as_str).unwrap_or("").to_owned();
                        let reqid = msg.get("reqid").and_then(Value::as_i64);
                        match dispatch(&cmd_name, reqid, &msg, &mut state, &shared).await {
                            Ok(response) => {
                                for r in response {
                                    send(&mut framed, r).await?;
                                }
                            }
                            Err(err_msg) => {
                                state.invalid_request_count += 1;
                                let err = make_error(reqid, &cmd_name, &err_msg);
                                send(&mut framed, err).await?;
                            }
                        }
                    }
                }
            }

            // ── value update broadcast ────────────────────────────────────
            event = value_rx.recv() => {
                match event {
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("[{}] value broadcast lagged {} messages", conn_id, n);
                    }
                    Err(_) => break,
                    Ok(ev) => {
                        let updates: Vec<WatchableUpdateRecord> = ev.updates.iter()
                            .filter(|(id, _)| state.subscriptions.contains_key(id))
                            .map(|(id, val)| WatchableUpdateRecord {
                                id: id.clone(),
                                v: val.to_json(),
                                t: ev.timestamp_us,
                            })
                            .collect();
                        if !updates.is_empty() {
                            log::trace!("[{conn_id}] pushing {} value updates", updates.len());
                            let msg = json_serialize(&S2cWatchableUpdate {
                                cmd: cmd::WATCHABLE_UPDATE,
                                reqid: None,
                                updates,
                            });
                            send(&mut framed, msg).await?;
                        }
                    }
                }
            }

            // ── periodic server status push ───────────────────────────────
            _ = status_interval.tick() => {
                log::debug!("[{conn_id}] periodic server status push");
                send(&mut framed, make_server_status(&shared, None)).await?;
            }
        }
    }

    Ok(())
}

// ── request dispatcher ───────────────────────────────────────────────────────

async fn dispatch(
    cmd_name: &str,
    reqid: Option<i64>,
    msg: &Value,
    state: &mut ConnState,
    shared: &Arc<ServerShared>,
) -> Result<Vec<Value>, String> {
    log::debug!("dispatch: cmd={} reqid={:?}", cmd_name, reqid);
    match cmd_name {
        cmd::ECHO => {
            let payload = msg["payload"].as_str().ok_or("missing payload")?;
            Ok(vec![json_serialize(&S2cEcho { cmd: cmd::ECHO_RESPONSE, reqid, payload: payload.to_owned() })])
        }

        cmd::GET_SERVER_STATUS => {
            Ok(vec![make_server_status(shared, reqid)])
        }

        cmd::GET_DEVICE_INFO => {
            Ok(vec![make_device_info(shared, reqid)])
        }

        cmd::GET_WATCHABLE_COUNT => {
            let ds = &shared.datastore;
            Ok(vec![json_serialize(&S2cGetWatchableCount {
                cmd: cmd::RESPONSE_GET_WATCHABLE_COUNT,
                reqid,
                qty: WatchableQty {
                    var: ds.var_count(),
                    alias: ds.alias_count(),
                    rpv: ds.rpv_count(),
                    var_factory: 0,
                },
            })])
        }

        cmd::GET_WATCHABLE_LIST => {
            let max_per = msg["max_per_response"].as_u64().unwrap_or(1000) as usize;
            let type_filter = extract_type_filter(msg);
            let name_filter = msg.get("filter")
                .and_then(|f| f.get("name"))
                .and_then(|n| {
                    if n.is_string() {
                        Some(vec![n.as_str().unwrap().to_owned()])
                    } else if n.is_array() {
                        Some(n.as_array().unwrap().iter().filter_map(|v| v.as_str().map(str::to_owned)).collect())
                    } else {
                        None
                    }
                });

            let all: Vec<_> = shared.datastore.all_entries()
                .filter(|e| {
                    if let Some(ref tf) = type_filter {
                        tf.contains(&e.definition.kind)
                    } else {
                        true
                    }
                })
                .filter(|e| {
                    if let Some(ref nf) = name_filter {
                        nf.iter().any(|pat| glob_match(pat, &e.definition.path))
                    } else {
                        true
                    }
                })
                .collect();

            let mut responses = Vec::new();
            let chunks: Vec<_> = all.chunks(max_per).collect();
            let total = chunks.len();
            for (i, chunk) in chunks.into_iter().enumerate() {
                let done = i + 1 == total || total == 0;
                let mut vars = Vec::new();
                let mut aliases = Vec::new();
                let mut rpvs = Vec::new();
                for e in chunk {
                    let brief = WatchableBrief {
                        path: e.definition.path.clone(),
                        dtype: e.definition.dtype.as_api_str().to_owned(),
                        wtype: e.definition.kind.as_api_str().to_owned(),
                    };
                    match e.definition.kind {
                        WatchableKind::Var => vars.push(brief),
                        WatchableKind::Alias => aliases.push(brief),
                        WatchableKind::Rpv => rpvs.push(brief),
                    }
                }
                responses.push(json_serialize(&S2cGetWatchableList {
                    cmd: cmd::RESPONSE_GET_WATCHABLE_LIST,
                    reqid,
                    qty: WatchableQty { var: vars.len(), alias: aliases.len(), rpv: rpvs.len(), var_factory: 0 },
                    content: WatchableListContent { vars, alias: aliases, rpv: rpvs, var_factory: vec![] },
                    done,
                }));
            }
            if responses.is_empty() {
                // Empty datastore: send a single "done" response
                responses.push(json_serialize(&S2cGetWatchableList {
                    cmd: cmd::RESPONSE_GET_WATCHABLE_LIST,
                    reqid,
                    qty: WatchableQty { var: 0, alias: 0, rpv: 0, var_factory: 0 },
                    content: WatchableListContent { vars: vec![], alias: vec![], rpv: vec![], var_factory: vec![] },
                    done: true,
                }));
            }
            Ok(responses)
        }

        cmd::GET_WATCHABLE_INFO => {
            let paths = msg["watchables"].as_array()
                .ok_or("missing watchables array")?
                .iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>();
            let mut info = std::collections::HashMap::new();
            for path in paths {
                let entry = shared.datastore.get(path)
                    .ok_or_else(|| format!("Unknown watchable: {path}"))?;
                info.insert(path.to_owned(), make_detailed(entry));
            }
            Ok(vec![json_serialize(&S2cGetWatchableInfo { cmd: cmd::RESPONSE_GET_WATCHABLE_INFO, reqid, info })])
        }

        cmd::SUBSCRIBE_WATCHABLE => {
            let paths = msg["watchables"].as_array()
                .ok_or("missing watchables")?
                .iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>();
            let mut subscribed = std::collections::HashMap::new();
            for path in &paths {
                let entry = shared.datastore.get(path)
                    .ok_or_else(|| format!("Unknown watchable: {path}"))?;
                subscribed.insert(path.to_string(), make_detailed(entry));
                state.subscriptions.insert(path.to_string(), ());
            }
            log::debug!("subscribed to {} watchables", paths.len());
            let mut out = vec![json_serialize(&S2cSubscribeWatchable {
                cmd: cmd::RESPONSE_SUBSCRIBE_WATCHABLE,
                reqid,
                subscribed,
            })];
            // Send current values immediately after subscription
            let store = shared.value_store.lock().unwrap();
            let updates: Vec<WatchableUpdateRecord> = paths.iter()
                .filter_map(|p| shared.datastore.get(p))
                .filter_map(|e| {
                    store.get(&e.definition.path).map(|val| WatchableUpdateRecord {
                        id: e.definition.path.clone(),
                        v: val.to_json(),
                        t: server_time_us(shared.start_time),
                    })
                })
                .collect();
            drop(store);
            if !updates.is_empty() {
                out.push(json_serialize(&S2cWatchableUpdate { cmd: cmd::WATCHABLE_UPDATE, reqid: None, updates }));
            }
            Ok(out)
        }

        cmd::UNSUBSCRIBE_WATCHABLE => {
            let paths = msg["watchables"].as_array()
                .ok_or("missing watchables")?
                .iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>();
            let mut unsubscribed = Vec::new();
            for path in &paths {
                if let Some(_) = shared.datastore.get(path) {
                    state.subscriptions.remove(*path);
                }
                unsubscribed.push(path.to_string());
            }
            log::debug!("unsubscribed from {} watchables", paths.len());
            Ok(vec![json_serialize(&S2cUnsubscribeWatchable {
                cmd: cmd::RESPONSE_UNSUBSCRIBE_WATCHABLE,
                reqid,
                unsubscribed,
            })])
        }

        cmd::CHANGE_SUBSCRIPTION_UPDATE_RATE => {
            // We don't throttle, but acknowledge with the requested rates.
            let changes = msg["changes"].as_array().ok_or("missing changes")?;
            let mut effective_rates = std::collections::HashMap::new();
            for ch in changes {
                let id = ch["id"].as_str().ok_or("missing id in change")?.to_owned();
                let rate = ch.get("rate").and_then(Value::as_f64);
                effective_rates.insert(id, rate);
            }
            Ok(vec![json_serialize(&S2cChangeSubscriptionUpdateRate {
                cmd: cmd::RESPONSE_CHANGE_SUBSCRIPTION_UPDATE_RATE,
                reqid,
                effective_rates,
            })])
        }

        cmd::SET_LINK_CONFIG => {
            // Accept silently – we don't connect to real devices.
            Ok(vec![
                json_serialize(&S2cEmpty { cmd: cmd::RESPONSE_SET_LINK_CONFIG, reqid }),
                make_server_status(shared, None),
            ])
        }

        cmd::GET_INSTALLED_SFD => {
            Ok(vec![json_serialize(&S2cGetInstalledSfd { cmd: cmd::RESPONSE_GET_INSTALLED_SFD, reqid, sfd_list: vec![] })])
        }

        cmd::GET_LOADED_SFD => {
            Ok(vec![json_serialize(&S2cGetLoadedSfd { cmd: cmd::RESPONSE_GET_LOADED_SFD, reqid, sfd: None })])
        }

        cmd::GET_SERVER_STATS => {
            let uptime = shared.start_time.elapsed().as_secs_f64();
            let client_count = *shared.conn_count.lock().unwrap();
            Ok(vec![json_serialize(&S2cGetServerStats {
                cmd: cmd::RESPONSE_GET_SERVER_STATS,
                reqid,
                uptime,
                invalid_request_count: state.invalid_request_count,
                unexpected_error_count: 0,
                client_count,
            })])
        }

        cmd::WRITE_WATCHABLE => {
            let updates_arr = msg["updates"].as_array().ok_or("missing updates")?;
            let request_token = Uuid::new_v4().to_string();
            let count = updates_arr.len();
            let mut write_cmds = Vec::new();
            for upd in updates_arr {
                let path = upd["watchable"].as_str().ok_or("missing watchable id")?.to_owned();
                shared.datastore.get(&path).ok_or_else(|| format!("Unknown watchable: {path}"))?;
                let batch_index = upd["batch_index"].as_i64().ok_or("missing batch_index")?;
                let value = parse_write_value(&upd["value"])?;
                write_cmds.push((path, batch_index, value));
            }
            let mut out = vec![json_serialize(&S2cWriteWatchable {
                cmd: cmd::RESPONSE_WRITE_WATCHABLE,
                reqid,
                request_token: request_token.clone(),
                count,
            })];
            let now_us = server_time_us(shared.start_time);
            for (path, batch_index, value) in write_cmds {
                let watchable_path = path.clone();
                let _ = shared.write_tx.try_send(WriteCommand {
                    path,
                    value,
                    request_token: request_token.clone(),
                    batch_index,
                });
                out.push(json_serialize(&S2cWriteCompletion {
                    cmd: cmd::INFORM_WRITE_COMPLETION,
                    reqid: None,
                    batch_index,
                    watchable: watchable_path,
                    success: true,
                    request_token: request_token.clone(),
                    completion_server_time_us: now_us,
                }));
            }
            Ok(out)
        }

        cmd::WRITE_SINGLE_WATCHABLE => {
            let path = msg["server_path"].as_str().ok_or("missing server_path")?;
            let entry = shared.datastore.get(path)
                .ok_or_else(|| format!("Unknown watchable path: {path}"))?;
            let value = parse_write_value(&msg["value"])?;
            let _ = shared.write_tx.try_send(WriteCommand {
                path: entry.definition.path.clone(),
                value,
                request_token: Uuid::new_v4().to_string(),
                batch_index: 0,
            });
            Ok(vec![json_serialize(&S2cWriteSingleWatchable {
                cmd: cmd::RESPONSE_WRITE_SINGLE_WATCHABLE,
                reqid,
                success: true,
            })])
        }

        // Datalogging commands – return unavailable/empty responses so the GUI
        // doesn't stall waiting for a reply.
        cmd::REQUEST_DATALOGGING_ACQUISITION => {
            Ok(vec![make_error(reqid, cmd_name, "Datalogging not supported")])
        }
        cmd::LIST_DATALOGGING_ACQUISITIONS => {
            Ok(vec![json!({ "cmd": "response_list_datalogging_acquisitions", "reqid": reqid, "acquisitions": [] }).into()])
        }
        cmd::READ_DATALOGGING_ACQUISITION_CONTENT |
        cmd::UPDATE_DATALOGGING_ACQUISITION |
        cmd::DELETE_DATALOGGING_ACQUISITION |
        cmd::DELETE_ALL_DATALOGGING_ACQUISITION => {
            Ok(vec![make_error(reqid, cmd_name, "Datalogging not supported")])
        }

        unknown => {
            Err(format!("Unsupported command: {unknown}"))
        }
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn json_serialize<T: serde::Serialize>(v: &T) -> Value {
    serde_json::to_value(v).expect("serialization failed")
}

async fn send(
    framed: &mut Framed<TcpStream, ScrutinyCodec>,
    msg: Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    framed.send(msg).await?;
    Ok(())
}

fn server_time_us(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1_000_000.0
}

fn make_welcome(start: Instant) -> Value {
    use std::time::{SystemTime, UNIX_EPOCH};
    let zero_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        - start.elapsed().as_secs_f64();
    json_serialize(&S2cWelcome {
        cmd: cmd::WELCOME,
        reqid: None,
        server_time_zero_timestamp: zero_ts,
    })
}

fn make_server_status(shared: &ServerShared, reqid: Option<i64>) -> Value {
    let status = shared.conn_status.lock().unwrap().clone();
    let (device_status, device_session_id, link_operational) = match status {
        ConnectionStatus::Connected => ("connected_ready", Some(shared.session_id.clone()), true),
        ConnectionStatus::Connecting => ("connecting", None, false),
        ConnectionStatus::Disconnected => ("disconnected", None, false),
    };
    json_serialize(&S2cInformServerStatus {
        cmd: cmd::INFORM_SERVER_STATUS,
        reqid,
        device_status,
        device_session_id,
        loaded_sfd_firmware_id: None,
        datalogging_status: DataloggingStatus {
            datalogging_state: "unavailable",
            completion_ratio: None,
        },
        device_comm_link: DeviceCommLink {
            link_type: "none",
            link_operational,
            link_config: json!({}),
            demo_mode: false,
        },
    })
}

fn make_device_info(shared: &ServerShared, reqid: Option<i64>) -> Value {
    json_serialize(&S2cGetDeviceInfo {
        cmd: cmd::RESPONSE_GET_DEVICE_INFO,
        reqid,
        available: true,
        device_info: Some(DeviceInfo {
            session_id: shared.session_id.clone(),
            device_id: "scrutiny-rust-datasource",
            display_name: "Rust DataSource",
            max_tx_data_size: 4096,
            max_rx_data_size: 4096,
            max_bitrate_bps: None,
            rx_timeout_us: 1_000_000,
            heartbeat_timeout_us: 5_000_000,
            address_size_bits: 32,
            protocol_major: 1,
            protocol_minor: 0,
            supported_feature_map: SupportedFeatureMap {
                memory_write: false,
                datalogging: false,
                user_command: false,
                _64bits: false,
            },
            forbidden_memory_regions: vec![],
            readonly_memory_regions: vec![],
            datalogging_capabilities: None,
        }),
    })
}

fn make_error(reqid: Option<i64>, request_cmd: &str, msg: &str) -> Value {
    json_serialize(&S2cError {
        cmd: cmd::ERROR_RESPONSE,
        reqid,
        request_cmd: request_cmd.to_owned(),
        msg: msg.to_owned(),
    })
}

fn make_detailed(entry: &crate::datastore::DatastoreEntry) -> Value {
    let def = &entry.definition;
    let mut obj = json!({
        "id":    def.path,
        "path":  def.path,
        "dtype": def.dtype.as_api_str(),
        "type":  def.kind.as_api_str(),
        "enum":  null,
    });
    match def.kind {
        WatchableKind::Var => {
            obj["address"] = Value::Null;
            obj["bitoffset"] = Value::Null;
            obj["bitsize"] = Value::Null;
        }
        WatchableKind::Rpv => {
            obj["rpvid"] = Value::from(u32::from(entry.rpv_id));
        }
        WatchableKind::Alias => {
            // Alias support is not implemented; caller should not pass Alias entries.
        }
    }
    obj
}

fn extract_type_filter(msg: &Value) -> Option<Vec<WatchableKind>> {
    let filter = msg.get("filter")?;
    let type_val = filter.get("type")?;
    let types = if type_val.is_string() {
        vec![type_val.as_str().unwrap().to_owned()]
    } else if type_val.is_array() {
        type_val
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect()
    } else {
        return None;
    };
    Some(
        types
            .into_iter()
            .filter_map(|t| match t.as_str() {
                "var" => Some(WatchableKind::Var),
                "rpv" => Some(WatchableKind::Rpv),
                "alias" => Some(WatchableKind::Alias),
                _ => None,
            })
            .collect(),
    )
}

fn parse_write_value(v: &Value) -> Result<WatchableValue, String> {
    match v {
        Value::Bool(b) => Ok(WatchableValue::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(WatchableValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(WatchableValue::Float(f))
            } else {
                Err("Invalid numeric value".into())
            }
        }
        Value::String(s) => {
            let s = s.to_lowercase();
            if s == "true" {
                return Ok(WatchableValue::Bool(true));
            }
            if s == "false" {
                return Ok(WatchableValue::Bool(false));
            }
            s.parse::<f64>()
                .map(WatchableValue::Float)
                .map_err(|_| format!("Cannot parse value: {s}"))
        }
        _ => Err(format!("Unsupported value type: {v}")),
    }
}

/// Simple glob matching supporting `*` and `?` wildcards.
fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    glob_match_inner(&p, &t)
}

fn glob_match_inner(p: &[char], t: &[char]) -> bool {
    match (p.first(), t.first()) {
        (None, None) => true,
        (Some(&'*'), _) => {
            glob_match_inner(&p[1..], t) || (!t.is_empty() && glob_match_inner(p, &t[1..]))
        }
        (Some(&'?'), Some(_)) => glob_match_inner(&p[1..], &t[1..]),
        (Some(pc), Some(tc)) if pc == tc => glob_match_inner(&p[1..], &t[1..]),
        _ => false,
    }
}

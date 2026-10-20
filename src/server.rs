
use anyhow::Result;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinSet;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};
use tracing::{debug, error, info, warn};

use crate::btc_index::history::HistoryIndex;
use crate::btc_index::{BtcIndex, IndexState};
use crate::btc_index::used::UsedSet;
use crate::node_probe::NodeState;
use crate::node_stats::NodeStats;
use crate::rpc::{BtcRpc, NodeError, never_sent};
use crate::watch::WatchState;

const COINBASE_MATURITY: u64 = 100;

pub const PROTOCOL: u64 = 3;

const HISTORY_LIMIT: usize = 1000;

const GOODBYE_DRAIN: Duration = Duration::from_secs(2);

const MAX_VOUTS: usize = 1000;

const MAX_EARLY_FRAMES: usize = 10_000;

#[derive(Clone)]
pub struct Ready {
    pub idx: BtcIndex,
    pub used: UsedSet,
    pub history: HistoryIndex,
    pub prune_rx: watch::Receiver<u32>,
    pub blocks: broadcast::Sender<String>,
    pub txs: broadcast::Sender<String>,
    pub watch_state: WatchState,
}

pub async fn run(
    listener: TcpListener,
    state_rx: watch::Receiver<IndexState>,
    node_rx: watch::Receiver<NodeState>,
    stats_rx: watch::Receiver<NodeStats>,
    ready_rx: watch::Receiver<Option<Ready>>,
    rpc: BtcRpc,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            _ = stopping(&mut shutdown) => break,
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let state_rx = state_rx.clone();
                let node_rx = node_rx.clone();
                let stats_rx = stats_rx.clone();
                let ready_rx = ready_rx.clone();
                let rpc = rpc.clone();
                let shutdown = shutdown.clone();
                conns.spawn(async move {
                    match tokio_tungstenite::accept_async(stream).await {
                        Ok(ws) => {
                            if let Err(e) = serve(ws, state_rx, node_rx, stats_rx, ready_rx, rpc, shutdown).await {
                                debug!("connection from {} ended: {:?}", peer, e);
                            }
                        }
                        Err(e) => debug!("ws handshake failed from {}: {:?}", peer, e),
                    }
                });
            }
        }
    }
    let _ = tokio::time::timeout(GOODBYE_DRAIN, async {
        while conns.join_next().await.is_some() {}
    })
    .await;
    Ok(())
}

async fn stopping(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|stop| *stop).await;
}

async fn goodbye(sink: &mut SplitSink<WebSocketStream<TcpStream>, Message>) {
    let _ = sink
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Away,
            reason: "indexd shutting down".into(),
        })))
        .await;
}

async fn serve(
    ws: WebSocketStream<TcpStream>,
    state_rx: watch::Receiver<IndexState>,
    node_rx: watch::Receiver<NodeState>,
    stats_rx: watch::Receiver<NodeStats>,
    ready_rx: watch::Receiver<Option<Ready>>,
    rpc: BtcRpc,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let (mut sink, mut stream) = ws.split();
    loop {
        let msg = tokio::select! {
            _ = stopping(&mut shutdown) => {
                goodbye(&mut sink).await;
                return Ok(());
            }
            msg = stream.next() => match msg {
                Some(msg) => msg?,
                None => return Ok(()),
            },
        };
        match msg {
            Message::Text(text) => {
                let req = serde_json::from_str::<Value>(&text).unwrap_or_default();
                if req["method"].as_str() == Some("subscribe_events") {
                    return subscribe(
                        &req, sink, stream, &state_rx, &node_rx, &stats_rx, ready_rx, shutdown,
                    )
                    .await;
                }
                let snapshot = ready_rx.borrow().clone();
                let response = answer(&req, snapshot.as_ref(), &state_rx, &node_rx, &rpc).await;
                sink.send(Message::text(response.to_string())).await?;
            }
            Message::Ping(p) => sink.send(Message::Pong(p)).await?,
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn subscribe(
    req: &Value,
    mut sink: SplitSink<WebSocketStream<TcpStream>, Message>,
    mut stream: SplitStream<WebSocketStream<TcpStream>>,
    state_rx: &watch::Receiver<IndexState>,
    node_rx: &watch::Receiver<NodeState>,
    stats_rx: &watch::Receiver<NodeStats>,
    mut ready_rx: watch::Receiver<Option<Ready>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut state_watch = state_rx.clone();
    let mut node_watch = node_rx.clone();
    state_watch.borrow_and_update();
    let node = node_watch.borrow_and_update().as_str();
    let hello = json!({
        "type": "hello",
        "v": PROTOCOL,
        "state": state_str(&state_watch),
        "node": node,
    })
    .to_string();
    sink.send(Message::text(hello)).await?;
    let (mut state_open, mut node_open) = (true, true);
    let mut early: Vec<(bool, String)> = Vec::new();

    let ready = loop {
        let snapshot = ready_rx.borrow_and_update().clone();
        if let Some(r) = snapshot {
            break r;
        }
        tokio::select! {
            _ = stopping(&mut shutdown) => {
                goodbye(&mut sink).await;
                return Ok(());
            }
            changed = ready_rx.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
            changed = state_watch.changed(), if state_open => match changed {
                Ok(()) => sink.send(Message::text(status_frame(state_rx, node_rx))).await?,
                Err(_) => state_open = false,
            },
            changed = node_watch.changed(), if node_open => match changed {
                Ok(()) => sink.send(Message::text(status_frame(state_rx, node_rx))).await?,
                Err(_) => node_open = false,
            },
            msg = stream.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let frame = serde_json::from_str::<Value>(&text).unwrap_or_default();
                    match (frame["method"].as_str(), frame["address"].as_str()) {
                        (Some("watch"), Some(a)) => early.push((true, a.to_string())),
                        (Some("unwatch"), Some(a)) => early.push((false, a.to_string())),
                        _ => debug!("unknown frame while waiting for the store: {}", text),
                    }
                    if early.len() > MAX_EARLY_FRAMES {
                        anyhow::bail!("more than {} watch frames before subscribed", MAX_EARLY_FRAMES);
                    }
                }
                Some(Ok(Message::Ping(p))) => sink.send(Message::Pong(p)).await?,
                Some(Ok(Message::Close(_))) | None => return Ok(()),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
            },
        }
    };

    let blocks_rx = ready.blocks.subscribe();
    let txs_rx = ready.txs.subscribe();
    let mut addresses = 0;
    for a in req["addresses"].as_array().into_iter().flatten().filter_map(Value::as_str) {
        ready.watch_state.watch(a);
        addresses += 1;
    }
    let mut txids = 0;
    for t in req["txids"].as_array().into_iter().flatten().filter_map(Value::as_str) {
        match valid_txid(t) {
            Some(t) => {
                ready.watch_state.watch_txid(&t);
                txids += 1;
            }
            None => debug!("subscribe_events: skipped invalid txid {}", t),
        }
    }
    for (watch, a) in early {
        if watch {
            ready.watch_state.watch(&a);
        } else {
            ready.watch_state.unwatch(&a);
        }
    }
    let subscribed = json!({ "type": "subscribed", "addresses": addresses, "txids": txids });
    sink.send(Message::text(subscribed.to_string())).await?;

    stream_events(sink, stream, state_rx, node_rx, stats_rx, ready, blocks_rx, txs_rx, shutdown).await
}

#[allow(clippy::too_many_arguments)]
async fn stream_events(
    mut sink: SplitSink<WebSocketStream<TcpStream>, Message>,
    mut stream: SplitStream<WebSocketStream<TcpStream>>,
    state_rx: &watch::Receiver<IndexState>,
    node_rx: &watch::Receiver<NodeState>,
    stats_rx: &watch::Receiver<NodeStats>,
    ready: Ready,
    mut blocks_rx: broadcast::Receiver<String>,
    mut txs_rx: broadcast::Receiver<String>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let Ready { watch_state, .. } = ready;
    info!(
        "event stream connected ({} addresses watched)",
        watch_state.addresses.len()
    );
    let mut state_watch = state_rx.clone();
    let mut state_open = true;
    let mut node_watch = node_rx.clone();
    let mut node_open = true;
    let mut stats_watch = stats_rx.clone();
    let mut stats_open = true;
    let first = stats_rx.borrow().frame();
    sink.send(Message::text(first)).await?;
    loop {
        tokio::select! {
            _ = stopping(&mut shutdown) => {
                goodbye(&mut sink).await;
                return Ok(());
            }
            changed = state_watch.changed(), if state_open => match changed {
                Ok(()) => {
                    sink.send(Message::text(status_frame(state_rx, node_rx))).await?;
                }
                Err(_) => {
                    debug!("state channel closed — no further state frames on this stream");
                    state_open = false;
                }
            },

            changed = node_watch.changed(), if node_open => match changed {
                Ok(()) => {
                    sink.send(Message::text(status_frame(state_rx, node_rx))).await?;
                }
                Err(_) => {
                    debug!("node channel closed — no further node frames on this stream");
                    node_open = false;
                }
            },
            changed = stats_watch.changed(), if stats_open => match changed {
                Ok(()) => {
                    let frame = stats_rx.borrow().frame();
                    sink.send(Message::text(frame)).await?;
                }
                Err(_) => {
                    debug!("stats channel closed — no further node_stats frames on this stream");
                    stats_open = false;
                }
            },
            event = blocks_rx.recv() => match event {
                Ok(frame) => {
                    if let Some(filtered) = filter_block_frame(&frame, &watch_state) {
                        sink.send(Message::text(filtered)).await?;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    debug!("event stream lagged {} block frames, sending resync", n);
                    sink.send(Message::text(json!({ "type": "resync" }).to_string())).await?;
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            },
            event = txs_rx.recv() => match event {
                Ok(frame) => sink.send(Message::text(frame)).await?,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    debug!("event stream dropped {} transaction frames", n);
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            },
            msg = stream.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let req = serde_json::from_str::<Value>(&text).unwrap_or_default();
                    match (req["method"].as_str(), req["address"].as_str()) {
                        (Some("watch"), Some(a)) => watch_state.watch(a),
                        (Some("unwatch"), Some(a)) => watch_state.unwatch(a),
                        _ => debug!("unknown frame on event stream: {}", text),
                    }
                }
                Some(Ok(Message::Ping(p))) => sink.send(Message::Pong(p)).await?,
                Some(Ok(Message::Close(_))) | None => return Ok(()),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
            },
        }
    }
}

pub(crate) fn filter_block_frame(frame: &str, watch_state: &WatchState) -> Option<String> {
    let event = serde_json::from_str::<Value>(frame).ok()?;
    if event["type"].as_str() != Some("block") {
        return Some(frame.to_string());
    }
    let changes: Vec<Value> = event["changes"]
        .as_array()?
        .iter()
        .filter(|c| {
            c["address"]
                .as_str()
                .is_some_and(|a| watch_state.addresses.contains(a))
        })
        .cloned()
        .collect();
    Some(
        json!({
            "type": "block",
            "height": event["height"],
            "hash": event["hash"],
            "changes": changes
        })
        .to_string(),
    )
}

fn status_frame(
    state_rx: &watch::Receiver<IndexState>,
    node_rx: &watch::Receiver<NodeState>,
) -> String {
    json!({
        "type": "state",
        "state": state_str(state_rx),
        "node": node_rx.borrow().as_str(),
    })
    .to_string()
}

pub(crate) fn state_str(state_rx: &watch::Receiver<IndexState>) -> &'static str {
    let state = *state_rx.borrow();
    if state == IndexState::Live && state_rx.has_changed().is_err() {
        return "catching_up";
    }
    match state {
        IndexState::Live => "live",
        IndexState::CatchingUp => "catching_up",
        IndexState::Empty => "empty",
        IndexState::Opening => "opening",
    }
}

pub(crate) async fn answer(
    req: &Value,
    ready: Option<&Ready>,
    state_rx: &watch::Receiver<IndexState>,
    node_rx: &watch::Receiver<NodeState>,
    rpc: &BtcRpc,
) -> Value {
    let state = state_str(state_rx);
    let node = node_rx.borrow().as_str();

    let Some(Ready {
        idx, used, history, prune_rx, watch_state, ..
    }) = ready
    else {
        return match req["method"].as_str() {
            Some("state") => json!({ "state": state, "height": Value::Null, "node": node }),
            Some("broadcast") => broadcast(req, state, rpc).await,
            Some("tx_detail") => tx_detail(req, state, rpc).await,
            Some("in_mempool") => in_mempool(req, state, rpc).await,
            Some("confirmed_height") => confirmed_height(req, state, rpc).await,
            Some("watch_txid") | Some("unwatch_txid") => match txid_of(req, state) {
                Err(resp) => resp,
                Ok(_) => json!({ "state": state, "ok": false }),
            },
            Some("balance") | Some("utxos") | Some("spendable") => match spk_for(req, state) {
                Err(resp) => resp,
                Ok(_) => json!({ "state": state }),
            },
            Some("utxos_batch") | Some("used_batch") | Some("history") => json!({ "state": state }),
            _ => json!({ "state": state, "error": "unknown method" }),
        };
    };

    match req["method"].as_str() {
        Some("state") => json!({
            "state": state,
            "height": idx.tip().ok().flatten().map(|(h, _)| h),
            "node": node,
            "history": history.coverage().ok().flatten().map(|(low, high)| {
                json!({ "from": low.max(*prune_rx.borrow()), "to": high })
            }),
        }),

        Some("broadcast") => broadcast(req, state, rpc).await,
        Some("tx_detail") => tx_detail(req, state, rpc).await,
        Some("in_mempool") => in_mempool(req, state, rpc).await,
        Some("confirmed_height") => confirmed_height(req, state, rpc).await,

        Some("watch_txid") => match txid_of(req, state) {
            Err(resp) => resp,
            Ok(txid) => {
                watch_state.watch_txid(&txid);
                json!({ "state": state, "ok": true })
            }
        },
        Some("unwatch_txid") => match txid_of(req, state) {
            Err(resp) => resp,
            Ok(txid) => {
                watch_state.unwatch_txid(&txid);
                json!({ "state": state, "ok": true })
            }
        },

        Some("balance") => match spk_for(req, state) {
            Err(resp) => resp,
            Ok(_) if state != "live" => json!({ "state": state }),
            Ok(spk) => match idx.query_spk(&spk) {
                Ok(bal) => json!({
                    "state": "live",
                    "sats": bal.sats,
                    "utxo_count": bal.utxo_count
                }),
                Err(e) => {
                    error!("query_spk failed: {:?}", e);
                    json!({ "state": state, "error": "index read failed" })
                }
            },
        },

        Some("utxos") => match spk_for(req, state) {
            Err(resp) => resp,
            Ok(_) if state != "live" => json!({ "state": state }),
            Ok(spk) => match merged_utxos(idx, watch_state, req["address"].as_str(), &spk) {
                Ok(utxos) => {
                    let address = req["address"].as_str().unwrap_or_default().to_string();
                    let mut sets = vec![(address, utxos)];
                    match mark_unwatched_spends(rpc, watch_state, &mut sets).await {
                        Ok(()) => json!({ "state": "live", "utxos": sets.remove(0).1 }),
                        Err(e) => {
                            warn!("gettxspendingprevout failed: {:?}", e);
                            json!({ "state": "live", "error": "node error during utxo check" })
                        }
                    }
                }
                Err(e) => {
                    error!("utxos_for_spk failed: {:?}", e);
                    json!({ "state": state, "error": "index read failed" })
                }
            },
        },

        Some("utxos_batch") => utxos_batch(req, idx, watch_state, state, rpc).await,
        Some("used_batch") => used_batch(req, used, state),
        Some("history") => history_query(req, history, prune_rx, state),

        Some("spendable") => match spk_for(req, state) {
            Err(resp) => resp,
            Ok(_) if state != "live" => json!({ "state": state }),
            Ok(spk) => match merged_utxos(idx, watch_state, req["address"].as_str(), &spk) {
                Ok(candidates) => spendable(candidates, rpc).await,
                Err(e) => {
                    error!("utxos_for_spk failed: {:?}", e);
                    json!({ "state": state, "error": "index read failed" })
                }
            },
        },

        _ => json!({ "state": state, "error": "unknown method" }),
    }
}

async fn utxos_batch(req: &Value, idx: &BtcIndex, watch_state: &WatchState, state: &str, rpc: &BtcRpc) -> Value {
    if state != "live" {
        return json!({ "state": state });
    }
    let Some(addrs) = req["addresses"].as_array() else {
        return json!({ "state": state, "error": "missing addresses" });
    };
    if addrs.len() > 8192 {
        return json!({ "state": state, "error": "too many addresses" });
    }
    let mut sets: Vec<(String, Vec<Value>)> = Vec::new();
    for addr in addrs {
        let Some(a) = addr.as_str() else {
            return json!({ "state": state, "error": "invalid address" });
        };
        let Some(spk) = BtcIndex::script_pubkey_for(a) else {
            return json!({ "state": state, "error": "invalid address" });
        };
        match merged_utxos(idx, watch_state, Some(a), &spk) {
            Ok(utxos) if utxos.is_empty() => {}
            Ok(utxos) => sets.push((a.to_string(), utxos)),
            Err(e) => {
                error!("utxos_batch read failed for {}: {:?}", a, e);
                return json!({ "state": state, "error": "index read failed" });
            }
        }
    }
    if let Err(e) = mark_unwatched_spends(rpc, watch_state, &mut sets).await {
        warn!("gettxspendingprevout failed: {:?}", e);
        return json!({ "state": state, "error": "node error during utxo check" });
    }
    let wallets: Vec<Value> = sets
        .into_iter()
        .map(|(address, utxos)| json!({ "address": address, "utxos": utxos }))
        .collect();
    json!({ "state": "live", "wallets": wallets })
}

fn used_batch(req: &Value, used: &UsedSet, state: &str) -> Value {
    if state != "live" {
        return json!({ "state": state });
    }
    let Some(addrs) = req["addresses"].as_array() else {
        return json!({ "state": state, "error": "missing addresses" });
    };
    if addrs.len() > 8192 {
        return json!({ "state": state, "error": "too many addresses" });
    }
    match used.cursor() {
        Ok(Some(_)) => {}
        Ok(None) => return json!({ "state": state, "error": "used set not built" }),
        Err(e) => {
            error!("used_batch cursor read failed: {:?}", e);
            return json!({ "state": state, "error": "index read failed" });
        }
    }
    let mut names = Vec::with_capacity(addrs.len());
    let mut spks = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let Some(a) = addr.as_str() else {
            return json!({ "state": state, "error": "invalid address" });
        };
        let Some(spk) = BtcIndex::script_pubkey_for(a) else {
            return json!({ "state": state, "error": "invalid address" });
        };
        names.push(a);
        spks.push(spk);
    }
    match used.contains_batch(&spks) {
        Ok(flags) => {
            let hits: Vec<&str> = names.iter().zip(flags).filter(|(_, u)| *u).map(|(a, _)| *a).collect();
            json!({ "state": "live", "used": hits })
        }
        Err(e) => {
            error!("used_batch read failed: {:?}", e);
            json!({ "state": state, "error": "index read failed" })
        }
    }
}

fn history_query(req: &Value, history: &HistoryIndex, prune_rx: &watch::Receiver<u32>, state: &str) -> Value {
    if state != "live" {
        return json!({ "state": state });
    }
    let Some(addrs) = req["addresses"].as_array() else {
        return json!({ "state": state, "error": "missing addresses" });
    };
    if addrs.len() > 8192 {
        return json!({ "state": state, "error": "too many addresses" });
    }
    let limit = req["limit"]
        .as_u64()
        .map_or(HISTORY_LIMIT, |l| (l as usize).clamp(1, HISTORY_LIMIT));
    let mut spks = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let Some(spk) = addr.as_str().and_then(BtcIndex::script_pubkey_for) else {
            return json!({ "state": state, "error": "invalid address" });
        };
        spks.push(spk);
    }
    match history.history(&spks, *prune_rx.borrow(), limit) {
        Ok(None) => json!({ "state": "live", "error": "history index not built" }),
        Ok(Some(h)) => json!({
            "state": "live",
            "from": h.from,
            "to": h.to,
            "transactions": h
                .transactions
                .iter()
                .map(|(height, txid)| json!({ "txid": txid_display_hex(txid), "height": height }))
                .collect::<Vec<_>>(),
            "truncated": h.truncated,
        }),
        Err(e) => {
            error!("history read failed: {:?}", e);
            json!({ "state": state, "error": "index read failed" })
        }
    }
}

async fn mark_unwatched_spends(
    rpc: &BtcRpc,
    watch_state: &WatchState,
    sets: &mut [(String, Vec<Value>)],
) -> Result<()> {
    let mut outpoints: Vec<Value> = Vec::new();
    for (address, coins) in sets.iter() {
        if watch_state.addresses.contains(address) {
            continue;
        }
        for coin in coins {
            if coin["height"].as_u64().unwrap_or(0) > 0 {
                outpoints.push(json!({ "txid": coin["txid"], "vout": coin["vout"] }));
            }
        }
    }
    if outpoints.is_empty() {
        return Ok(());
    }
    let results = rpc.call("gettxspendingprevout", json!([outpoints])).await?;
    let mut spenders: HashMap<String, String> = HashMap::new();
    for entry in results.as_array().into_iter().flatten() {
        if let (Some(txid), Some(vout), Some(spender)) =
            (entry["txid"].as_str(), entry["vout"].as_u64(), entry["spendingtxid"].as_str())
        {
            spenders.insert(format!("{}:{}", txid, vout), spender.to_string());
        }
    }
    if spenders.is_empty() {
        return Ok(());
    }
    for (address, coins) in sets.iter_mut() {
        if watch_state.addresses.contains(address) {
            continue;
        }
        for coin in coins.iter_mut() {
            let key = format!("{}:{}", coin["txid"].as_str().unwrap_or_default(), coin["vout"]);
            if let Some(spender) = spenders.get(&key) {
                coin["spent_by"] = json!(spender);
            }
        }
    }
    Ok(())
}

fn merged_utxos(
    idx: &BtcIndex,
    watch_state: &WatchState,
    address: Option<&str>,
    spk: &[u8],
) -> Result<Vec<Value>> {
    let records = idx.utxos_for_spk(spk)?;
    let mut confirmed_keys = HashSet::new();
    let mut utxos: Vec<Value> = records
        .iter()
        .map(|rec| {
            let txid = txid_display_hex(&rec.txid);
            let key = format!("{}:{}", txid, rec.vout);
            confirmed_keys.insert(key.clone());
            let mut coin = json!({
                "txid": txid,
                "vout": rec.vout,
                "sats": rec.sats,
                "height": rec.height
            });
            if let Some(spender) = watch_state.spender_of(&key) {
                coin["spent_by"] = json!(spender);
            }
            coin
        })
        .collect();
    if let Some(addr) = address {
        utxos.extend(watch_state.unconfirmed_for(addr, &confirmed_keys));
    }
    Ok(utxos)
}

async fn spendable(candidates: Vec<Value>, rpc: &BtcRpc) -> Value {
    let mut out = Vec::with_capacity(candidates.len());
    for utxo in candidates {
        let (Some(txid), Some(vout)) = (utxo["txid"].as_str(), utxo["vout"].as_u64()) else {
            continue;
        };
        let view = match rpc.call("gettxout", json!([txid, vout, true])).await {
            Ok(v) => v,
            Err(e) => {
                warn!("gettxout failed for {}:{}: {:?}", txid, vout, e);
                return json!({ "state": "live", "error": "node error during utxo check" });
            }
        };
        if view.is_null() {
            debug!("utxo {}:{} spent in node view, dropping", txid, vout);
            continue;
        }
        if view["coinbase"].as_bool() == Some(true)
            && view["confirmations"].as_u64().unwrap_or(0) < COINBASE_MATURITY
        {
            debug!("utxo {}:{} is immature coinbase, dropping", txid, vout);
            continue;
        }
        out.push(json!({ "txid": txid, "vout": vout, "sats": utxo["sats"] }));
    }
    json!({ "state": "live", "utxos": out })
}

async fn broadcast(req: &Value, state: &str, rpc: &BtcRpc) -> Value {
    let Some(tx_hex) = req["tx_hex"].as_str() else {
        return json!({ "state": state, "error": "missing tx_hex" });
    };
    match rpc.call("sendrawtransaction", json!([tx_hex])).await {
        Ok(v) => {
            let txid = v.as_str().unwrap_or_default();
            debug!("broadcast accepted: {}", txid);
            json!({ "state": state, "txid": txid })
        }
        Err(e) => match e.downcast_ref::<NodeError>() {
            Some(NodeError(node_err)) => {
                debug!("node rejected transaction: {}", node_err);
                json!({ "state": state, "node_error": node_err })
            }
            None if never_sent(&e) => {
                warn!("sendrawtransaction not sent, node unreachable: {:?}", e);
                json!({ "state": state, "error": "bitcoin node unreachable", "unsent": true })
            }
            None => {
                error!("sendrawtransaction outcome unknown: {:?}", e);
                json!({ "state": state, "error": "no answer from the bitcoin node" })
            }
        },
    }
}

async fn in_mempool(req: &Value, state: &str, rpc: &BtcRpc) -> Value {
    let txid = match txid_of(req, state) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    match rpc.call("getmempoolentry", json!([txid])).await {
        Ok(entry) => json!({ "state": state, "in_mempool": entry.is_object() }),
        Err(e) => match e.downcast_ref::<NodeError>() {
            Some(NodeError(err)) if err["code"].as_i64() == Some(-5) => {
                json!({ "state": state, "in_mempool": false })
            }
            _ => {
                warn!("in_mempool {}: no answer from the node: {:?}", txid, e);
                json!({ "state": state, "error": "no answer from the bitcoin node" })
            }
        },
    }
}

async fn confirmed_height(req: &Value, state: &str, rpc: &BtcRpc) -> Value {
    let txid = match txid_of(req, state) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    let Some(vouts) = req["vouts"].as_array() else {
        return json!({ "state": state, "error": "missing vouts" });
    };
    if vouts.is_empty() || vouts.len() > MAX_VOUTS {
        return json!({ "state": state, "error": "bad vouts" });
    }
    let no_answer = json!({ "state": state, "error": "no answer from the bitcoin node" });
    for vout in vouts {
        let Some(vout) = vout.as_u64() else {
            return json!({ "state": state, "error": "bad vouts" });
        };
        let out = match rpc.call("gettxout", json!([txid, vout, false])).await {
            Ok(v) => v,
            Err(e) => {
                warn!("confirmed_height {}:{}: no answer from the node: {:?}", txid, vout, e);
                return no_answer;
            }
        };
        let (Some(confirmations), Some(tip)) = (out["confirmations"].as_u64(), out["bestblock"].as_str()) else {
            continue;
        };
        if confirmations == 0 {
            continue;
        }
        let header = match rpc.call("getblockheader", json!([tip])).await {
            Ok(h) => h,
            Err(e) => {
                warn!("confirmed_height {}: getblockheader {} failed: {:?}", txid, tip, e);
                return no_answer;
            }
        };
        let Some(tip_height) = header["height"].as_u64() else {
            return no_answer;
        };
        return json!({ "state": state, "height": (tip_height + 1).saturating_sub(confirmations) });
    }
    json!({ "state": state, "height": Value::Null })
}

async fn tx_detail(req: &Value, state: &str, rpc: &BtcRpc) -> Value {
    let Some(txid) = req["txid"].as_str() else {
        return json!({ "state": state, "error": "missing txid" });
    };
    let Some(height) = req["height"].as_u64() else {
        return json!({ "state": state, "error": "missing height" });
    };
    let blockhash = match rpc.call("getblockhash", json!([height])).await {
        Ok(v) => match v.as_str() {
            Some(h) => h.to_string(),
            None => return json!({ "state": state, "error": "getblockhash: not a string" }),
        },
        Err(e) => {
            warn!("tx_detail: getblockhash {} failed: {:?}", height, e);
            return node_failure(&e, state, "getblockhash failed");
        }
    };
    let tx = match rpc.call("getrawtransaction", json!([txid, 2, blockhash])).await {
        Ok(v) => v,
        Err(e) => {
            if e.downcast_ref::<NodeError>().is_some() {
                debug!("tx_detail: {} at height {} not recoverable (pruned away, or wrong height): {:?}", txid, height, e);
            } else {
                warn!("tx_detail: getrawtransaction {} at height {} got no answer: {:?}", txid, height, e);
            }
            return node_failure(&e, state, "transaction not recoverable");
        }
    };

    let inputs: Vec<Value> = tx["vin"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|vin| vin.get("coinbase").is_none())
        .map(|vin| {
            let prevout = &vin["prevout"];
            json!({
                "address": prevout["scriptPubKey"]["address"].as_str(),
                "sats": prevout["value"].as_f64().map(|v| (v * 1e8).round() as u64),
            })
        })
        .collect();
    let outputs: Vec<Value> = tx["vout"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|vout| {
            let spk_hex = vout["scriptPubKey"]["hex"].as_str().unwrap_or("");
            if spk_hex.starts_with("6a") || spk_hex.len() > 20_000 {
                return None;
            }
            Some(json!({
                "address": vout["scriptPubKey"]["address"].as_str(),
                "sats": vout["value"].as_f64().map(|v| (v * 1e8).round() as u64),
                "vout": vout["n"].as_u64(),
            }))
        })
        .collect();

    json!({
        "state": state,
        "tx": {
            "type": "confirmed_tx",
            "txid": txid,
            "height": height,
            "blocktime": tx["blocktime"].as_u64().unwrap_or(0),
            "fees": tx["fee"]
                .as_f64()
                .map(|f| ((f * 1e8).round() as u64).to_string())
                .unwrap_or_else(|| "0".to_string()),
            "inputs": inputs,
            "outputs": outputs,
        }
    })
}

fn spk_for(req: &Value, state: &str) -> Result<Vec<u8>, Value> {
    let Some(addr) = req["address"].as_str() else {
        return Err(json!({ "state": state, "error": "missing address" }));
    };
    BtcIndex::script_pubkey_for(addr)
        .ok_or_else(|| json!({ "state": state, "error": "invalid address" }))
}

fn node_failure(e: &anyhow::Error, state: &str, what: &str) -> Value {
    if e.downcast_ref::<NodeError>().is_some() {
        json!({ "state": state, "error": what })
    } else {
        json!({ "state": state, "error": "no answer from the bitcoin node", "no_answer": true })
    }
}

fn valid_txid(txid: &str) -> Option<String> {
    (txid.len() == 64 && txid.bytes().all(|b| b.is_ascii_hexdigit())).then(|| txid.to_ascii_lowercase())
}

fn txid_of(req: &Value, state: &str) -> Result<String, Value> {
    match req["txid"].as_str() {
        None => Err(json!({ "state": state, "error": "missing txid" })),
        Some(t) => valid_txid(t).ok_or_else(|| json!({ "state": state, "error": "invalid txid" })),
    }
}

fn txid_display_hex(txid: &[u8; 32]) -> String {
    txid.iter().rev().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btc_index::store::UtxoRecord;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    type Ws = WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

    const TXID: &str = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b";
    const ADDR: &str = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4";
    const ADDR2: &str = "12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX";

    fn states() -> (
        watch::Sender<IndexState>,
        watch::Receiver<IndexState>,
        watch::Sender<NodeState>,
        watch::Receiver<NodeState>,
    ) {
        let (state_tx, state_rx) = watch::channel(IndexState::Opening);
        let (node_tx, node_rx) = watch::channel(NodeState::Unknown);
        (state_tx, state_rx, node_tx, node_rx)
    }

    fn temp_path(tag: &str) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "indexd-server-test-{}-{}-{}.redb",
            std::process::id(),
            tag,
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn ready() -> (Ready, Vec<PathBuf>) {
        ready_at(0)
    }

    fn ready_at(prune_height: u32) -> (Ready, Vec<PathBuf>) {
        let (index_path, used_path, history_path) = (temp_path("index"), temp_path("used"), temp_path("history"));
        let idx = BtcIndex::open(index_path.to_str().unwrap()).unwrap();
        let used = UsedSet::open(used_path.to_str().unwrap()).unwrap();
        let history = HistoryIndex::open(history_path.to_str().unwrap()).unwrap();
        let (blocks, _) = broadcast::channel(16);
        let (txs, _) = broadcast::channel(16);
        let watch_state = WatchState::new(idx.clone());
        let (_prune_tx, prune_rx) = watch::channel(prune_height);
        (
            Ready { idx, used, history, prune_rx, blocks, txs, watch_state },
            vec![index_path, used_path, history_path],
        )
    }

    fn cleanup(paths: Vec<PathBuf>) {
        for p in paths {
            let _ = std::fs::remove_file(p);
        }
    }

    async fn refused_url() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{}", addr)
    }

    fn ok(result: Value) -> Option<Value> {
        Some(json!({ "result": result, "error": null, "id": "relay" }))
    }

    fn err(code: i64, message: &str) -> Option<Value> {
        Some(json!({ "result": null, "error": { "code": code, "message": message }, "id": "relay" }))
    }

    async fn fake_node<F>(reply: F) -> String
    where
        F: Fn(&str, &Value) -> Option<Value> + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let reply = Arc::new(reply);
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let reply = Arc::clone(&reply);
                tokio::spawn(async move {
                    let Some(body) = read_request(&mut sock).await else { return };
                    let req: Value = serde_json::from_slice(&body).unwrap_or_default();
                    let Some(resp) = reply(req["method"].as_str().unwrap_or_default(), &req["params"]) else {
                        return;
                    };
                    let text = resp.to_string();
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        text.len()
                    );
                    let _ = sock.write_all(head.as_bytes()).await;
                    let _ = sock.write_all(text.as_bytes()).await;
                });
            }
        });
        url
    }

    async fn read_request(sock: &mut TcpStream) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = sock.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
            let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else { continue };
            let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
            let len: usize = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);
            let start = end + 4;
            while buf.len() < start + len {
                let n = sock.read(&mut chunk).await.ok()?;
                if n == 0 {
                    return None;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            return Some(buf[start..start + len].to_vec());
        }
    }

    async fn next_msg(ws: &mut Ws) -> Message {
        tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("no frame within 5 s")
            .expect("stream ended")
            .expect("websocket error")
    }

    async fn next_json(ws: &mut Ws) -> Value {
        loop {
            match next_msg(ws).await {
                Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                Message::Pong(_) | Message::Ping(_) => continue,
                other => panic!("expected a text frame, got {:?}", other),
            }
        }
    }

    async fn round_trip(ws: &mut Ws, tag: u8) {
        ws.send(Message::Ping(vec![tag].into())).await.unwrap();
        loop {
            match next_msg(ws).await {
                Message::Pong(p) if p.to_vec() == vec![tag] => return,
                Message::Pong(_) => continue,
                other => panic!("expected pong {}, got {:?}", tag, other),
            }
        }
    }

    #[tokio::test]
    async fn broadcast_says_unsent_only_when_the_node_was_never_reached() {
        let (_state_tx, state_rx, _node_tx, node_rx) = states();
        let req = json!({ "method": "broadcast", "tx_hex": "00" });

        let rpc = BtcRpc::for_test(&refused_url().await);
        let resp = answer(&req, None, &state_rx, &node_rx, &rpc).await;
        assert_eq!(resp["unsent"], true);
        assert!(resp["error"].is_string());

        let rpc = BtcRpc::for_test(&fake_node(|_, _| None).await);
        let resp = answer(&req, None, &state_rx, &node_rx, &rpc).await;
        assert!(resp.get("unsent").is_none(), "{resp}");
        assert!(resp["error"].is_string());
        assert!(resp.get("txid").is_none());

        let rpc = BtcRpc::for_test(&fake_node(|_, _| err(-26, "min relay fee not met")).await);
        let resp = answer(&req, None, &state_rx, &node_rx, &rpc).await;
        assert_eq!(resp["node_error"]["code"], -26);
        assert!(resp.get("unsent").is_none());
        let rpc = BtcRpc::for_test(&fake_node(|_, _| ok(json!(TXID))).await);
        assert_eq!(answer(&req, None, &state_rx, &node_rx, &rpc).await["txid"], TXID);
    }

    #[tokio::test]
    async fn in_mempool_keeps_no_answer_apart_from_no() {
        let (_state_tx, state_rx, _node_tx, node_rx) = states();
        let url = fake_node(|method, params| match (method, params[0].as_str()) {
            ("getmempoolentry", Some(TXID)) => ok(json!({ "vsize": 141 })),
            ("getmempoolentry", _) => err(-5, "Transaction not in mempool"),
            _ => err(-32601, "Method not found"),
        })
        .await;
        let rpc = BtcRpc::for_test(&url);
        let ask = |txid: &str| json!({ "method": "in_mempool", "txid": txid });
        let other = "bb".repeat(32);

        assert_eq!(answer(&ask(TXID), None, &state_rx, &node_rx, &rpc).await["in_mempool"], true);
        assert_eq!(answer(&ask(&other), None, &state_rx, &node_rx, &rpc).await["in_mempool"], false);
        let dead = BtcRpc::for_test(&refused_url().await);
        let resp = answer(&ask(TXID), None, &state_rx, &node_rx, &dead).await;
        assert!(resp.get("in_mempool").is_none(), "{resp}");
        assert!(resp["error"].is_string());
        assert_eq!(answer(&ask("xyz"), None, &state_rx, &node_rx, &rpc).await["error"], "invalid txid");
    }

    #[tokio::test]
    async fn confirmed_height_keeps_no_answer_apart_from_null() {
        let (_state_tx, state_rx, _node_tx, node_rx) = states();
        let url = fake_node(|method, params| match method {
            "gettxout" if params[1] == 1 => ok(json!({ "confirmations": 3, "bestblock": "tiphash" })),
            "gettxout" => ok(Value::Null),
            "getblockheader" if params[0] == "tiphash" => ok(json!({ "height": 100 })),
            _ => err(-32601, "Method not found"),
        })
        .await;
        let rpc = BtcRpc::for_test(&url);
        let ask = |vouts: Value| json!({ "method": "confirmed_height", "txid": TXID, "vouts": vouts });

        assert_eq!(answer(&ask(json!([0, 1])), None, &state_rx, &node_rx, &rpc).await["height"], 98);
        let resp = answer(&ask(json!([0])), None, &state_rx, &node_rx, &rpc).await;
        assert_eq!(resp.get("height"), Some(&Value::Null), "{resp}");
        assert!(resp.get("error").is_none());
        let dead = BtcRpc::for_test(&refused_url().await);
        let resp = answer(&ask(json!([1])), None, &state_rx, &node_rx, &dead).await;
        assert!(resp.get("height").is_none(), "{resp}");
        assert!(resp["error"].is_string());
        assert_eq!(answer(&ask(json!([])), None, &state_rx, &node_rx, &rpc).await["error"], "bad vouts");
        assert_eq!(answer(&ask(json!(["x"])), None, &state_rx, &node_rx, &rpc).await["error"], "bad vouts");
    }

    #[tokio::test]
    async fn tx_detail_keeps_a_node_verdict_apart_from_no_answer() {
        let (_state_tx, state_rx, _node_tx, node_rx) = states();
        let ask = json!({ "method": "tx_detail", "txid": TXID, "height": 100 });
        let url = fake_node(|method, _| match method {
            "getblockhash" => ok(json!("blockhash")),
            _ => err(-5, "No such mempool or blockchain transaction"),
        })
        .await;
        let resp = answer(&ask, None, &state_rx, &node_rx, &BtcRpc::for_test(&url)).await;
        assert_eq!(resp["error"], "transaction not recoverable");
        assert!(resp.get("no_answer").is_none(), "{resp}");
        let dead = BtcRpc::for_test(&refused_url().await);
        let resp = answer(&ask, None, &state_rx, &node_rx, &dead).await;
        assert_eq!(resp["no_answer"], true, "{resp}");
        assert!(resp.get("tx").is_none());
    }

    #[tokio::test]
    async fn txid_watch_is_acknowledged_and_waits_for_the_store() {
        let (_state_tx, state_rx, _node_tx, node_rx) = states();
        let rpc = BtcRpc::for_test(&refused_url().await);
        let (ready, paths) = ready();
        let watch = json!({ "method": "watch_txid", "txid": TXID.to_uppercase() });

        assert_eq!(answer(&watch, None, &state_rx, &node_rx, &rpc).await["ok"], false);
        assert_eq!(answer(&watch, Some(&ready), &state_rx, &node_rx, &rpc).await["ok"], true);
        assert!(ready.watch_state.txids().contains(TXID));
        let unwatch = json!({ "method": "unwatch_txid", "txid": TXID });
        assert_eq!(answer(&unwatch, Some(&ready), &state_rx, &node_rx, &rpc).await["ok"], true);
        assert!(!ready.watch_state.txids().contains(TXID));
        let bad = json!({ "method": "watch_txid", "txid": "not-a-txid" });
        assert_eq!(answer(&bad, Some(&ready), &state_rx, &node_rx, &rpc).await["error"], "invalid txid");
        assert!(ready.watch_state.txids().is_empty());
        for method in ["in_mempool", "confirmed_height"] {
            let req = json!({ "method": method, "txid": TXID, "vouts": [0] });
            let resp = answer(&req, Some(&ready), &state_rx, &node_rx, &rpc).await;
            assert_ne!(resp["error"], "unknown method", "{method} is missing from the open-store table");
        }
        cleanup(paths);
    }

    #[tokio::test]
    async fn event_stream_waits_out_the_store_then_registers_and_says_goodbye() {
        let (_state_tx, state_rx, node_tx, node_rx) = states();
        let (_stats_tx, stats_rx) = watch::channel(NodeStats::default());
        let (ready_tx, ready_rx) = watch::channel::<Option<Ready>>(None);
        let (stop_tx, stop_rx) = watch::channel(false);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let rpc = BtcRpc::for_test(&refused_url().await);
        let server = tokio::spawn(run(listener, state_rx, node_rx, stats_rx, ready_rx, rpc, stop_rx));

        let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str()).await.unwrap();
        let subscribe = json!({ "method": "subscribe_events", "addresses": [ADDR], "txids": [TXID] });
        ws.send(Message::text(subscribe.to_string())).await.unwrap();
        let hello = next_json(&mut ws).await;
        assert_eq!(hello["type"], "hello");
        assert_eq!(hello["v"], PROTOCOL);
        assert_eq!(hello["state"], "opening");

        round_trip(&mut ws, 1).await;
        node_tx.send_replace(NodeState::Up);
        let state = next_json(&mut ws).await;
        assert_eq!(state, json!({ "type": "state", "state": "opening", "node": "up" }));
        ws.send(Message::text(json!({ "method": "watch", "address": ADDR2 }).to_string())).await.unwrap();
        round_trip(&mut ws, 2).await;

        let (ready, paths) = ready();
        let watch_state = ready.watch_state.clone();
        ready_tx.send_replace(Some(ready));
        assert_eq!(next_json(&mut ws).await, json!({ "type": "subscribed", "addresses": 1, "txids": 1 }));
        assert!(watch_state.addresses.contains(ADDR));
        assert!(watch_state.addresses.contains(ADDR2));
        assert!(watch_state.txids().contains(TXID));
        assert_eq!(next_json(&mut ws).await["type"], "node_stats");

        stop_tx.send_replace(true);
        loop {
            match next_msg(&mut ws).await {
                Message::Close(Some(frame)) => {
                    assert_eq!(frame.code, CloseCode::Away);
                    break;
                }
                Message::Pong(_) | Message::Text(_) => continue,
                other => panic!("expected a 1001 close, got {:?}", other),
            }
        }
        tokio::time::timeout(Duration::from_secs(5), server).await.unwrap().unwrap().unwrap();
        cleanup(paths);
    }

    #[tokio::test]
    async fn a_query_connection_gets_a_goodbye_too() {
        let (_state_tx, state_rx, _node_tx, node_rx) = states();
        let (_stats_tx, stats_rx) = watch::channel(NodeStats::default());
        let (_ready_tx, ready_rx) = watch::channel::<Option<Ready>>(None);
        let (stop_tx, stop_rx) = watch::channel(false);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let rpc = BtcRpc::for_test(&refused_url().await);
        let server = tokio::spawn(run(listener, state_rx, node_rx, stats_rx, ready_rx, rpc, stop_rx));

        let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str()).await.unwrap();
        ws.send(Message::text(json!({ "method": "state" }).to_string())).await.unwrap();
        assert_eq!(
            next_json(&mut ws).await,
            json!({ "state": "opening", "height": null, "node": "unknown" })
        );
        stop_tx.send_replace(true);
        match next_msg(&mut ws).await {
            Message::Close(Some(frame)) => assert_eq!(frame.code, CloseCode::Away),
            other => panic!("expected a 1001 close, got {:?}", other),
        }
        tokio::time::timeout(Duration::from_secs(5), server).await.unwrap().unwrap().unwrap();
    }
    #[tokio::test]
    async fn a_consumed_coin_stays_listed_and_names_its_spender() {
        let (state_tx, state_rx, _node_tx, node_rx) = states();
        state_tx.send_replace(IndexState::Live);
        let (ready, paths) = ready();
        let spk = BtcIndex::script_pubkey_for(ADDR).unwrap();
        let rec = UtxoRecord { txid: [0xab; 32], vout: 1, sats: 5_000, height: 900_000 };
        ready.idx.insert_for_test(&spk, &rec).unwrap();
        ready.watch_state.watch(ADDR);
        let key = format!("{}:1", "ab".repeat(32));
        ready.watch_state.mark_spent(&key, TXID);
        let dead = BtcRpc::for_test(&refused_url().await);
        let req = json!({ "method": "utxos", "address": ADDR });
        let resp = answer(&req, Some(&ready), &state_rx, &node_rx, &dead).await;
        let coins = resp["utxos"].as_array().unwrap();
        assert_eq!(coins.len(), 1, "{resp}");
        assert_eq!(coins[0]["sats"], 5_000);
        assert_eq!(coins[0]["height"], 900_000);
        assert_eq!(coins[0]["spent_by"], TXID);
        cleanup(paths);
    }

    #[tokio::test]
    async fn the_scan_asks_the_node_about_unwatched_coins() {
        let (state_tx, state_rx, _node_tx, node_rx) = states();
        state_tx.send_replace(IndexState::Live);
        let (ready, paths) = ready();
        let spk = BtcIndex::script_pubkey_for(ADDR2).unwrap();
        ready.idx.insert_for_test(&spk, &UtxoRecord { txid: [0xcd; 32], vout: 0, sats: 700, height: 900_001 }).unwrap();
        ready.idx.insert_for_test(&spk, &UtxoRecord { txid: [0xef; 32], vout: 2, sats: 800, height: 900_002 }).unwrap();
        let spent = "cd".repeat(32);
        let url = fake_node(move |method, params| match method {
            "gettxspendingprevout" => {
                let asked = params[0].as_array().unwrap();
                assert_eq!(asked.len(), 2, "both confirmed coins are asked in one call");
                ok(json!([
                    { "txid": spent, "vout": 0, "spendingtxid": TXID },
                    { "txid": "ef".repeat(32), "vout": 2 }
                ]))
            }
            _ => err(-32601, "Method not found"),
        })
        .await;
        let rpc = BtcRpc::for_test(&url);
        let req = json!({ "method": "utxos_batch", "addresses": [ADDR2, ADDR] });
        let resp = answer(&req, Some(&ready), &state_rx, &node_rx, &rpc).await;
        let wallets = resp["wallets"].as_array().unwrap();
        assert_eq!(wallets.len(), 1, "{resp}");
        let coins = wallets[0]["utxos"].as_array().unwrap();
        assert_eq!(coins.len(), 2);
        let by_vout = |v: u64| coins.iter().find(|c| c["vout"] == v).unwrap().clone();
        assert_eq!(by_vout(0)["spent_by"], TXID);
        assert!(by_vout(2).get("spent_by").is_none());
        let dead = BtcRpc::for_test(&refused_url().await);
        let resp = answer(&req, Some(&ready), &state_rx, &node_rx, &dead).await;
        assert_eq!(resp["error"], "node error during utxo check", "{resp}");
        assert!(resp.get("wallets").is_none());
        let empty = json!({ "method": "utxos_batch", "addresses": [ADDR] });
        let resp = answer(&empty, Some(&ready), &state_rx, &node_rx, &dead).await;
        assert_eq!(resp["wallets"], json!([]), "{resp}");
        cleanup(paths);
    }

    #[tokio::test]
    async fn the_history_index_answers_the_window_it_holds() {
        let (state_tx, state_rx, _node_tx, node_rx) = states();
        state_tx.send_replace(IndexState::Live);
        let dead = BtcRpc::for_test(&refused_url().await);
        let req = json!({ "method": "history", "addresses": [ADDR, ADDR2] });

        let (ready, paths) = ready_at(950_000);
        let resp = answer(&req, Some(&ready), &state_rx, &node_rx, &dead).await;
        assert_eq!(resp["error"], "history index not built", "{resp}");
        assert!(answer(&json!({ "method": "state" }), Some(&ready), &state_rx, &node_rx, &dead).await["history"].is_null());
        cleanup(paths);

        let (ready, paths) = ready_at(950_000);
        let spk = BtcIndex::script_pubkey_for(ADDR).unwrap();
        let spk2 = BtcIndex::script_pubkey_for(ADDR2).unwrap();
        ready
            .history
            .seed_for_test(
                941_744,
                970_000,
                &[
                    (spk.clone(), 941_800, [0x11; 32]),
                    (spk.clone(), 960_000, [0x22; 32]),
                    (spk2.clone(), 960_000, [0x22; 32]),
                    (spk2, 969_999, [0x33; 32]),
                ],
            )
            .unwrap();
        let resp = answer(&req, Some(&ready), &state_rx, &node_rx, &dead).await;
        assert_eq!(resp["from"], 950_000, "the floor is the prune height, not the index's bottom");
        assert_eq!(resp["to"], 970_000);
        let txs = resp["transactions"].as_array().unwrap();
        assert_eq!(txs.len(), 2, "one below the floor, one shared by both addresses: {resp}");
        assert_eq!(txs[0]["height"], 969_999);
        assert_eq!(txs[0]["txid"], "33".repeat(32));
        assert_eq!(txs[1]["height"], 960_000);
        assert_eq!(resp["truncated"], false);
        let state = answer(&json!({ "method": "state" }), Some(&ready), &state_rx, &node_rx, &dead).await;
        assert_eq!(state["history"], json!({ "from": 950_000, "to": 970_000 }));

        let one = json!({ "method": "history", "addresses": [ADDR, ADDR2], "limit": 1 });
        let resp = answer(&one, Some(&ready), &state_rx, &node_rx, &dead).await;
        assert_eq!(resp["transactions"].as_array().unwrap().len(), 1);
        assert_eq!(resp["truncated"], true);
        let bad = json!({ "method": "history", "addresses": [ADDR, "nonsense"] });
        let resp = answer(&bad, Some(&ready), &state_rx, &node_rx, &dead).await;
        assert_eq!(resp["error"], "invalid address");
        state_tx.send_replace(IndexState::CatchingUp);
        let resp = answer(&req, Some(&ready), &state_rx, &node_rx, &dead).await;
        assert_eq!(resp, json!({ "state": "catching_up" }));
        cleanup(paths);
    }
}

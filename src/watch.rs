
use anyhow::{Context, Result};
use dannesk_btc_codec::address::Address;
use dannesk_btc_codec::tx::Transaction;
use dashmap::{DashMap, DashSet};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};
use zeromq::{Socket, SocketRecv, SubSocket};

use crate::btc_index::BtcIndex;
use crate::mempool::{self, MempoolIndex};
use crate::rpc::BtcRpc;

const RETRY_DELAY: Duration = Duration::from_secs(5);
const QUIET_RETRIES: Duration = Duration::from_secs(600);
const SEEN_CAP: usize = 100_000;
const UNDO_CAP: usize = 100_000;
const INPUT_FETCHES: usize = 4;

#[derive(Clone)]
pub struct WatchState {
    pub addresses: Arc<DashSet<String>>,
    spks: Arc<DashSet<Vec<u8>>>,
    tracker: Arc<DashMap<String, (String, u64)>>,
    by_address: Arc<DashMap<String, HashSet<String>>>,
    mempool: Arc<MempoolIndex>,
    enrich: Option<EnrichCtx>,
    queued: Arc<DashSet<String>>,
    seen: Arc<DashSet<String>>,
    untracked_by: Arc<DashMap<String, Vec<(String, String, u64)>>>,
    spent_by: Arc<DashMap<String, String>>,
    txids: Arc<DashSet<String>>,
    idx: BtcIndex,
}

#[derive(Clone)]
pub struct EnrichCtx {
    backlog: mpsc::UnboundedSender<String>,
}

impl WatchState {
    pub fn new(idx: BtcIndex) -> Self {
        Self {
            addresses: Arc::new(DashSet::new()),
            spks: Arc::new(DashSet::new()),
            tracker: Arc::new(DashMap::new()),
            by_address: Arc::new(DashMap::new()),
            mempool: Arc::new(MempoolIndex::default()),
            enrich: None,
            queued: Arc::new(DashSet::new()),
            seen: Arc::new(DashSet::new()),
            untracked_by: Arc::new(DashMap::new()),
            spent_by: Arc::new(DashMap::new()),
            txids: Arc::new(DashSet::new()),
            idx,
        }
    }

    pub fn with_enrichment(mut self, rpc: BtcRpc, txs: broadcast::Sender<String>) -> Self {
        let (backlog, queue) = mpsc::unbounded_channel();
        self.enrich = Some(EnrichCtx { backlog });
        let mut worker = self.clone();
        worker.enrich = None;
        spawn_enricher(worker, rpc, txs, queue);
        self
    }

    pub fn mempool(&self) -> Arc<MempoolIndex> {
        Arc::clone(&self.mempool)
    }

    pub fn spks(&self) -> Arc<DashSet<Vec<u8>>> {
        Arc::clone(&self.spks)
    }

    pub fn txids(&self) -> Arc<DashSet<String>> {
        Arc::clone(&self.txids)
    }

    pub fn watch_txid(&self, txid: &str) {
        self.txids.insert(txid.to_string());
    }

    pub fn unwatch_txid(&self, txid: &str) {
        self.txids.remove(txid);
    }

    pub fn spender_of(&self, key: &str) -> Option<String> {
        self.spent_by.get(key).map(|e| e.value().clone())
    }

    pub(crate) fn mark_spent(&self, key: &str, txid: &str) {
        self.spent_by.insert(key.to_string(), txid.to_string());
    }

    fn unmark_spent(&self, key: &str, txid: &str) {
        self.spent_by.remove_if(key, |_, spender| spender == txid);
    }

    pub fn track(&self, key: String, address: &str, sats: u64) {
        let previous = self.tracker.insert(key.clone(), (address.to_string(), sats));
        if let Some((old_address, _)) = previous
            && old_address != address
        {
            self.forget_key(&old_address, &key);
        }
        self.by_address
            .entry(address.to_string())
            .or_default()
            .insert(key);
    }

    pub fn untrack(&self, key: &str) {
        if let Some((_, (address, _))) = self.tracker.remove(key) {
            self.forget_key(&address, key);
        }
    }

    fn forget_key(&self, address: &str, key: &str) {
        let emptied = match self.by_address.get_mut(address) {
            Some(mut keys) => {
                keys.remove(key);
                keys.is_empty()
            }
            None => false,
        };
        if emptied {
            self.by_address.remove_if(address, |_, keys| keys.is_empty());
        }
    }

    pub fn watch(&self, address: &str) {
        if !self.addresses.insert(address.to_string()) {
            return;
        }
        if let Some(spk) = BtcIndex::script_pubkey_for(address) {
            self.spks.insert(spk);
        }
        self.seed(address);
        if let Some(ctx) = &self.enrich
            && self.queued.insert(address.to_string())
        {
            let _ = ctx.backlog.send(address.to_string());
        }
        debug!("watching {} ({} tracked outpoints total)", address, self.tracker.len());
    }

    pub fn unwatch(&self, address: &str) {
        self.addresses.remove(address);
        if let Some(spk) = BtcIndex::script_pubkey_for(address) {
            self.spks.remove(&spk);
        }
        if let Some((_, keys)) = self.by_address.remove(address) {
            for key in keys {
                self.tracker.remove(&key);
            }
        }
        let mut marked: Vec<String> = Vec::new();
        self.untracked_by.retain(|_, entries| {
            entries.retain(|(key, owner, _)| {
                if owner == address {
                    marked.push(key.clone());
                    false
                } else {
                    true
                }
            });
            !entries.is_empty()
        });
        for key in marked {
            self.spent_by.remove(&key);
        }
        debug!("unwatched {}", address);
    }

    fn seed(&self, address: &str) {
        let Some(spk) = BtcIndex::script_pubkey_for(address) else {
            return;
        };
        match self.idx.utxos_for_spk(&spk) {
            Ok(records) => {
                for rec in records {
                    let txid: String =
                        rec.txid.iter().rev().map(|b| format!("{:02x}", b)).collect();
                    self.track(format!("{}:{}", txid, rec.vout), address, rec.sats);
                }
            }
            Err(e) => warn!("tracker seed failed for {}: {:?}", address, e),
        }
    }

    pub fn reseed_all(&self) {
        for addr in self.addresses.iter() {
            self.seed(&addr);
        }
        info!("tracker reseeded for {} watched addresses", self.addresses.len());
    }

    pub fn unconfirmed_for(&self, address: &str, confirmed: &HashSet<String>) -> Vec<Value> {
        let keys: Vec<String> = match self.by_address.get(address) {
            Some(keys) => keys
                .iter()
                .filter(|k| !confirmed.contains(*k))
                .cloned()
                .collect(),
            None => return Vec::new(),
        };
        keys.into_iter()
            .filter_map(|key| {
                let sats = self.tracker.get(&key)?.value().1;
                let (txid, vout) = key.split_once(':')?;
                Some(json!({
                    "txid": txid,
                    "vout": vout.parse::<u32>().ok()?,
                    "sats": sats,
                    "height": 0
                }))
            })
            .collect()
    }
}

pub fn spawn_rawtx(
    state: WatchState,
    rpc: BtcRpc,
    zmq_endpoint: String,
    events: broadcast::Sender<String>,
) {
    spawn_drop_consumer(state.clone(), rpc.clone(), events.clone(), state.mempool.drop_queue());
    tokio::spawn(async move {
        let mut quiet_until: Option<tokio::time::Instant> = None;
        loop {
            if let Err(e) = follow_rawtx(&state, &rpc, &zmq_endpoint, &events).await {
                let now = tokio::time::Instant::now();
                if quiet_until.is_none_or(|t| now >= t) {
                    warn!("rawtx follower error (retrying every {:?}, repeats at debug for 10 min): {:?}", RETRY_DELAY, e);
                    quiet_until = Some(now + QUIET_RETRIES);
                } else {
                    debug!("rawtx follower error, retrying: {:?}", e);
                }
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    });
}

// A drop a reconciliation found runs the same undo as a ZMQ drop, tells relay if it had heard of the transaction.
fn spawn_drop_consumer(
    state: WatchState,
    rpc: BtcRpc,
    events: broadcast::Sender<String>,
    mut drops: mpsc::UnboundedReceiver<String>,
) {
    tokio::spawn(async move {
        while let Some(txid) = drops.recv().await {
            undo_dropped_tx(&state, &rpc, &txid).await;
            if state.seen.remove(&txid).is_some() {
                debug!("mempool drop (reconciled): {}", txid);
                let _ = events.send(json!({ "type": "dropped_tx", "txid": txid }).to_string());
            }
        }
    });
}

async fn follow_rawtx(
    state: &WatchState,
    rpc: &BtcRpc,
    zmq_endpoint: &str,
    events: &broadcast::Sender<String>,
) -> Result<()> {
    let mut sub = SubSocket::new();
    sub.connect(zmq_endpoint)
        .await
        .with_context(|| format!("ZMQ connect failed: {}", zmq_endpoint))?;
    sub.subscribe("rawtx").await.context("ZMQ subscribe failed")?;
    sub.subscribe("sequence").await.context("ZMQ subscribe failed")?;
    info!("rawtx follower connected to {}", zmq_endpoint);
    // Subscribed first, then listed so that nothing falls between the two.
    state.mempool.request_walk();
    let (mut rawtx_gap, mut sequence_gap) = (Gap::default(), Gap::default());

    loop {
        let msg = sub.recv().await.context("ZMQ recv failed")?;
        let frames = msg.into_vec();
        let Some(topic) = frames.first() else { continue };
        let Some(payload) = frames.get(1) else { continue };
        let counter = match topic.as_ref() {
            b"rawtx" => Some(&mut rawtx_gap),
            b"sequence" => Some(&mut sequence_gap),
            _ => None,
        };
        if let Some(counter) = counter
            && let Some(seq) = frames.get(2).and_then(|f| <[u8; 4]>::try_from(f.as_ref()).ok())
            && let Some(previous) = counter.noticed(u32::from_le_bytes(seq))
        {
            info!(
                "ZMQ {} frames jumped from {} to {}, reconciling the mempool mirror",
                String::from_utf8_lossy(topic),
                previous,
                u32::from_le_bytes(seq)
            );
            state.mempool.request_walk();
        }

        if topic.as_ref() == b"sequence" {
            if let Some(txid) = dropped_txid(payload) {
                undo_dropped_tx(state, rpc, &txid).await;
                if state.seen.remove(&txid).is_some() {
                    debug!("mempool drop: {}", txid);
                    let _ = events.send(
                        json!({ "type": "dropped_tx", "txid": txid }).to_string(),
                    );
                }
            }
            continue;
        }
        if topic.as_ref() != b"rawtx" {
            continue;
        }

        let Ok(tx) = Transaction::deserialize(payload) else {
            debug!("undecodable rawtx frame ({} bytes)", payload.len());
            continue;
        };

        let txid = tx.txid().to_string();
        state.mempool.insert_live(&txid, mempool::output_spks(&tx), mempool::input_prevouts(&tx));

        let mut matched = false;
        for out in &tx.output {
            if let Some(addr) = Address::from_script(&out.script_pubkey)
                && state.addresses.contains(&addr.to_string())
            {
                matched = true;
                break;
            }
        }
        if !matched {
            for input in &tx.input {
                let key = format!(
                    "{}:{}",
                    input.previous_output.txid, input.previous_output.vout
                );
                if state.tracker.contains_key(&key) {
                    matched = true;
                    break;
                }
            }
        }
        if !matched && !state.txids.is_empty() && state.txids.contains(&txid) {
            matched = true;
        }
        if !matched {
            continue;
        }

        if state.seen.len() >= SEEN_CAP {
            state.seen.clear();
        }
        if !state.seen.insert(txid.clone()) {
            continue;
        }

        let frame = build_mempool_frame(state, rpc, &tx, &txid).await;
        debug!("mempool match: {}", txid);
        let _ = events.send(frame);
    }
}

#[derive(Default)]
struct Gap {
    last: Option<u32>,
}

impl Gap {
    /// One topic's frames are numbered consecutively so a jump is the publisher's queue overflowing, or the node restarting.
    fn noticed(&mut self, seq: u32) -> Option<u32> {
        let jumped = match self.last {
            Some(last) if seq != last.wrapping_add(1) => Some(last),
            _ => None,
        };
        self.last = Some(seq);
        jumped
    }
}

fn dropped_txid(body: &[u8]) -> Option<String> {
    if body.len() < 33 || body[32] != b'R' {
        return None;
    }
    Some(body[..32].iter().map(|b| format!("{:02x}", b)).collect())
}

async fn resolve_input(
    state: &WatchState,
    rpc: &BtcRpc,
    prev_txid: String,
    vout: u32,
) -> (String, Option<String>, Option<u64>) {
    let key = format!("{}:{}", prev_txid, vout);
    let tracked = state.tracker.get(&key).map(|entry| entry.value().clone());
    if let Some((address, sats)) = tracked {
        return (key, Some(address), Some(sats));
    }
    match rpc.call("gettxout", json!([prev_txid, vout, false])).await {
        Ok(out) if !out.is_null() => (
            key,
            out["scriptPubKey"]["address"].as_str().map(|s| s.to_string()),
            out["value"].as_f64().map(|v| (v * 1e8).round() as u64),
        ),
        _ => (key, None, None),
    }
}

async fn build_mempool_frame(
    state: &WatchState,
    rpc: &BtcRpc,
    tx: &Transaction,
    txid: &str,
) -> String {
    // Inputs the tracker knows cost nothing; the rest are asked of the node a few at a time, answers kept in order.
    let outpoints: Vec<(String, u32)> = tx
        .input
        .iter()
        .map(|input| (input.previous_output.txid.to_string(), input.previous_output.vout))
        .collect();
    let resolved: Vec<(String, Option<String>, Option<u64>)> = stream::iter(outpoints)
        .map(|(prev_txid, vout)| resolve_input(state, rpc, prev_txid, vout))
        .buffered(INPUT_FETCHES)
        .collect()
        .await;
    let mut inputs = Vec::with_capacity(tx.input.len());
    let mut untracked: Vec<(String, String, u64)> = Vec::new();
    for (input, (key, address, sats)) in tx.input.iter().zip(resolved) {
        if let (Some(a), Some(s)) = (address.as_deref(), sats)
            && state.addresses.contains(a)
        {
            state.untrack(&key);
            state.mark_spent(&key, txid);
            untracked.push((key.clone(), a.to_string(), s));
        }
        inputs.push(json!({
            "txid": input.previous_output.txid.to_string(),
            "vout": input.previous_output.vout,
            "address": address,
            "sats": sats,
        }));
    }
    if !untracked.is_empty() {
        if state.untracked_by.len() >= UNDO_CAP {
            warn!("untracked_by hit {} entries — clearing (leak backstop)", UNDO_CAP);
            state.untracked_by.clear();
            state.spent_by.clear();
        }
        // Merged, not replaced, because a rebuild that fails one lookup must not shrink the undo record.
        let mut entry = state.untracked_by.entry(txid.to_string()).or_default();
        for item in untracked {
            if !entry.iter().any(|(key, _, _)| *key == item.0) {
                entry.push(item);
            }
        }
    }

    let mut outputs = Vec::with_capacity(tx.output.len());
    for (vout, out) in tx.output.iter().enumerate() {
        let address = Address::from_script(&out.script_pubkey).map(|a| a.to_string());
        let sats = out.value;
        if let Some(a) = address.as_deref()
            && state.addresses.contains(a)
        {
            state.track(format!("{}:{}", txid, vout), a, sats);
        }
        outputs.push(json!({ "address": address, "sats": sats, "vout": vout }));
    }

    let (fees, vsize) = match rpc.call("getmempoolentry", json!([txid])).await {
        Ok(entry) => (
            entry["fees"]["base"]
                .as_f64()
                .map(|f| ((f * 1e8).round() as u64).to_string())
                .unwrap_or_else(|| "0".to_string()),
            // vsize deprecated in version 32
            entry["vsize_bip141"].as_u64().or_else(|| entry["vsize"].as_u64()),
        ),
        Err(_) => ("0".to_string(), None),
    };

    json!({
        "type": "mempool_tx",
        "txid": txid,
        "fees": fees,
        "vsize": vsize,
        "inputs": inputs,
        "outputs": outputs
    })
    .to_string()
}

async fn undo_dropped_tx(state: &WatchState, rpc: &BtcRpc, txid: &str) {
    let paid_spks = state.mempool.evict(txid);
    if paid_spks.iter().any(|s| state.spks.contains(s)) {
        let prefix = format!("{}:", txid);
        let phantoms: Vec<String> = state
            .tracker
            .iter()
            .filter(|e| e.key().starts_with(&prefix))
            .map(|e| e.key().clone())
            .collect();
        for key in phantoms {
            debug!("drop {}: untracking phantom output {}", txid, key);
            state.untrack(&key);
        }
    }
    let Some((_, entries)) = state.untracked_by.remove(txid) else {
        return;
    };
    for (key, address, sats) in entries {
        state.unmark_spent(&key, txid);
        let Some((prev_txid, vout)) = key.split_once(':') else { continue };
        let Ok(vout) = vout.parse::<u32>() else { continue };
        match rpc.call("gettxout", json!([prev_txid, vout, true])).await {
            Ok(out) if !out.is_null() => {
                debug!("drop {}: restoring input {} to {}", txid, key, address);
                state.track(key.clone(), &address, sats);
            }
            Ok(_) => {}
            Err(e) => warn!("drop {}: gettxout {} failed, leaving untracked: {:?}", txid, key, e),
        }
    }
}

// One worker: a relay's full address list after a reconnect is answered from the mirror, never fanned out to the node.
fn spawn_enricher(
    state: WatchState,
    rpc: BtcRpc,
    txs: broadcast::Sender<String>,
    mut queue: mpsc::UnboundedReceiver<String>,
) {
    tokio::spawn(async move {
        while let Some(address) = queue.recv().await {
            state.queued.remove(&address);
            state.mempool.warmed().await;
            if !state.addresses.contains(&address) {
                continue;
            }
            enrich_watch(&state, &rpc, &txs, &address).await;
        }
    });
}

fn mempool_matches(state: &WatchState, address: &str) -> HashSet<String> {
    let keys: Vec<String> = state
        .by_address
        .get(address)
        .map(|keys| keys.iter().cloned().collect())
        .unwrap_or_default();
    let mut txids: HashSet<String> = keys
        .iter()
        .filter_map(|key| state.mempool.spender_of(key))
        .collect();
    if let Some(spk) = BtcIndex::script_pubkey_for(address) {
        txids.extend(state.mempool.txids_for_spk(&spk));
    }
    txids
}

// Parents first because a child's frame must find the coin its parent's frame tracked.
fn parents_first(state: &WatchState, pending: &HashSet<String>) -> Vec<String> {
    let mut ready: Vec<String> = pending
        .iter()
        .filter(|txid| state.mempool.parents_in_mempool(txid).iter().all(|parent| !pending.contains(parent)))
        .cloned()
        .collect();
    if ready.is_empty() {
        ready = pending.iter().cloned().collect();
    }
    ready.sort();
    ready
}

// Each frame can track new coins of the address, whose own spenders are then matches too.. run to the fixpoint.
async fn enrich_watch(state: &WatchState, rpc: &BtcRpc, txs: &broadcast::Sender<String>, address: &str) {
    let mut done: HashSet<String> = HashSet::new();
    loop {
        let pending: HashSet<String> = mempool_matches(state, address)
            .into_iter()
            .filter(|txid| !done.contains(txid))
            .collect();
        if pending.is_empty() {
            return;
        }
        for txid in parents_first(state, &pending) {
            done.insert(txid.clone());
            let Ok(hex) = rpc.call("getrawtransaction", json!([txid])).await else {
                debug!("enrichment tx {} gone from mempool mid-task", txid);
                continue;
            };
            let Some(tx) = hex
                .as_str()
                .and_then(|h| Transaction::from_hex(h).ok())
            else {
                continue;
            };
            let frame = build_mempool_frame(state, rpc, &tx, &txid).await;
            debug!("watch-time mempool match for {}: {}", address, txid);
            let _ = txs.send(frame);
        }
    }
}

pub fn spawn_block_consumer(state: WatchState, mut blocks: broadcast::Receiver<String>) {
    tokio::spawn(async move {
        loop {
            match blocks.recv().await {
                Ok(frame) => {
                    let Ok(event) = serde_json::from_str::<Value>(&frame) else { continue };
                    match event["type"].as_str() {
                        Some("block") => {
                            evict_confirmed_from_mempool_index(&state, &event);
                            apply_block_to_tracker(&state, &event);
                        }
                        Some("reorg") => state.reseed_all(),
                        _ => {}
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("tracker lagged {} block events, reseeding", n);
                    state.reseed_all();
                    state.mempool.request_walk();
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    });
}

fn evict_confirmed_from_mempool_index(state: &WatchState, event: &Value) {
    for change in event["changes"].as_array().into_iter().flatten() {
        for rec in change["created"].as_array().into_iter().flatten() {
            if let Some(txid) = rec["txid"].as_str() {
                state.mempool.evict(txid);
                if let Some((_, entries)) = state.untracked_by.remove(txid) {
                    for (key, _, _) in entries {
                        state.unmark_spent(&key, txid);
                    }
                }
            }
        }
    }
}

fn apply_block_to_tracker(state: &WatchState, event: &Value) {
    let Some(changes) = event["changes"].as_array() else { return };
    for change in changes {
        let Some(address) = change["address"].as_str() else { continue };
        if !state.addresses.contains(address) {
            continue;
        }
        for rec in change["spent"].as_array().into_iter().flatten() {
            if let (Some(txid), Some(vout)) = (rec["txid"].as_str(), rec["vout"].as_u64()) {
                let key = format!("{}:{}", txid, vout);
                state.untrack(&key);
                state.spent_by.remove(&key);
            }
        }
        for rec in change["created"].as_array().into_iter().flatten() {
            if let (Some(txid), Some(vout), Some(sats)) = (
                rec["txid"].as_str(),
                rec["vout"].as_u64(),
                rec["sats"].as_u64(),
            ) {
                state.track(format!("{}:{}", txid, vout), address, sats);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btc_index::BtcIndex;

    const A: &str = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4";
    const B: &str = "12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX";

    fn watch_state() -> (WatchState, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "watch-test-{}-{:?}.redb",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        let idx = BtcIndex::open(path.to_str().unwrap()).unwrap();
        (WatchState::new(idx), path)
    }

    fn assert_consistent(s: &WatchState) {
        for entry in s.tracker.iter() {
            let address = &entry.value().0;
            assert!(
                s.by_address
                    .get(address)
                    .is_some_and(|keys| keys.contains(entry.key())),
                "tracker has {} but by_address[{}] does not",
                entry.key(),
                address
            );
        }
        for entry in s.by_address.iter() {
            assert!(
                !entry.value().is_empty(),
                "by_address[{}] is an empty set that was never reclaimed",
                entry.key()
            );
            for key in entry.value().iter() {
                assert!(
                    s.tracker.contains_key(key),
                    "by_address[{}] holds stale key {}",
                    entry.key(),
                    key
                );
            }
        }
    }

    #[test]
    fn watch_time_matches_come_from_the_mirror_alone() {
        let (s, path) = watch_state();
        let coin = format!("{}:0", "aa".repeat(32));
        s.track(coin.clone(), A, 100);
        s.mempool.insert_live("spender", vec![], vec![mempool::outpoint_key(&coin).unwrap()]);
        s.mempool.insert_live("payer", vec![BtcIndex::script_pubkey_for(A).unwrap()], vec![]);
        let other = format!("{}:1", "bb".repeat(32));
        s.mempool.insert_live("stranger", vec![BtcIndex::script_pubkey_for(B).unwrap()], vec![mempool::outpoint_key(&other).unwrap()]);
        let mut found: Vec<String> = mempool_matches(&s, A).into_iter().collect();
        found.sort();
        assert_eq!(found, vec!["payer".to_string(), "spender".to_string()]);
        assert!(mempool_matches(&s, "bc1qnothing").is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_real_transactions_input_finds_the_tracked_coin() {
        use dannesk_btc_codec::tx::{OutPoint, SEQUENCE_FINAL, Transaction, TxIn, TxOut, Txid};
        let (s, path) = watch_state();
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = i as u8;
        }
        let txid = Txid::from_bytes(bytes);
        s.track(format!("{}:3", txid), A, 100);
        let tx = Transaction {
            version: 2,
            lock_time: 0,
            input: vec![TxIn {
                previous_output: OutPoint { txid, vout: 3 },
                script_sig: vec![],
                sequence: SEQUENCE_FINAL,
                witness: vec![],
            }],
            output: vec![TxOut { value: 90, script_pubkey: BtcIndex::script_pubkey_for(B).unwrap() }],
        };
        s.mempool.insert_live("spender", mempool::output_spks(&tx), mempool::input_prevouts(&tx));
        assert_eq!(mempool_matches(&s, A), HashSet::from(["spender".to_string()]));
        assert!(mempool_matches(&s, B).contains("spender"), "the payee side matches by script");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_parent_is_enriched_before_its_child() {
        use dannesk_btc_codec::tx::{OutPoint, Txid};
        let (s, path) = watch_state();
        let parent = Txid::from_bytes([0x5a; 32]);
        let parent_id = parent.to_string();
        let pays_a = || vec![BtcIndex::script_pubkey_for(A).unwrap()];
        let confirmed = mempool::outpoint_key(&format!("{}:0", "cc".repeat(32))).unwrap();
        s.mempool.insert_live(&parent_id, pays_a(), vec![confirmed]);
        s.mempool.insert_live("child", pays_a(), vec![mempool::prevout_key(&OutPoint { txid: parent, vout: 0 })]);
        let both: HashSet<String> = [parent_id.clone(), "child".to_string()].into_iter().collect();
        assert_eq!(parents_first(&s, &both), vec![parent_id.clone()], "the child waits for its parent");
        let child_alone: HashSet<String> = ["child".to_string()].into_iter().collect();
        assert_eq!(parents_first(&s, &child_alone), vec!["child".to_string()], "a parent already done is no bar");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_jump_in_the_zmq_counter_is_noticed_once() {
        let mut g = Gap::default();
        assert_eq!(g.noticed(5), None, "the first frame sets the baseline");
        assert_eq!(g.noticed(6), None);
        assert_eq!(g.noticed(9), Some(6), "7 and 8 were dropped");
        assert_eq!(g.noticed(10), None);
        assert_eq!(g.noticed(0), Some(10), "the publisher restarted");
        let mut w = Gap::default();
        w.noticed(u32::MAX);
        assert_eq!(w.noticed(0), None, "wrap-around is consecutive");
    }

    #[test]
    fn track_and_untrack_keep_both_indexes_consistent() {
        let (s, path) = watch_state();
        s.track("aa:0".into(), A, 100);
        s.track("aa:1".into(), A, 200);
        s.track("bb:0".into(), B, 300);
        assert_consistent(&s);
        assert_eq!(s.tracker.len(), 3);

        s.untrack("aa:0");
        assert_consistent(&s);
        assert_eq!(s.tracker.len(), 2);
        assert!(!s.by_address.get(A).unwrap().contains("aa:0"));

        s.untrack("never-seen:7");
        assert_consistent(&s);
        assert_eq!(s.tracker.len(), 2);

        s.track("aa:1".into(), A, 999);
        assert_consistent(&s);
        assert_eq!(s.tracker.len(), 2);
        assert_eq!(s.by_address.get(A).unwrap().len(), 1);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn retracking_to_a_new_address_moves_the_key() {
        let (s, path) = watch_state();
        s.track("aa:0".into(), A, 100);
        s.track("aa:0".into(), B, 100);
        assert_consistent(&s);
        assert_eq!(s.tracker.get("aa:0").unwrap().value().0, B);
        assert!(
            s.by_address.get(A).is_none(),
            "A's set should have emptied and been reclaimed"
        );
        assert!(s.by_address.get(B).unwrap().contains("aa:0"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn emptied_address_sets_are_reclaimed() {
        let (s, path) = watch_state();
        s.track("aa:0".into(), A, 100);
        s.untrack("aa:0");
        assert_consistent(&s);
        assert!(
            s.by_address.get(A).is_none(),
            "spending an address's last coin must not leak a map entry"
        );
        assert!(s.tracker.is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn the_script_set_tracks_the_address_set() {
        let (s, path) = watch_state();
        let spk_a = BtcIndex::script_pubkey_for(A).unwrap();
        let spk_b = BtcIndex::script_pubkey_for(B).unwrap();

        s.watch(A);
        s.watch(B);
        assert!(s.spks().contains(&spk_a));
        assert!(s.spks().contains(&spk_b));
        assert_eq!(s.spks().len(), s.addresses.len());

        s.watch(A);
        assert_eq!(s.spks().len(), 2);

        s.unwatch(A);
        assert!(!s.spks().contains(&spk_a), "A's script must go with A");
        assert!(s.spks().contains(&spk_b), "B's must not");
        assert_eq!(s.spks().len(), s.addresses.len());

        s.watch("not-an-address");
        assert_eq!(s.spks().len(), 1);
        s.unwatch("not-an-address");
        assert_eq!(s.spks().len(), 1);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn unwatch_drops_only_that_addresses_outpoints() {
        let (s, path) = watch_state();
        s.addresses.insert(A.to_string());
        s.addresses.insert(B.to_string());
        s.track("aa:0".into(), A, 100);
        s.track("aa:1".into(), A, 200);
        s.track("bb:0".into(), B, 300);

        s.unwatch(A);
        assert_consistent(&s);
        assert!(!s.addresses.contains(A));
        assert_eq!(s.tracker.len(), 1);
        assert!(s.tracker.contains_key("bb:0"), "B's coin must survive");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn unwatch_clears_the_marks_of_its_consumed_coins() {
        let (s, path) = watch_state();
        s.addresses.insert(A.to_string());
        s.addresses.insert(B.to_string());
        s.untracked_by.insert(
            "t1".to_string(),
            vec![("aa:0".to_string(), A.to_string(), 5), ("bb:0".to_string(), B.to_string(), 7)],
        );
        s.untracked_by.insert("t2".to_string(), vec![("aa:1".to_string(), A.to_string(), 9)]);
        for key in ["aa:0", "aa:1"] {
            s.mark_spent(key, "t1");
        }
        s.mark_spent("bb:0", "t1");

        s.unwatch(A);
        assert_eq!(s.spender_of("aa:0"), None);
        assert_eq!(s.spender_of("aa:1"), None);
        assert_eq!(s.spender_of("bb:0").as_deref(), Some("t1"), "B's mark is not A's to drop");
        assert!(!s.untracked_by.contains_key("t2"), "an entry left empty goes too");
        assert_eq!(s.untracked_by.get("t1").unwrap().value().len(), 1, "B's coin stays in the shared entry");
        assert_consistent(&s);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn unconfirmed_for_is_address_scoped_and_skips_confirmed() {
        let (s, path) = watch_state();
        s.track("aa:0".into(), A, 100);
        s.track("aa:1".into(), A, 200);
        s.track("bb:0".into(), B, 300);

        let confirmed: HashSet<String> = ["aa:0".to_string()].into_iter().collect();
        let out = s.unconfirmed_for(A, &confirmed);
        assert_eq!(out.len(), 1, "aa:0 is confirmed, bb:0 belongs to B");
        assert_eq!(out[0]["txid"], "aa");
        assert_eq!(out[0]["vout"], 1);
        assert_eq!(out[0]["sats"], 200);
        assert_eq!(out[0]["height"], 0);

        assert!(
            s.unconfirmed_for("never-watched", &HashSet::new()).is_empty(),
            "unknown address must not scan or panic"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_spend_mark_follows_the_undo_entry() {
        let (s, path) = watch_state();
        s.mark_spent("aa:0", "t1");
        assert_eq!(s.spender_of("aa:0").as_deref(), Some("t1"));
        s.mark_spent("aa:0", "t2");
        s.unmark_spent("aa:0", "t1");
        assert_eq!(s.spender_of("aa:0").as_deref(), Some("t2"));
        s.unmark_spent("aa:0", "t2");
        assert_eq!(s.spender_of("aa:0"), None);

        s.untracked_by.insert("t4".to_string(), vec![("cc:0".to_string(), A.to_string(), 5)]);
        s.mark_spent("cc:0", "t4");
        evict_confirmed_from_mempool_index(
            &s,
            &json!({ "changes": [{ "address": A, "created": [{ "txid": "t4", "vout": 0, "sats": 1 }], "spent": [] }] }),
        );
        assert_eq!(s.spender_of("cc:0"), None);
        assert!(!s.untracked_by.contains_key("t4"));

        s.addresses.insert(A.to_string());
        s.track("bb:1".to_string(), A, 10);
        s.mark_spent("bb:1", "t3");
        apply_block_to_tracker(
            &s,
            &json!({ "changes": [{ "address": A, "spent": [{ "txid": "bb", "vout": 1 }], "created": [] }] }),
        );
        assert_eq!(s.spender_of("bb:1"), None);
        assert!(!s.tracker.contains_key("bb:1"));
        assert_consistent(&s);
        let _ = std::fs::remove_file(path);
    }
}

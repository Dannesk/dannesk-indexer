
use anyhow::{Context, Result};
use dannesk_btc_codec::address::Address;
use dannesk_btc_codec::tx::Transaction;
use dashmap::{DashMap, DashSet};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};
use zeromq::{Socket, SocketRecv, SubSocket};

use crate::btc_index::BtcIndex;
use crate::mempool::{self, MempoolIndex};
use crate::rpc::BtcRpc;

const RETRY_DELAY: Duration = Duration::from_secs(5);
const QUIET_RETRIES: Duration = Duration::from_secs(600);
const SEEN_CAP: usize = 100_000;
const UNDO_CAP: usize = 100_000;

#[derive(Clone)]
pub struct WatchState {
    pub addresses: Arc<DashSet<String>>,
    spks: Arc<DashSet<Vec<u8>>>,
    tracker: Arc<DashMap<String, (String, u64)>>,
    by_address: Arc<DashMap<String, HashSet<String>>>,
    mempool: Arc<MempoolIndex>,
    enrich: Option<EnrichCtx>,
    untracked_by: Arc<DashMap<String, Vec<(String, String, u64)>>>,
    spent_by: Arc<DashMap<String, String>>,
    txids: Arc<DashSet<String>>,
    idx: BtcIndex,
}

#[derive(Clone)]
pub struct EnrichCtx {
    rpc: BtcRpc,
    txs: broadcast::Sender<String>,
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
            untracked_by: Arc::new(DashMap::new()),
            spent_by: Arc::new(DashMap::new()),
            txids: Arc::new(DashSet::new()),
            idx,
        }
    }

    pub fn with_enrichment(mut self, rpc: BtcRpc, txs: broadcast::Sender<String>) -> Self {
        self.enrich = Some(EnrichCtx { rpc, txs });
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
        if let Some(ctx) = self.enrich.clone() {
            let state = self.clone();
            let address = address.to_string();
            tokio::spawn(async move { enrich_watch(state, ctx, address).await });
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
    tokio::spawn(async move {
        let mut seen: HashSet<String> = HashSet::new();
        let mut quiet_until: Option<tokio::time::Instant> = None;
        loop {
            if let Err(e) = follow_rawtx(&state, &rpc, &zmq_endpoint, &events, &mut seen).await {
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

async fn follow_rawtx(
    state: &WatchState,
    rpc: &BtcRpc,
    zmq_endpoint: &str,
    events: &broadcast::Sender<String>,
    seen: &mut HashSet<String>,
) -> Result<()> {
    let mut sub = SubSocket::new();
    sub.connect(zmq_endpoint)
        .await
        .with_context(|| format!("ZMQ connect failed: {}", zmq_endpoint))?;
    sub.subscribe("rawtx").await.context("ZMQ subscribe failed")?;
    sub.subscribe("sequence").await.context("ZMQ subscribe failed")?;
    info!("rawtx follower connected to {}", zmq_endpoint);

    loop {
        let msg = sub.recv().await.context("ZMQ recv failed")?;
        let frames = msg.into_vec();
        let Some(topic) = frames.first() else { continue };
        let Some(payload) = frames.get(1) else { continue };

        if topic.as_ref() == b"sequence" {
            if let Some(txid) = dropped_txid(payload) {
                undo_dropped_tx(state, rpc, &txid).await;
                if seen.remove(&txid) {
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
        state.mempool.insert(&txid, mempool::output_spks(&tx));

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

        if seen.len() >= SEEN_CAP {
            seen.clear();
        }
        if !seen.insert(txid.clone()) {
            continue;
        }

        let frame = build_mempool_frame(state, rpc, &tx, &txid).await;
        debug!("mempool match: {}", txid);
        let _ = events.send(frame);
    }
}

fn dropped_txid(body: &[u8]) -> Option<String> {
    if body.len() < 33 || body[32] != b'R' {
        return None;
    }
    Some(body[..32].iter().map(|b| format!("{:02x}", b)).collect())
}

async fn build_mempool_frame(
    state: &WatchState,
    rpc: &BtcRpc,
    tx: &Transaction,
    txid: &str,
) -> String {
    let mut inputs = Vec::with_capacity(tx.input.len());
    let mut untracked: Vec<(String, String, u64)> = Vec::new();
    for input in &tx.input {
        let key = format!(
            "{}:{}",
            input.previous_output.txid, input.previous_output.vout
        );
        let (address, sats) = match state.tracker.get(&key) {
            Some(entry) => {
                let (a, s) = entry.value().clone();
                (Some(a), Some(s))
            }
            None => match rpc
                .call(
                    "gettxout",
                    json!([input.previous_output.txid.to_string(), input.previous_output.vout, false]),
                )
                .await
            {
                Ok(out) if !out.is_null() => (
                    out["scriptPubKey"]["address"].as_str().map(|s| s.to_string()),
                    out["value"].as_f64().map(|v| (v * 1e8).round() as u64),
                ),
                _ => (None, None),
            },
        };
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
        state.untracked_by.insert(txid.to_string(), untracked);
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
            entry["vsize"].as_u64(),
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

async fn enrich_watch(state: WatchState, ctx: EnrichCtx, address: String) {
    let mut txids: HashSet<String> = HashSet::new();

    let outpoints: Vec<Value> = match state.by_address.get(&address) {
        Some(keys) => keys
            .iter()
            .filter_map(|k| {
                let (txid, vout) = k.split_once(':')?;
                Some(json!({ "txid": txid, "vout": vout.parse::<u32>().ok()? }))
            })
            .collect(),
        None => Vec::new(),
    };
    if !outpoints.is_empty() {
        match ctx.rpc.call("gettxspendingprevout", json!([outpoints])).await {
            Ok(results) => {
                for entry in results.as_array().into_iter().flatten() {
                    if let Some(spender) = entry["spendingtxid"].as_str() {
                        txids.insert(spender.to_string());
                    }
                }
            }
            Err(e) => warn!("gettxspendingprevout failed for {}: {:?}", address, e),
        }
    }

    if state.mempool.is_warm()
        && let Some(spk) = BtcIndex::script_pubkey_for(&address)
    {
        txids.extend(state.mempool.txids_for_spk(&spk));
    }

    for txid in txids {
        let Ok(hex) = ctx.rpc.call("getrawtransaction", json!([txid])).await else {
            debug!("enrichment tx {} gone from mempool mid-task", txid);
            continue;
        };
        let Some(tx) = hex
            .as_str()
            .and_then(|h| Transaction::from_hex(h).ok())
        else {
            continue;
        };
        let frame = build_mempool_frame(&state, &ctx.rpc, &tx, &txid).await;
        debug!("watch-time mempool match for {}: {}", address, txid);
        let _ = ctx.txs.send(frame);
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

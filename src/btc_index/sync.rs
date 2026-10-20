
use anyhow::{Context, Result, bail};
use dashmap::DashSet;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tracing::{debug, error, info, warn};
use zeromq::{Socket, SocketRecv, SubSocket};

use super::store::{META, META_TIP, UNDO, UNDO_DEPTH, UTXOS, UndoRow, UtxoRecord};
use super::history::{self, HistoryBlock, HistoryIndex};
use super::used::{self, AppliedBlock, UsedSet};
use super::{BtcIndex, IndexState};
use crate::rpc::BtcRpc;

const GETBLOCK_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_FALLBACK: Duration = Duration::from_secs(60);
const RETRY_DELAY: Duration = Duration::from_secs(5);
const QUIET_RETRIES: Duration = Duration::from_secs(600);

#[derive(Clone)]
pub struct SyncEvents {
    pub blocks: broadcast::Sender<String>,
    pub txs: broadcast::Sender<String>,
    pub watched: Arc<DashSet<Vec<u8>>>,
    pub txids: Arc<DashSet<String>>,
}

pub fn spawn(
    idx: BtcIndex,
    used: UsedSet,
    hist: HistoryIndex,
    rpc: BtcRpc,
    zmq_endpoint: String,
    state_tx: watch::Sender<IndexState>,
    events: SyncEvents,
) {
    let _ = state_tx.send(idx.state());
    tokio::spawn(run(idx, used, hist, rpc, zmq_endpoint, state_tx, events));
}

async fn run(
    idx: BtcIndex,
    used: UsedSet,
    hist: HistoryIndex,
    rpc: BtcRpc,
    zmq_endpoint: String,
    state_tx: watch::Sender<IndexState>,
    events: SyncEvents,
) {
    if matches!(idx.tip(), Ok(None)) {
        error!("btc_index is empty — run `indexd build-index` (daemon stopped) to create it; queries answer state=empty");
        let _ = state_tx.send(IndexState::Empty);
        return;
    }

    let mut quiet_until: Option<tokio::time::Instant> = None;
    loop {
        if let Err(e) = cycle(&idx, &used, &hist, &rpc, &zmq_endpoint, &state_tx, &events).await {
            let _ = state_tx.send(IndexState::CatchingUp);
            let now = tokio::time::Instant::now();
            if quiet_until.is_none_or(|t| now >= t) {
                error!("btc_index sync error (retrying every {:?}, repeats at debug for 10 min): {:?}", RETRY_DELAY, e);
                quiet_until = Some(now + QUIET_RETRIES);
            } else {
                debug!("btc_index sync error, retrying: {:?}", e);
            }
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }
}

async fn cycle(
    idx: &BtcIndex,
    used: &UsedSet,
    hist: &HistoryIndex,
    rpc: &BtcRpc,
    zmq_endpoint: &str,
    state_tx: &watch::Sender<IndexState>,
    events: &SyncEvents,
) -> Result<()> {
    let _ = state_tx.send(IndexState::CatchingUp);
    converge(idx, used, hist, rpc, events).await?;
    let (h, _) = idx.tip()?.context("tip vanished")?;
    let _ = state_tx.send(IndexState::Live);
    info!("btc_index LIVE at height {}", h);

    let mut sub = SubSocket::new();
    sub.connect(zmq_endpoint)
        .await
        .with_context(|| format!("ZMQ connect failed: {}", zmq_endpoint))?;
    sub.subscribe("hashblock")
        .await
        .context("ZMQ subscribe failed")?;

    loop {
        tokio::select! {
            msg = sub.recv() => {
                msg.context("ZMQ recv failed")?;
                debug!("hashblock notification");
            }
            _ = tokio::time::sleep(POLL_FALLBACK) => {
                debug!("btc_index poll fallback tick");
            }
        }
        converge(idx, used, hist, rpc, events).await?;
    }
}

async fn converge(
    idx: &BtcIndex,
    used: &UsedSet,
    hist: &HistoryIndex,
    rpc: &BtcRpc,
    events: &SyncEvents,
) -> Result<()> {
    {
        let (our_h, _) = idx.tip()?.context("index has no tip")?;
        used::catch_up(used, rpc, our_h).await?;
        history::catch_up(hist, rpc, our_h).await?;
    }
    loop {
        let (our_h, our_hash) = idx.tip()?.context("index has no tip")?;
        let node_h = rpc
            .call("getblockcount", json!([]))
            .await?
            .as_u64()
            .context("getblockcount: not a number")? as u32;

        if node_h < our_h {
            rewind_one(idx, used, hist, &events.blocks)?;
            continue;
        }
        let node_hash_at_ours =
            hash_from_rpc(&rpc.call("getblockhash", json!([our_h])).await?)?;
        if node_hash_at_ours != our_hash {
            warn!("btc_index reorg detected at height {}, rewinding", our_h);
            rewind_one(idx, used, hist, &events.blocks)?;
            continue;
        }
        if our_h == node_h {
            return Ok(());
        }

        let next_hash = rpc.call("getblockhash", json!([our_h + 1])).await?;
        let next_hash = next_hash.as_str().context("getblockhash: not a string")?;
        let block = rpc
            .call_with_timeout("getblock", json!([next_hash, 3]), GETBLOCK_TIMEOUT)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "getblock {} failed — if pruned away, rebuild the index from a fresh dump: {:?}",
                    next_hash,
                    e
                )
            })?;
        apply_block(idx, used, hist, &block, events)?;
        let applied = our_h + 1;
        if node_h - applied >= 3 && applied % 10 == 0 {
            info!("btc_index catching up: {}/{}", applied, node_h);
        } else if node_h - applied < 3 {
            debug!("btc_index at {}/{}", applied, node_h);
        }
    }
}

struct WatchedTx {
    txid: String,
    fee_sats: u64,
    spent: Range<usize>,
    created: Range<usize>,
}

fn apply_block(idx: &BtcIndex, used: &UsedSet, hist: &HistoryIndex, block: &Value, events: &SyncEvents) -> Result<()> {
    let height = block["height"].as_u64().context("block: no height")? as u32;
    let this_hash = txid_bytes(block["hash"].as_str().context("block: no hash")?)?;
    let prev_hash = txid_bytes(
        block["previousblockhash"]
            .as_str()
            .context("block: no previousblockhash")?,
    )?;

    let (our_h, our_hash) = idx.tip()?.context("index has no tip")?;
    if height != our_h + 1 || prev_hash != our_hash {
        bail!(
            "apply_block out of order: block {} on {:?}, index at {}",
            height,
            &block["previousblockhash"],
            our_h
        );
    }

    let mut spent: Vec<(Vec<u8>, UtxoRecord)> = Vec::new();
    let mut created: Vec<(Vec<u8>, UtxoRecord)> = Vec::new();
    let mut watched_txs: Vec<WatchedTx> = Vec::new();
    let mut hist_block = HistoryBlock::new(height, this_hash, prev_hash);

    for tx in block["tx"].as_array().context("block: no tx array")? {
        let txid_hex = tx["txid"].as_str().context("tx: no txid")?;
        let txid = txid_bytes(txid_hex)?;
        let first_spent = spent.len();
        let first_created = created.len();
        let mut tx_spks: Vec<Vec<u8>> = Vec::new();
        let mut watched = !events.txids.is_empty() && events.txids.contains(txid_hex);

        for vin in tx["vin"].as_array().context("tx: no vin")? {
            if vin.get("coinbase").is_some() {
                continue;
            }
            let prevout = vin
                .get("prevout")
                .context("vin has no prevout — getblock verbosity 3 required")?;
            let spk = hex_decode(
                prevout["scriptPubKey"]["hex"]
                    .as_str()
                    .context("prevout: no script hex")?,
            )?;
            watched = watched || events.watched.contains(&spk);
            tx_spks.push(spk.clone());
            spent.push((
                spk,
                UtxoRecord {
                    txid: txid_bytes(vin["txid"].as_str().context("vin: no txid")?)?,
                    vout: vin["vout"].as_u64().context("vin: no vout")? as u32,
                    sats: sats(&prevout["value"])?,
                    height: prevout["height"].as_u64().context("prevout: no height")? as u32,
                },
            ));
        }
        for vout in tx["vout"].as_array().context("tx: no vout")? {
            let spk = hex_decode(
                vout["scriptPubKey"]["hex"]
                    .as_str()
                    .context("vout: no script hex")?,
            )?;
            if spk.first() == Some(&0x6a) || spk.len() > 10_000 {
                continue;
            }
            watched = watched || events.watched.contains(&spk);
            tx_spks.push(spk.clone());
            created.push((
                spk,
                UtxoRecord {
                    txid,
                    vout: vout["n"].as_u64().context("vout: no n")? as u32,
                    sats: sats(&vout["value"])?,
                    height,
                },
            ));
        }

        if watched {
            watched_txs.push(WatchedTx {
                txid: txid_hex.to_string(),
                fee_sats: match tx.get("fee") {
                    Some(v) => sats(v)?,
                    None => 0,
                },
                spent: first_spent..spent.len(),
                created: first_created..created.len(),
            });
        }
        hist_block.push_tx(txid, tx_spks);
    }

    let event = block_event(height, block["hash"].as_str().unwrap_or_default(), &spent, &created);
    let blocktime = block["time"].as_u64().unwrap_or(0);
    let tx_frames: Vec<String> = watched_txs
        .iter()
        .map(|w| confirmed_tx_frame(w, height, blocktime, &spent, &created))
        .collect();

    let mut used_scripts: Vec<Vec<u8>> =
        created.iter().map(|(spk, _)| spk).filter(|s| used::is_address_script(s)).cloned().collect();
    used_scripts.sort_unstable();
    used_scripts.dedup();

    let write = idx.db.begin_write()?;
    let mut spends_not_indexed = 0u32;
    {
        let mut utxos = write.open_multimap_table(UTXOS)?;
        for (spk, rec) in &spent {
            if !utxos.remove(spk.as_slice(), rec.encode().as_slice())? {
                spends_not_indexed += 1;
            }
        }
        for (spk, rec) in &created {
            utxos.insert(spk.as_slice(), rec.encode().as_slice())?;
        }
        let mut undo = write.open_table(UNDO)?;
        let row = UndoRow {
            this_hash,
            prev_hash,
            spent,
            created,
        };
        undo.insert(height, row.encode().as_slice())?;
        if let Some(old) = height.checked_sub(UNDO_DEPTH + 1) {
            undo.remove(old)?;
        }
        let mut meta = write.open_table(META)?;
        meta.insert(META_TIP, tip_bytes(height, &this_hash).as_slice())?;
    }
    write.commit()?;
    if used.cursor()?.is_some() {
        used.apply(&[AppliedBlock { height, hash: this_hash, prev: prev_hash, scripts: used_scripts }])?;
    }
    hist.apply(&hist_block)?;
    let _ = events.blocks.send(event);
    let confirmed = tx_frames.len();
    for frame in tx_frames {
        let _ = events.txs.send(frame);
    }
    if confirmed > 0 {
        debug!("block {}: {} watched transaction(s) confirmed", height, confirmed);
    }

    if spends_not_indexed > 0 {
        debug!(
            "block {}: {} spends not in index (uncompressed-P2PK era)",
            height, spends_not_indexed
        );
    }
    Ok(())
}

fn confirmed_tx_frame(
    tx: &WatchedTx,
    height: u32,
    blocktime: u64,
    spent: &[(Vec<u8>, UtxoRecord)],
    created: &[(Vec<u8>, UtxoRecord)],
) -> String {
    fn address(spk: &[u8]) -> Option<String> {
        dannesk_btc_codec::address::Address::from_script(spk).map(|a| a.to_string())
    }
    let inputs: Vec<Value> = spent[tx.spent.clone()]
        .iter()
        .map(|(spk, rec)| json!({ "address": address(spk), "sats": rec.sats }))
        .collect();
    let outputs: Vec<Value> = created[tx.created.clone()]
        .iter()
        .map(|(spk, rec)| json!({ "address": address(spk), "sats": rec.sats, "vout": rec.vout }))
        .collect();
    json!({
        "type": "confirmed_tx",
        "txid": tx.txid,
        "height": height,
        "blocktime": blocktime,
        "fees": tx.fee_sats.to_string(),
        "inputs": inputs,
        "outputs": outputs
    })
    .to_string()
}

fn block_event(
    height: u32,
    hash_display: &str,
    spent: &[(Vec<u8>, UtxoRecord)],
    created: &[(Vec<u8>, UtxoRecord)],
) -> String {
    fn rec_json(rec: &UtxoRecord) -> Value {
        json!({
            "txid": rec.txid.iter().rev().map(|b| format!("{:02x}", b)).collect::<String>(),
            "vout": rec.vout,
            "sats": rec.sats,
            "height": rec.height
        })
    }
    let mut by_spk: BTreeMap<Vec<u8>, (Vec<Value>, Vec<Value>)> = BTreeMap::new();
    for (spk, rec) in created {
        by_spk.entry(spk.clone()).or_default().0.push(rec_json(rec));
    }
    for (spk, rec) in spent {
        by_spk.entry(spk.clone()).or_default().1.push(rec_json(rec));
    }
    let changes: Vec<Value> = by_spk
        .into_iter()
        .map(|(spk, (created, spent))| {
            let address = dannesk_btc_codec::address::Address::from_script(&spk).map(|a| a.to_string());
            json!({ "address": address, "created": created, "spent": spent })
        })
        .collect();
    json!({ "type": "block", "height": height, "hash": hash_display, "changes": changes })
        .to_string()
}

fn rewind_one(idx: &BtcIndex, used: &UsedSet, hist: &HistoryIndex, events: &broadcast::Sender<String>) -> Result<()> {
    let (h, our_hash) = idx.tip()?.context("index has no tip")?;
    let prev_for_used: [u8; 32];
    let write = idx.db.begin_write()?;
    {
        let mut undo = write.open_table(UNDO)?;
        let row = undo
            .remove(h)?
            .with_context(|| {
                format!(
                    "no undo row for height {} — reorg deeper than {} blocks, rebuild the index from a fresh dump",
                    h, UNDO_DEPTH
                )
            })?;
        let row = UndoRow::decode(row.value()).context("corrupt undo row")?;
        if row.this_hash != our_hash {
            bail!("undo row hash mismatch at height {} — index corrupt, rebuild required", h);
        }
        let mut utxos = write.open_multimap_table(UTXOS)?;
        for (spk, rec) in &row.spent {
            utxos.insert(spk.as_slice(), rec.encode().as_slice())?;
        }
        for (spk, rec) in &row.created {
            utxos.remove(spk.as_slice(), rec.encode().as_slice())?;
        }
        let mut meta = write.open_table(META)?;
        meta.insert(META_TIP, tip_bytes(h - 1, &row.prev_hash).as_slice())?;
        prev_for_used = row.prev_hash;
    }
    write.commit()?;
    if let Some((uh, _)) = used.cursor()? {
        if uh >= h {
            used.set_cursor(h - 1, &prev_for_used)?;
        }
    }
    if let Some((hh, _)) = hist.cursor()?
        && hh >= h
    {
        hist.rewind_to(h - 1, &prev_for_used)?;
    }
    let _ = events.send(json!({ "type": "reorg", "height": h }).to_string());
    info!("btc_index rewound block {}", h);
    Ok(())
}

fn tip_bytes(height: u32, hash: &[u8; 32]) -> Vec<u8> {
    let mut b = Vec::with_capacity(36);
    b.extend_from_slice(&height.to_le_bytes());
    b.extend_from_slice(hash);
    b
}

pub(super) fn txid_bytes(display_hex: &str) -> Result<[u8; 32]> {
    let mut b = hex_decode(display_hex)?;
    if b.len() != 32 {
        bail!("hash is {} bytes, expected 32", b.len());
    }
    b.reverse();
    Ok(b.try_into().unwrap())
}

fn hash_from_rpc(v: &Value) -> Result<[u8; 32]> {
    txid_bytes(v.as_str().context("hash: not a string")?)
}

pub(super) fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        bail!("odd-length hex");
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).context("bad hex"))
        .collect()
}

fn sats(v: &Value) -> Result<u64> {
    let btc = v.as_f64().context("amount: not a number")?;
    Ok((btc * 100_000_000.0).round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btc_index::AddressBalance;

    fn temp_used() -> UsedSet {
        static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "btc-sync-used-{}-{}.redb",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        UsedSet::open(path.to_str().unwrap()).unwrap()
    }

    fn temp_hist() -> HistoryIndex {
        static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "btc-sync-hist-{}-{}.redb",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        HistoryIndex::open(path.to_str().unwrap()).unwrap()
    }

    fn temp_index() -> (BtcIndex, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "btc-sync-test-{}-{:?}.redb",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        (BtcIndex::open(path.to_str().unwrap()).unwrap(), path)
    }

    fn hex_rev(b: &[u8; 32]) -> String {
        b.iter().rev().map(|x| format!("{:02x}", x)).collect()
    }

    const ADDR_A: &str = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4";
    const ADDR_B: &str = "12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX";

    fn ev() -> broadcast::Sender<String> {
        broadcast::channel(8).0
    }

    fn sinks(watched: &[&[u8]]) -> (SyncEvents, broadcast::Receiver<String>) {
        let (txs, txs_rx) = broadcast::channel(16);
        let set = DashSet::new();
        for spk in watched {
            set.insert(spk.to_vec());
        }
        (
            SyncEvents { blocks: ev(), txs, watched: Arc::new(set), txids: Arc::new(DashSet::new()) },
            txs_rx,
        )
    }

    fn seed(idx: &BtcIndex, spk_a: &[u8]) {
        let write = idx.db.begin_write().unwrap();
        {
            let mut utxos = write.open_multimap_table(UTXOS).unwrap();
            let rec = UtxoRecord {
                txid: [0xAA; 32],
                vout: 0,
                sats: 50_000,
                height: 90,
            };
            utxos.insert(spk_a, rec.encode().as_slice()).unwrap();
            let mut meta = write.open_table(META).unwrap();
            meta.insert(META_TIP, tip_bytes(100, &[1u8; 32]).as_slice())
                .unwrap();
        }
        write.commit().unwrap();
    }

    fn block_101(spk_a: &[u8], spk_b: &[u8]) -> Value {
        serde_json::json!({
            "height": 101,
            "hash": hex_rev(&[2u8; 32]),
            "previousblockhash": hex_rev(&[1u8; 32]),
            "time": 1_786_761_453u64,
            "tx": [
                {
                    "txid": hex_rev(&[0xCB; 32]),
                    "vin": [{ "coinbase": "aa" }],
                    "vout": [
                        { "n": 0, "value": 3.125, "scriptPubKey": { "hex": hex::encode_dummy(spk_b) } }
                    ]
                },
                {
                    "txid": hex_rev(&[0xBB; 32]),
                    "fee": 0.00001,
                    "vin": [{
                        "txid": hex_rev(&[0xAA; 32]),
                        "vout": 0,
                        "prevout": {
                            "height": 90,
                            "value": 0.0005,
                            "scriptPubKey": { "hex": hex::encode_dummy(spk_a) }
                        }
                    }],
                    "vout": [
                        { "n": 0, "value": 0.0003, "scriptPubKey": { "hex": hex::encode_dummy(spk_b) } },
                        { "n": 1, "value": 0.00019, "scriptPubKey": { "hex": hex::encode_dummy(spk_a) } },
                        { "n": 2, "value": 0.0, "scriptPubKey": { "hex": "6a04deadbeef" } }
                    ]
                }
            ]
        })
    }

    mod hex {
        pub fn encode_dummy(b: &[u8]) -> String {
            b.iter().map(|x| format!("{:02x}", x)).collect()
        }
    }

    #[test]
    fn hex_and_txid_roundtrip() {
        let h = [7u8; 32];
        assert_eq!(txid_bytes(&hex_rev(&h)).unwrap(), h);
        assert!(txid_bytes("zz").is_err());
        assert_eq!(sats(&serde_json::json!(1.35266916)).unwrap(), 135_266_916);
        assert_eq!(sats(&serde_json::json!(0.00000001)).unwrap(), 1);
    }

    #[test]
    fn apply_then_rewind_restores_exactly() {
        let (idx, path) = temp_index();
        let spk_a = BtcIndex::script_pubkey_for(ADDR_A).unwrap();
        let spk_b = BtcIndex::script_pubkey_for(ADDR_B).unwrap();
        seed(&idx, &spk_a);

        apply_block(&idx, &temp_used(), &temp_hist(), &block_101(&spk_a, &spk_b), &sinks(&[]).0).unwrap();

        assert_eq!(idx.tip().unwrap(), Some((101, [2u8; 32])));
        assert_eq!(
            idx.query_spk(&spk_a).unwrap(),
            AddressBalance { sats: 19_000, utxo_count: 1 }
        );
        assert_eq!(
            idx.query_spk(&spk_b).unwrap(),
            AddressBalance { sats: 312_530_000, utxo_count: 2 }
        );

        assert!(apply_block(&idx, &temp_used(), &temp_hist(), &block_101(&spk_a, &spk_b), &sinks(&[]).0).is_err());

        rewind_one(&idx, &temp_used(), &temp_hist(), &ev()).unwrap();
        assert_eq!(idx.tip().unwrap(), Some((100, [1u8; 32])));
        assert_eq!(
            idx.query_spk(&spk_a).unwrap(),
            AddressBalance { sats: 50_000, utxo_count: 1 }
        );
        assert_eq!(
            idx.query_spk(&spk_b).unwrap(),
            AddressBalance { sats: 0, utxo_count: 0 }
        );

        apply_block(&idx, &temp_used(), &temp_hist(), &block_101(&spk_a, &spk_b), &sinks(&[]).0).unwrap();
        assert_eq!(idx.tip().unwrap(), Some((101, [2u8; 32])));

        rewind_one(&idx, &temp_used(), &temp_hist(), &ev()).unwrap();
        assert!(rewind_one(&idx, &temp_used(), &temp_hist(), &ev()).is_err());

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_watched_transaction_confirms_with_its_counterparties_and_fee() {
        let (idx, path) = temp_index();
        let spk_a = BtcIndex::script_pubkey_for(ADDR_A).unwrap();
        let spk_b = BtcIndex::script_pubkey_for(ADDR_B).unwrap();
        seed(&idx, &spk_a);

        let (events, mut rx) = sinks(&[&spk_b]);
        apply_block(&idx, &temp_used(), &temp_hist(), &block_101(&spk_a, &spk_b), &events).unwrap();

        let frames: Vec<Value> = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|f| serde_json::from_str(&f).unwrap())
            .collect();
        assert_eq!(frames.len(), 2, "the coinbase pays B too");

        let paid = frames
            .iter()
            .find(|f| f["txid"] == hex_rev(&[0xBB; 32]))
            .expect("the payment must be framed");
        assert_eq!(paid["type"], "confirmed_tx");
        assert_eq!(paid["height"], 101);
        assert_eq!(paid["blocktime"], 1_786_761_453u64);
        assert_eq!(paid["fees"], "1000");
        assert_eq!(paid["inputs"][0]["address"], ADDR_A);
        assert_eq!(paid["inputs"][0]["sats"], 50_000);
        assert_eq!(paid["outputs"][0]["address"], ADDR_B);
        assert_eq!(paid["outputs"][0]["sats"], 30_000);
        assert_eq!(paid["outputs"][0]["vout"], 0);
        assert_eq!(paid["outputs"].as_array().unwrap().len(), 2);

        let coinbase = frames
            .iter()
            .find(|f| f["txid"] == hex_rev(&[0xCB; 32]))
            .expect("the coinbase pays B and must be framed");
        assert!(coinbase["inputs"].as_array().unwrap().is_empty());
        assert_eq!(coinbase["fees"], "0", "a coinbase pays no fee");

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn an_unwatched_block_emits_no_transaction_frames() {
        let (idx, path) = temp_index();
        let spk_a = BtcIndex::script_pubkey_for(ADDR_A).unwrap();
        let spk_b = BtcIndex::script_pubkey_for(ADDR_B).unwrap();
        seed(&idx, &spk_a);

        let (events, mut rx) = sinks(&[]);
        apply_block(&idx, &temp_used(), &temp_hist(), &block_101(&spk_a, &spk_b), &events).unwrap();
        assert!(rx.try_recv().is_err(), "nothing is watched, nothing is framed");

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn spending_is_matched_from_prevout_not_output() {
        let (idx, path) = temp_index();
        let spk_a = BtcIndex::script_pubkey_for(ADDR_A).unwrap();
        let spk_b = BtcIndex::script_pubkey_for(ADDR_B).unwrap();
        seed(&idx, &spk_a);

        let (events, mut rx) = sinks(&[&spk_a]);
        apply_block(&idx, &temp_used(), &temp_hist(), &block_101(&spk_a, &spk_b), &events).unwrap();

        let frames: Vec<Value> = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|f| serde_json::from_str(&f).unwrap())
            .collect();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["txid"], hex_rev(&[0xBB; 32]));
        assert_eq!(frames[0]["inputs"][0]["address"], ADDR_A);
        assert_eq!(frames[0]["outputs"][1]["address"], ADDR_A);
        assert_eq!(frames[0]["outputs"][1]["sats"], 19_000);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_block_lands_in_the_history_index_and_a_rewind_takes_it_out() {
        let (idx, path) = temp_index();
        let spk_a = BtcIndex::script_pubkey_for(ADDR_A).unwrap();
        let spk_b = BtcIndex::script_pubkey_for(ADDR_B).unwrap();
        seed(&idx, &spk_a);
        let hist = temp_hist();

        apply_block(&idx, &temp_used(), &hist, &block_101(&spk_a, &spk_b), &sinks(&[]).0).unwrap();
        assert_eq!(hist.coverage().unwrap(), Some((101, 101)), "a fresh store starts at this block");
        let by_a = hist.history(&[spk_a.clone()], 0, 10).unwrap().unwrap();
        assert_eq!(by_a.transactions, vec![(101, [0xBB; 32])], "A paid and took change: once");
        let by_b = hist.history(&[spk_b.clone()], 0, 10).unwrap().unwrap();
        assert_eq!(
            by_b.transactions.iter().map(|(_, t)| t[0]).collect::<Vec<_>>(),
            vec![0xBB, 0xCB],
            "B was paid by the coinbase and by A (same height: txid order)"
        );

        rewind_one(&idx, &temp_used(), &hist, &ev()).unwrap();
        assert_eq!(hist.cursor().unwrap(), Some((100, [1u8; 32])));
        assert!(hist.history(&[spk_b.clone()], 0, 10).unwrap().is_none(), "nothing covered: honest, not empty");
        let mut other = block_101(&spk_a, &spk_b);
        other["hash"] = json!(hex_rev(&[3u8; 32]));
        other["tx"] = json!([{
            "txid": hex_rev(&[0xCC; 32]),
            "vin": [{ "coinbase": "aa" }],
            "vout": [{ "n": 0, "value": 3.125, "scriptPubKey": { "hex": hex::encode_dummy(&spk_a) } }]
        }]);
        apply_block(&idx, &temp_used(), &hist, &other, &sinks(&[]).0).unwrap();
        assert_eq!(hist.coverage().unwrap(), Some((101, 101)));
        assert!(hist.history(&[spk_b], 0, 10).unwrap().unwrap().transactions.is_empty(), "the rewound block's entries are gone");
        assert_eq!(hist.history(&[spk_a], 0, 10).unwrap().unwrap().transactions, vec![(101, [0xCC; 32])]);
        let _ = std::fs::remove_file(path);
    }
}

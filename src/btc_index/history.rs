
use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, TryStreamExt};
use redb::{Database, MultimapTableDefinition, MultimapTableHandle, ReadableDatabase, ReadableTable, TableDefinition};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use super::IndexState;
use super::store::UNDO_DEPTH;
use super::sync::{hex_decode, txid_bytes};
use super::used::is_address_script;
use crate::rpc::BtcRpc;

pub const EPOCH: u32 = 144;

pub const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
pub const META_CURSOR: &str = "cursor";
pub const META_LOW: &str = "low";
pub const UNDO: TableDefinition<u32, &[u8]> = TableDefinition::new("undo");

const CACHE_BYTES: usize = 512 << 20;

const BATCH: u32 = 100;
const FETCH_CONCURRENCY: usize = 3;
const PRUNE_POLL: Duration = Duration::from_secs(10 * 60);
const REPORT_EVERY: u32 = 1_000;
const RETRY_DELAY: Duration = Duration::from_secs(5);
const QUIET_RETRIES: Duration = Duration::from_secs(600);
const GETBLOCK_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entry {
    pub spk: Vec<u8>,
    pub txid: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryBlock {
    pub height: u32,
    pub hash: [u8; 32],
    pub prev: [u8; 32],
    pub entries: Vec<Entry>,
}

impl HistoryBlock {
    pub fn new(height: u32, hash: [u8; 32], prev: [u8; 32]) -> Self {
        Self { height, hash, prev, entries: Vec::new() }
    }

    pub fn push_tx(&mut self, txid: [u8; 32], mut spks: Vec<Vec<u8>>) {
        spks.retain(|s| is_address_script(s));
        spks.sort_unstable();
        spks.dedup();
        self.entries.extend(spks.into_iter().map(|spk| Entry { spk, txid }));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Low {
    pub height: u32,
    pub hash: [u8; 32],
    pub prev: [u8; 32],
}

#[derive(Debug, PartialEq, Eq)]
pub struct History {
    pub from: u32,
    pub to: u32,
    pub transactions: Vec<(u32, [u8; 32])>,
    pub truncated: bool,
}

fn value_bytes(height: u32, txid: &[u8; 32]) -> [u8; 36] {
    let mut b = [0u8; 36];
    b[..4].copy_from_slice(&height.to_be_bytes());
    b[4..].copy_from_slice(txid);
    b
}

fn value_parse(b: &[u8]) -> Option<(u32, [u8; 32])> {
    if b.len() != 36 {
        return None;
    }
    Some((u32::from_be_bytes(b[..4].try_into().ok()?), b[4..].try_into().ok()?))
}

fn cursor_bytes(height: u32, hash: &[u8; 32]) -> Vec<u8> {
    let mut b = Vec::with_capacity(36);
    b.extend_from_slice(&height.to_le_bytes());
    b.extend_from_slice(hash);
    b
}

fn cursor_parse(b: &[u8]) -> Option<(u32, [u8; 32])> {
    if b.len() != 36 {
        return None;
    }
    Some((u32::from_le_bytes(b[..4].try_into().ok()?), b[4..36].try_into().ok()?))
}

fn low_bytes(low: &Low) -> Vec<u8> {
    let mut b = Vec::with_capacity(68);
    b.extend_from_slice(&low.height.to_le_bytes());
    b.extend_from_slice(&low.hash);
    b.extend_from_slice(&low.prev);
    b
}

fn low_parse(b: &[u8]) -> Option<Low> {
    if b.len() != 68 {
        return None;
    }
    Some(Low {
        height: u32::from_le_bytes(b[..4].try_into().ok()?),
        hash: b[4..36].try_into().ok()?,
        prev: b[36..68].try_into().ok()?,
    })
}

fn undo_encode(block: &HistoryBlock) -> Vec<u8> {
    let mut out = Vec::with_capacity(68 + block.entries.len() * 64);
    out.extend_from_slice(&block.hash);
    out.extend_from_slice(&block.prev);
    out.extend_from_slice(&(block.entries.len() as u32).to_le_bytes());
    for e in &block.entries {
        out.push(e.spk.len() as u8);
        out.extend_from_slice(&e.spk);
        out.extend_from_slice(&e.txid);
    }
    out
}

fn undo_decode(height: u32, b: &[u8]) -> Option<HistoryBlock> {
    fn take<'a>(b: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
        if b.len() < n {
            return None;
        }
        let (head, tail) = b.split_at(n);
        *b = tail;
        Some(head)
    }
    let mut b = b;
    let hash: [u8; 32] = take(&mut b, 32)?.try_into().ok()?;
    let prev: [u8; 32] = take(&mut b, 32)?.try_into().ok()?;
    let n = u32::from_le_bytes(take(&mut b, 4)?.try_into().ok()?) as usize;
    let mut entries = Vec::with_capacity(n.min(1 << 20));
    for _ in 0..n {
        let len = take(&mut b, 1)?[0] as usize;
        let spk = take(&mut b, len)?.to_vec();
        let txid: [u8; 32] = take(&mut b, 32)?.try_into().ok()?;
        entries.push(Entry { spk, txid });
    }
    if !b.is_empty() {
        return None;
    }
    Some(HistoryBlock { height, hash, prev, entries })
}

fn day_name(height: u32) -> String {
    format!("hist_{}", height / EPOCH)
}

fn day_def(name: &str) -> MultimapTableDefinition<'_, &'static [u8], &'static [u8]> {
    MultimapTableDefinition::new(name)
}

fn epoch_of(name: &str) -> Option<u32> {
    name.strip_prefix("hist_")?.parse().ok()
}

#[derive(Clone)]
pub struct HistoryIndex {
    db: Arc<Database>,
}

impl HistoryIndex {
    pub fn open(path: &str) -> Result<Self> {
        let db = Database::builder()
            .set_cache_size(CACHE_BYTES)
            .create(path)
            .with_context(|| format!("Failed to open history index at {path}"))?;
        Ok(Self { db: Arc::new(db) })
    }

    fn meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(META) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(table.get(key)?.map(|g| g.value().to_vec()))
    }

    pub fn cursor(&self) -> Result<Option<(u32, [u8; 32])>> {
        Ok(self.meta(META_CURSOR)?.as_deref().and_then(cursor_parse))
    }

    pub fn low(&self) -> Result<Option<Low>> {
        Ok(self.meta(META_LOW)?.as_deref().and_then(low_parse))
    }

    pub fn coverage(&self) -> Result<Option<(u32, u32)>> {
        match (self.low()?, self.cursor()?) {
            (Some(low), Some((high, _))) if low.height <= high => Ok(Some((low.height, high))),
            _ => Ok(None),
        }
    }

    pub fn prev_hash_at(&self, height: u32) -> Result<Option<[u8; 32]>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(UNDO) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(table.get(height)?.and_then(|g| g.value().get(32..64).and_then(|p| p.try_into().ok())))
    }

    pub fn apply(&self, block: &HistoryBlock) -> Result<bool> {
        let write = self.db.begin_write()?;
        {
            let mut meta = write.open_table(META)?;
            let mut undo = write.open_table(UNDO)?;
            let cursor = meta.get(META_CURSOR)?.map(|g| g.value().to_vec()).as_deref().and_then(cursor_parse);
            if let Some((h, hash)) = cursor {
                if block.height <= h {
                    let held = undo
                        .get(block.height)?
                        .is_some_and(|g| g.value().get(..32) == Some(&block.hash[..]));
                    if held {
                        return Ok(false);
                    }
                    bail!(
                        "history index already past block {} on a different chain (cursor {}) — rewind first",
                        block.height,
                        h
                    );
                }
                if block.height != h + 1 || block.prev != hash {
                    bail!(
                        "history index out of order: block {} on prev {:02x?}.., cursor at {}",
                        block.height,
                        &block.prev[..4],
                        h
                    );
                }
            }
            let name = day_name(block.height);
            let mut day = write.open_multimap_table(day_def(&name))?;
            let mut sorted: Vec<&Entry> = block.entries.iter().collect();
            sorted.sort_unstable();
            for e in sorted {
                day.insert(e.spk.as_slice(), value_bytes(block.height, &e.txid).as_slice())?;
            }
            undo.insert(block.height, undo_encode(block).as_slice())?;
            if let Some(old) = block.height.checked_sub(UNDO_DEPTH + 1) {
                undo.remove(old)?;
            }
            meta.insert(META_CURSOR, cursor_bytes(block.height, &block.hash).as_slice())?;
            let low = meta.get(META_LOW)?.map(|g| g.value().to_vec()).as_deref().and_then(low_parse);
            if low.is_none_or(|l| block.height <= l.height) {
                let low = Low { height: block.height, hash: block.hash, prev: block.prev };
                meta.insert(META_LOW, low_bytes(&low).as_slice())?;
            }
        }
        write.commit()?;
        Ok(true)
    }

    pub fn rewind_to(&self, height: u32, hash: &[u8; 32]) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut meta = write.open_table(META)?;
            let mut undo = write.open_table(UNDO)?;
            let (h, _) = meta
                .get(META_CURSOR)?
                .map(|g| g.value().to_vec())
                .as_deref()
                .and_then(cursor_parse)
                .context("history index has no cursor to rewind")?;
            if h < height {
                bail!("history index at {} is below the rewind target {}", h, height);
            }
            if h > height {
                if h != height + 1 {
                    bail!("history index rewind from {} to {} is not one step", h, height);
                }
                let row = undo.remove(h)?.map(|g| g.value().to_vec());
                if let Some(block) = row.as_deref().and_then(|r| undo_decode(h, r)) {
                    let name = day_name(h);
                    let mut day = write.open_multimap_table(day_def(&name))?;
                    for e in &block.entries {
                        day.remove(e.spk.as_slice(), value_bytes(h, &e.txid).as_slice())?;
                    }
                }
            }
            meta.insert(META_CURSOR, cursor_bytes(height, hash).as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    pub fn backfill(&self, blocks: &[HistoryBlock]) -> Result<()> {
        let Some(top) = blocks.first() else { return Ok(()) };
        let write = self.db.begin_write()?;
        {
            let mut meta = write.open_table(META)?;
            let low = meta
                .get(META_LOW)?
                .map(|g| g.value().to_vec())
                .as_deref()
                .and_then(low_parse)
                .context("history index has no coverage to extend")?;
            if top.height + 1 != low.height || top.hash != low.prev {
                bail!(
                    "backfill block {} does not continue the coverage (low {} expects prev {:02x?}.., got {:02x?}..)",
                    top.height,
                    low.height,
                    &low.prev[..4],
                    &top.hash[..4]
                );
            }
            for pair in blocks.windows(2) {
                if pair[1].height + 1 != pair[0].height || pair[1].hash != pair[0].prev {
                    bail!("backfill blocks are not a chain at {}", pair[0].height);
                }
            }
            let mut by_day: BTreeMap<u32, Vec<(&Entry, u32)>> = BTreeMap::new();
            for b in blocks {
                by_day.entry(b.height / EPOCH).or_default().extend(b.entries.iter().map(|e| (e, b.height)));
            }
            for (epoch, mut list) in by_day {
                list.sort_unstable_by(|a, b| a.0.spk.cmp(&b.0.spk).then(a.1.cmp(&b.1)).then(a.0.txid.cmp(&b.0.txid)));
                let name = format!("hist_{epoch}");
                let mut day = write.open_multimap_table(day_def(&name))?;
                for (e, h) in list {
                    day.insert(e.spk.as_slice(), value_bytes(h, &e.txid).as_slice())?;
                }
            }
            let bottom = blocks.last().expect("non-empty");
            let low = Low { height: bottom.height, hash: bottom.hash, prev: bottom.prev };
            meta.insert(META_LOW, low_bytes(&low).as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    pub fn prune(&self, prune_height: u32) -> Result<usize> {
        let gone: Vec<u32> = {
            let read = self.db.begin_read()?;
            read.list_multimap_tables()?
                .filter_map(|t| epoch_of(t.name()))
                .filter(|e| e * EPOCH + EPOCH - 1 < prune_height)
                .collect()
        };
        if gone.is_empty() {
            return Ok(0);
        }
        let write = self.db.begin_write()?;
        for e in &gone {
            let name = format!("hist_{e}");
            write.delete_multimap_table(day_def(&name))?;
        }
        {
            let mut meta = write.open_table(META)?;
            let low = meta.get(META_LOW)?.map(|g| g.value().to_vec()).as_deref().and_then(low_parse);
            let oldest_kept = (gone.iter().max().expect("non-empty") + 1) * EPOCH;
            if let Some(low) = low
                && low.height < oldest_kept
            {
                let low = Low { height: oldest_kept, hash: [0; 32], prev: [0; 32] };
                meta.insert(META_LOW, low_bytes(&low).as_slice())?;
            }
        }
        write.commit()?;
        Ok(gone.len())
    }

    pub fn history(&self, spks: &[Vec<u8>], floor: u32, limit: usize) -> Result<Option<History>> {
        let read = self.db.begin_read()?;
        let meta = match read.open_table(META) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let low = meta.get(META_LOW)?.map(|g| g.value().to_vec()).as_deref().and_then(low_parse);
        let cursor = meta.get(META_CURSOR)?.map(|g| g.value().to_vec()).as_deref().and_then(cursor_parse);
        let (Some(low), Some((high, _))) = (low, cursor) else { return Ok(None) };
        if low.height > high {
            return Ok(None);
        }
        let from = low.height.max(floor);
        let mut found: HashMap<[u8; 32], u32> = HashMap::new();
        if from <= high {
            for epoch in (from / EPOCH..=high / EPOCH).rev() {
                let name = format!("hist_{epoch}");
                let table = match read.open_multimap_table(day_def(&name)) {
                    Ok(t) => t,
                    Err(redb::TableError::TableDoesNotExist(_)) => continue,
                    Err(e) => return Err(e.into()),
                };
                for spk in spks {
                    for v in table.get(spk.as_slice())? {
                        let v = v?;
                        if let Some((h, txid)) = value_parse(v.value())
                            && h >= from
                        {
                            found.entry(txid).and_modify(|e| *e = (*e).max(h)).or_insert(h);
                        }
                    }
                }
            }
        }
        let mut transactions: Vec<(u32, [u8; 32])> = found.into_iter().map(|(t, h)| (h, t)).collect();
        transactions.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        let truncated = transactions.len() > limit;
        transactions.truncate(limit);
        Ok(Some(History { from, to: high, transactions, truncated }))
    }

    #[cfg(test)]
    pub(crate) fn seed_for_test(&self, low: u32, high: u32, entries: &[(Vec<u8>, u32, [u8; 32])]) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut meta = write.open_table(META)?;
            meta.insert(META_CURSOR, cursor_bytes(high, &[0xee; 32]).as_slice())?;
            let l = Low { height: low, hash: [0xdd; 32], prev: [0xcc; 32] };
            meta.insert(META_LOW, low_bytes(&l).as_slice())?;
            write.open_table(UNDO)?;
            for (spk, height, txid) in entries {
                let name = day_name(*height);
                let mut day = write.open_multimap_table(day_def(&name))?;
                day.insert(spk.as_slice(), value_bytes(*height, txid).as_slice())?;
            }
        }
        write.commit()?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct BlockLite {
    hash: String,
    height: u32,
    #[serde(default)]
    previousblockhash: Option<String>,
    tx: Vec<TxLite>,
}

#[derive(Deserialize)]
struct TxLite {
    txid: String,
    vin: Vec<VinLite>,
    vout: Vec<VoutLite>,
}

#[derive(Deserialize)]
struct VinLite {
    #[serde(default)]
    prevout: Option<PrevoutLite>,
}

#[derive(Deserialize)]
struct PrevoutLite {
    #[serde(rename = "scriptPubKey")]
    script_pub_key: SpkLite,
}

#[derive(Deserialize)]
struct VoutLite {
    #[serde(rename = "scriptPubKey")]
    script_pub_key: SpkLite,
}

#[derive(Deserialize)]
struct SpkLite {
    hex: String,
}

impl BlockLite {
    fn into_block(self) -> Result<HistoryBlock> {
        let prev = match &self.previousblockhash {
            Some(p) => txid_bytes(p)?,
            None => [0; 32],
        };
        let mut block = HistoryBlock::new(self.height, txid_bytes(&self.hash)?, prev);
        for tx in self.tx {
            let mut spks = Vec::with_capacity(tx.vin.len() + tx.vout.len());
            for vin in tx.vin {
                if let Some(prevout) = vin.prevout {
                    spks.push(hex_decode(&prevout.script_pub_key.hex)?);
                }
            }
            for vout in tx.vout {
                spks.push(hex_decode(&vout.script_pub_key.hex)?);
            }
            block.push_tx(txid_bytes(&tx.txid)?, spks);
        }
        Ok(block)
    }
}

async fn fetch_block(rpc: &BtcRpc, height: u32) -> Result<HistoryBlock> {
    let hash = rpc.call("getblockhash", json!([height])).await?;
    let hash = hash.as_str().context("getblockhash: not a string")?.to_string();
    let lite: BlockLite = rpc.call_typed("getblock", json!([hash, 3]), GETBLOCK_TIMEOUT).await?;
    lite.into_block()
}

async fn hash_below(hist: &HistoryIndex, rpc: &BtcRpc, height: u32) -> Result<[u8; 32]> {
    if let Some(prev) = hist.prev_hash_at(height)? {
        return Ok(prev);
    }
    let below = height.checked_sub(1).context("history index cannot step below genesis")?;
    let hash = rpc.call("getblockhash", json!([below])).await?;
    txid_bytes(hash.as_str().context("getblockhash: not a string")?)
}

pub async fn catch_up(hist: &HistoryIndex, rpc: &BtcRpc, target: u32) -> Result<()> {
    let mut applied = 0u32;
    loop {
        let Some((h, hash)) = hist.cursor()? else {
            let block = fetch_block(rpc, target).await?;
            hist.apply(&block)?;
            info!(
                "history index started at height {} — the backfill extends it down to the node's prune height in the background",
                target
            );
            return Ok(());
        };
        if h > target {
            let prev = hash_below(hist, rpc, h).await?;
            hist.rewind_to(h - 1, &prev)?;
            continue;
        }
        let node = txid_bytes(
            rpc.call("getblockhash", json!([h]))
                .await?
                .as_str()
                .context("getblockhash: not a string")?,
        )?;
        if node != hash {
            warn!("history index block {} is off the node's chain — stepping back", h);
            let prev = hash_below(hist, rpc, h).await?;
            hist.rewind_to(h - 1, &prev)?;
            continue;
        }
        if h == target {
            break;
        }
        let block = fetch_block(rpc, h + 1).await?;
        if block.prev != hash {
            continue;
        }
        hist.apply(&block)?;
        applied += 1;
    }
    if applied > 0 {
        info!("history index caught up {} blocks to {}", applied, target);
    }
    Ok(())
}

pub fn spawn(hist: HistoryIndex, rpc: BtcRpc, state_rx: watch::Receiver<IndexState>, prune_tx: watch::Sender<u32>) {
    tokio::spawn(run(hist, rpc, state_rx, prune_tx));
}

enum Step {
    Built { blocks: u32, low: u32, prune_height: u32 },
    Done { low: u32, high: u32 },
    Waiting,
}

async fn run(hist: HistoryIndex, rpc: BtcRpc, mut state_rx: watch::Receiver<IndexState>, prune_tx: watch::Sender<u32>) {
    let mut quiet_until: Option<Instant> = None;
    let mut since_report = 0u32;
    let mut building = false;
    loop {
        if *state_rx.borrow() != IndexState::Live {
            if state_rx.changed().await.is_err() {
                return;
            }
            continue;
        }
        match step(&hist, &rpc, &prune_tx).await {
            Ok(Step::Built { blocks, low, prune_height }) => {
                quiet_until = None;
                building = true;
                since_report += blocks;
                debug!("history index: backfilled {} blocks, bottom at {}", blocks, low);
                if since_report >= REPORT_EVERY {
                    since_report = 0;
                    info!(
                        "history index: bottom at {}, {} blocks to the prune height {}",
                        low,
                        low.saturating_sub(prune_height),
                        prune_height
                    );
                }
            }
            Ok(Step::Done { low, high }) => {
                quiet_until = None;
                if building {
                    building = false;
                    info!(
                        "history index complete: heights {}..{} ({} blocks, about {} days)",
                        low,
                        high,
                        high - low + 1,
                        (high - low + 1) / EPOCH
                    );
                }
                tokio::time::sleep(PRUNE_POLL).await;
            }
            Ok(Step::Waiting) => tokio::time::sleep(RETRY_DELAY).await,
            Err(e) => {
                let now = Instant::now();
                if quiet_until.is_none_or(|t| now >= t) {
                    error!("history index backfill error (retrying every {:?}, repeats at debug for 10 min): {:?}", RETRY_DELAY, e);
                    quiet_until = Some(now + QUIET_RETRIES);
                } else {
                    debug!("history index backfill error, retrying: {:?}", e);
                }
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}

pub(super) async fn prune_height(rpc: &BtcRpc) -> Result<u32> {
    let v: Value = rpc.call("getblockchaininfo", json!([])).await?;
    Ok(v["pruneheight"].as_u64().unwrap_or(0) as u32)
}

async fn step(hist: &HistoryIndex, rpc: &BtcRpc, prune_tx: &watch::Sender<u32>) -> Result<Step> {
    let prune = prune_height(rpc).await?;
    prune_tx.send_replace(prune);
    let Some(low) = hist.low()? else { return Ok(Step::Waiting) };
    if low.height <= prune {
        let h = hist.clone();
        let gone = tokio::task::spawn_blocking(move || h.prune(prune)).await??;
        if gone > 0 {
            debug!("history index: dropped {} day table(s) below the prune height {}", gone, prune);
        }
        let (low, high) = hist.coverage()?.context("history index coverage vanished")?;
        return Ok(Step::Done { low, high });
    }
    let top = low.height - 1;
    let bottom = low.height.saturating_sub(BATCH).max(prune);
    let heights: Vec<u32> = (bottom..=top).rev().collect();
    let blocks: Vec<HistoryBlock> = futures_util::stream::iter(heights)
        .map(|h| fetch_block(rpc, h))
        .buffered(FETCH_CONCURRENCY)
        .try_collect()
        .await?;
    let h = hist.clone();
    tokio::task::spawn_blocking(move || h.backfill(&blocks)).await??;
    Ok(Step::Built { blocks: top - bottom + 1, low: bottom, prune_height: prune })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp() -> (HistoryIndex, std::path::PathBuf) {
        static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "history-test-{}-{}.redb",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        (HistoryIndex::open(path.to_str().unwrap()).unwrap(), path)
    }

    fn spk(tag: u8) -> Vec<u8> {
        let mut s = vec![0x00, 0x14];
        s.extend_from_slice(&[tag; 20]);
        s
    }

    fn hash_of(height: u32) -> [u8; 32] {
        [height as u8; 32]
    }

    fn block(height: u32, prev: [u8; 32], txs: &[(u8, &[u8])]) -> HistoryBlock {
        let mut b = HistoryBlock::new(height, hash_of(height), prev);
        for (tx, tags) in txs {
            b.push_tx([*tx; 32], tags.iter().map(|t| spk(*t)).collect());
        }
        b
    }

    fn txids(h: &History) -> Vec<(u32, u8)> {
        h.transactions.iter().map(|(h, t)| (*h, t[0])).collect()
    }

    #[test]
    fn a_transaction_keeps_each_address_script_once() {
        let mut b = HistoryBlock::new(1, [1; 32], [0; 32]);
        let op_return = vec![0x6a, 0x04, 1, 2, 3, 4];
        b.push_tx([7; 32], vec![spk(1), spk(2), spk(1), op_return, spk(2)]);
        assert_eq!(b.entries, vec![Entry { spk: spk(1), txid: [7; 32] }, Entry { spk: spk(2), txid: [7; 32] }]);
        let decoded = undo_decode(1, &undo_encode(&b)).unwrap();
        assert_eq!(decoded, b);
        assert_eq!(value_parse(&value_bytes(970_000, &[9; 32])), Some((970_000, [9; 32])));
        assert_eq!(epoch_of("hist_6539"), Some(6539));
        assert_eq!(epoch_of("meta"), None);
        assert_eq!(day_name(941_744), "hist_6539");
    }

    #[test]
    fn apply_query_rewind() {
        let (hist, path) = temp();
        assert_eq!(hist.coverage().unwrap(), None);
        assert!(hist.history(&[spk(1)], 0, 10).unwrap().is_none(), "nothing covered: not built");
        let b100 = block(100, [99; 32], &[(1, &[1, 2]), (2, &[2])]);
        let b101 = block(101, b100.hash, &[(3, &[1])]);
        assert!(hist.apply(&b100).unwrap());
        assert!(hist.apply(&b101).unwrap());
        assert_eq!(hist.coverage().unwrap(), Some((100, 101)));
        assert_eq!(hist.low().unwrap(), Some(Low { height: 100, hash: [100; 32], prev: [99; 32] }));

        let h = hist.history(&[spk(1)], 0, 10).unwrap().unwrap();
        assert_eq!(h.from, 100);
        assert_eq!(h.to, 101);
        assert_eq!(txids(&h), vec![(101, 3), (100, 1)], "newest first");
        assert!(!h.truncated);
        let h = hist.history(&[spk(1), spk(2)], 0, 10).unwrap().unwrap();
        assert_eq!(txids(&h), vec![(101, 3), (100, 1), (100, 2)], "one entry per txid across addresses");
        let h = hist.history(&[spk(1), spk(2)], 0, 2).unwrap().unwrap();
        assert_eq!(txids(&h), vec![(101, 3), (100, 1)]);
        assert!(h.truncated);
        let h = hist.history(&[spk(1)], 101, 10).unwrap().unwrap();
        assert_eq!(h.from, 101);
        assert_eq!(txids(&h), vec![(101, 3)]);
        let h = hist.history(&[spk(1)], 105, 10).unwrap().unwrap();
        assert_eq!((h.from, h.to, h.transactions.len()), (105, 101, 0));

        assert!(!hist.apply(&b101).unwrap());
        let mut other = block(101, b100.hash, &[(4, &[1])]);
        other.hash = [0xab; 32];
        assert!(hist.apply(&other).is_err());
        assert!(hist.apply(&block(103, [0; 32], &[])).is_err(), "a gap is refused");

        hist.rewind_to(100, &b100.hash).unwrap();
        assert_eq!(hist.cursor().unwrap(), Some((100, [100; 32])));
        let h = hist.history(&[spk(1)], 0, 10).unwrap().unwrap();
        assert_eq!(txids(&h), vec![(100, 1)], "the rewound block's entries are gone");
        assert!(hist.apply(&other).unwrap(), "the replacement lands");
        assert_eq!(txids(&hist.history(&[spk(1)], 0, 10).unwrap().unwrap()), vec![(101, 4), (100, 1)]);
        assert!(hist.rewind_to(98, &[98; 32]).is_err());
        assert!(hist.rewind_to(102, &[1; 32]).is_err());
        drop(hist);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn backfill_extends_the_bottom_and_checks_the_chain() {
        let (hist, path) = temp();
        let top = block(300, hash_of(299), &[(9, &[1])]);
        hist.apply(&top).unwrap();
        let b299 = block(299, hash_of(298), &[(8, &[1])]);
        let b298 = block(298, hash_of(297), &[(7, &[2])]);
        hist.backfill(&[b299.clone(), b298.clone()]).unwrap();
        assert_eq!(hist.coverage().unwrap(), Some((298, 300)));
        assert_eq!(hist.low().unwrap(), Some(Low { height: 298, hash: hash_of(298), prev: hash_of(297) }));
        let h = hist.history(&[spk(1), spk(2)], 0, 10).unwrap().unwrap();
        assert_eq!(txids(&h), vec![(300, 9), (299, 8), (298, 7)]);
        let wrong = block(297, [1; 32], &[(6, &[1])]);
        let mut wrong_hash = wrong.clone();
        wrong_hash.hash = [5; 32];
        assert!(hist.backfill(&[wrong_hash]).is_err());
        assert!(hist.backfill(&[block(296, [0; 32], &[])]).is_err(), "a gap is refused");
        assert_eq!(hist.coverage().unwrap(), Some((298, 300)));
        let b297 = block(297, hash_of(296), &[]);
        let mut b296 = block(296, hash_of(295), &[]);
        b296.hash = [77; 32];
        assert!(hist.backfill(&[b297.clone(), b296]).is_err());
        hist.backfill(&[b297]).unwrap();
        assert_eq!(hist.coverage().unwrap(), Some((297, 300)));
        hist.rewind_to(299, &b299.hash).unwrap();
        hist.rewind_to(298, &b298.hash).unwrap();
        assert_eq!(hist.cursor().unwrap(), Some((298, b298.hash)));
        assert_eq!(txids(&hist.history(&[spk(1)], 0, 10).unwrap().unwrap()), vec![(299, 8)], "deep entries stay");
        drop(hist);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn prune_drops_day_tables_and_raises_the_bottom() {
        let (hist, path) = temp();
        hist.seed_for_test(10, 300, &[(spk(1), 10, [1; 32]), (spk(1), 150, [2; 32]), (spk(1), 300, [3; 32])])
            .unwrap();
        assert_eq!(txids(&hist.history(&[spk(1)], 0, 10).unwrap().unwrap()), vec![(300, 3), (150, 2), (10, 1)]);
        assert_eq!(hist.prune(100).unwrap(), 0);
        let h = hist.history(&[spk(1)], 100, 10).unwrap().unwrap();
        assert_eq!((h.from, txids(&h)), (100, vec![(300, 3), (150, 2)]));
        assert_eq!(hist.prune(200).unwrap(), 1);
        assert_eq!(hist.low().unwrap().map(|l| l.height), Some(144));
        assert_eq!(hist.coverage().unwrap(), Some((144, 300)));
        assert_eq!(txids(&hist.history(&[spk(1)], 0, 10).unwrap().unwrap()), vec![(300, 3), (150, 2)]);
        assert_eq!(hist.prune(200).unwrap(), 0, "idempotent");
        assert_eq!(hist.prune(290).unwrap(), 1);
        assert_eq!(hist.coverage().unwrap(), Some((288, 300)));
        drop(hist);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn a_node_block_becomes_entries() {
        let a = "0014".to_string() + &"aa".repeat(20);
        let b = "0014".to_string() + &"bb".repeat(20);
        let lite: BlockLite = serde_json::from_value(json!({
            "hash": "11".repeat(32),
            "height": 500,
            "previousblockhash": "22".repeat(32),
            "versionHex": "ignored",
            "tx": [
                { "txid": "cb".repeat(32), "vin": [{ "coinbase": "aa" }],
                  "vout": [{ "n": 0, "scriptPubKey": { "hex": b } }] },
                { "txid": "ee".repeat(32),
                  "vin": [{ "txid": "ff".repeat(32), "vout": 0, "prevout": { "value": 0.1, "scriptPubKey": { "hex": a } } }],
                  "vout": [{ "n": 0, "scriptPubKey": { "hex": b } }, { "n": 1, "scriptPubKey": { "hex": "6a00" } }] }
            ]
        }))
        .unwrap();
        let block = lite.into_block().unwrap();
        assert_eq!(block.height, 500);
        assert_eq!(block.hash, [0x11; 32]);
        assert_eq!(block.prev, [0x22; 32]);
        let spk_a = hex_decode(&a).unwrap();
        let spk_b = hex_decode(&b).unwrap();
        assert_eq!(
            block.entries,
            vec![
                Entry { spk: spk_b.clone(), txid: [0xcb; 32] },
                Entry { spk: spk_a, txid: [0xee; 32] },
                Entry { spk: spk_b, txid: [0xee; 32] },
            ]
        );
    }
}

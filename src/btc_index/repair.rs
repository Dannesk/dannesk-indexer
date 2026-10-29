
use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, TryStreamExt};
use redb::{ReadableDatabase, ReadableTable};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use super::BtcIndex;
use super::store::{UNDO, UTXOS, UtxoRecord};
use super::sync::{btc_to_sats, hex_decode, indexed_script, txid_bytes};
use crate::rpc::BtcRpc;

const BATCH: u32 = 10;
const FETCH_CONCURRENCY: usize = 3;
const REPORT_EVERY: u32 = 1_000;
const ATTEMPTS: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_secs(5);
const GETBLOCK_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Deserialize)]
pub(super) struct BlockTxs {
    tx: Vec<TxIo>,
}

#[derive(Deserialize)]
struct TxIo {
    txid: String,
    vin: Vec<VinRef>,
    vout: Vec<VoutCoin>,
}

#[derive(Deserialize)]
struct VinRef {
    #[serde(default)]
    txid: Option<String>,
    #[serde(default)]
    vout: Option<u32>,
}

#[derive(Deserialize)]
struct VoutCoin {
    value: f64,
    n: u32,
    #[serde(rename = "scriptPubKey")]
    script_pub_key: SpkHex,
}

#[derive(Deserialize)]
struct SpkHex {
    hex: String,
}

/// The coins a block both created and spent
pub(super) fn same_block_coins(height: u32, block: &BlockTxs) -> Result<Vec<(Vec<u8>, UtxoRecord)>> {
    let mut created: HashMap<([u8; 32], u32), (Vec<u8>, u64)> = HashMap::new();
    for tx in &block.tx {
        let txid = txid_bytes(&tx.txid)?;
        for out in &tx.vout {
            let spk = hex_decode(&out.script_pub_key.hex)?;
            if indexed_script(&spk) {
                created.insert((txid, out.n), (spk, btc_to_sats(out.value)));
            }
        }
    }
    let mut coins = Vec::new();
    for tx in &block.tx {
        for vin in &tx.vin {
            let (Some(prev), Some(vout)) = (vin.txid.as_deref(), vin.vout) else { continue };
            let txid = txid_bytes(prev)?;
            if let Some((spk, sats)) = created.remove(&(txid, vout)) {
                coins.push((spk, UtxoRecord { txid, vout, sats, height }));
            }
        }
    }
    Ok(coins)
}

struct Fetched {
    height: u32,
    hash: [u8; 32],
    coins: Vec<(Vec<u8>, UtxoRecord)>,
}

async fn fetch(rpc: &BtcRpc, height: u32) -> Result<Fetched> {
    let hash = rpc.call("getblockhash", json!([height])).await?;
    let hash = hash.as_str().context("getblockhash: not a string")?.to_string();
    let block: BlockTxs = rpc.call_typed("getblock", json!([hash, 2]), GETBLOCK_TIMEOUT).await?;
    Ok(Fetched { height, hash: txid_bytes(&hash)?, coins: same_block_coins(height, &block)? })
}

async fn fetch_heights(rpc: &BtcRpc, heights: &[u32], stop: &AtomicBool) -> Result<Vec<Fetched>> {
    let mut attempt = 1;
    loop {
        let batch: Result<Vec<Fetched>> = futures_util::stream::iter(heights.iter().copied())
            .map(|h| fetch(rpc, h))
            .buffered(FETCH_CONCURRENCY)
            .try_collect()
            .await;
        match batch {
            Ok(batch) => return Ok(batch),
            Err(e) if attempt < ATTEMPTS && !stop.load(Ordering::Relaxed) => {
                eprintln!("heights from {:?}: {:?}, retrying", heights.first(), e);
                attempt += 1;
                tokio::time::sleep(RETRY_DELAY).await;
                if stop.load(Ordering::Relaxed) {
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
        }
    }
}

/// Per block, the coins removed, or None where the index holds another block at that height.
fn remove(idx: &BtcIndex, batch: &[Fetched]) -> Result<Vec<Option<u64>>> {
    let write = idx.db.begin_write()?;
    let mut outcome = Vec::with_capacity(batch.len());
    {
        let undo = write.open_table(UNDO)?;
        let mut utxos = write.open_multimap_table(UTXOS)?;
        for block in batch {
            if let Some(row) = undo.get(block.height)?
                && row.value().get(..32) != Some(&block.hash[..])
            {
                outcome.push(None);
                continue;
            }
            let mut coins: Vec<&(Vec<u8>, UtxoRecord)> = block.coins.iter().collect();
            coins.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.encode().cmp(&b.1.encode())));
            let mut removed = 0u64;
            for (spk, rec) in coins {
                if utxos.remove(spk.as_slice(), rec.encode().as_slice())? {
                    removed += 1;
                }
            }
            outcome.push(Some(removed));
        }
    }
    write.commit()?;
    Ok(outcome)
}

async fn remove_fetched(idx: &BtcIndex, batch: Vec<Fetched>) -> Result<Vec<(u32, u64, Option<u64>)>> {
    let named: Vec<(u32, u64)> = batch.iter().map(|b| (b.height, b.coins.len() as u64)).collect();
    let i = idx.clone();
    let outcome = tokio::task::spawn_blocking(move || remove(&i, &batch)).await??;
    Ok(named.into_iter().zip(outcome).map(|((h, n), o)| (h, n, o)).collect())
}

fn lowest_undo(idx: &BtcIndex) -> Result<Option<(u32, [u8; 32])>> {
    let read = idx.db.begin_read()?;
    let undo = match read.open_table(UNDO) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let Some((height, row)) = undo.first()? else {
        return Ok(None);
    };
    let hash: [u8; 32] = row.value().get(..32).and_then(|b| b.try_into().ok()).context("short undo row")?;
    Ok(Some((height.value(), hash)))
}

async fn node_hash(rpc: &BtcRpc, height: u32) -> Result<[u8; 32]> {
    let hash = rpc.call("getblockhash", json!([height])).await?;
    txid_bytes(hash.as_str().context("getblockhash: not a string")?)
}

#[derive(Debug, PartialEq)]
enum Ending {
    Done,
    Stopped { next: u32 },
    Failed { next: u32, error: String },
}

struct Walk {
    start: u32,
    end: u32,
    tip: u32,
    through: Option<u32>,
    /// Height, coins named, coins removed, for every block that named any.
    blocks: Vec<(u32, u64, u64)>,
    skipped: Vec<u32>,
    ending: Ending,
}

impl Walk {
    fn add(&mut self, height: u32, named: u64, outcome: Option<u64>) {
        match outcome {
            None => self.skipped.push(height),
            Some(removed) if named > 0 => self.blocks.push((height, named, removed)),
            Some(_) => {}
        }
    }

    fn totals(&self) -> (u64, u64) {
        self.blocks.iter().fold((0, 0), |(n, r), b| (n + b.1, r + b.2))
    }
}

#[derive(Debug, PartialEq)]
struct Verdict {
    first: Option<u32>,
    named_before: u64,
    named_since: u64,
    removed_since: u64,
    /// Heights from the first removal where removed differs.
    odd: Vec<(u32, u32)>,
}

fn verdict(blocks: &[(u32, u64, u64)]) -> Verdict {
    let first = blocks.iter().find(|b| b.2 > 0).map(|b| b.0);
    let mut v = Verdict { first, named_before: 0, named_since: 0, removed_since: 0, odd: Vec::new() };
    for &(height, named, removed) in blocks {
        if first.is_some_and(|f| height >= f) {
            v.named_since += named;
            v.removed_since += removed;
            if named != removed {
                match v.odd.last_mut() {
                    Some((_, end)) if *end + 1 == height => *end = height,
                    _ => v.odd.push((height, height)),
                }
            }
        } else {
            v.named_before += named;
        }
    }
    v
}

async fn walk(idx: &BtcIndex, rpc: &BtcRpc, from: Option<u32>, until: Option<u32>, stop: &AtomicBool) -> Result<Walk> {
    let (tip, _) = idx.tip()?.context("the index is empty")?;
    let node_tip = rpc.call("getblockcount", json!([])).await?.as_u64().context("getblockcount: not a number")? as u32;
    let prune = super::history::prune_height(rpc).await?;
    let end = until.unwrap_or(tip).min(tip);
    if node_tip < end {
        bail!("the node's tip {} is below the walk's end {}: let the node catch up, or pass an end height", node_tip, end);
    }
    if let Some((height, hash)) = lowest_undo(idx)? {
        if height > node_tip {
            bail!("the node's tip {} is below the index's undo window from {}: let the node catch up", node_tip, height);
        }
        if node_hash(rpc, height).await? != hash {
            bail!("the node's block {} is not the index's, so they part below the undo window: rebuild the index", height);
        }
    }
    let start = from.unwrap_or(prune).max(prune);
    if start > end {
        bail!("nothing to walk: the start {} is above the end {}", start, end);
    }
    println!(
        "walking heights {}..={} (index tip {}, node tip {}, node prune height {})",
        start, end, tip, node_tip, prune
    );

    let started = Instant::now();
    let mut walked =
        Walk { start, end, tip, through: None, blocks: Vec::new(), skipped: Vec::new(), ending: Ending::Done };
    let mut since_report = 0u32;
    let mut height = start;
    while height <= end {
        if stop.load(Ordering::Relaxed) {
            walked.ending = Ending::Stopped { next: height };
            return Ok(walked);
        }
        let through = height.saturating_add(BATCH - 1).min(end);
        let heights: Vec<u32> = (height..=through).collect();
        let removed = match fetch_heights(rpc, &heights, stop).await {
            Ok(batch) => remove_fetched(idx, batch).await,
            Err(e) => Err(e),
        };
        match removed {
            Ok(outcome) => {
                for (h, named, o) in outcome {
                    walked.add(h, named, o);
                }
                walked.through = Some(through);
            }
            Err(e) => {
                walked.ending = Ending::Failed { next: height, error: format!("{:?}", e) };
                return Ok(walked);
            }
        }
        since_report += through - height + 1;
        if since_report >= REPORT_EVERY || through == end {
            since_report = 0;
            let (named, removed) = walked.totals();
            let first = verdict(&walked.blocks).first;
            println!(
                "through {}: {} coins named, {} removed{} ({:.0} blocks/s)",
                through,
                named,
                removed,
                first.map(|f| format!(", the first removal at {}", f)).unwrap_or_else(|| ", none removed yet".to_string()),
                (through - start + 1) as f64 / started.elapsed().as_secs_f64()
            );
        }
        height = through + 1;
    }

    if !walked.skipped.is_empty() {
        let again = std::mem::take(&mut walked.skipped);
        println!("fetching the {} skipped heights again", again.len());
        for chunk in again.chunks(BATCH as usize) {
            let refetched = if stop.load(Ordering::Relaxed) { None } else { fetch_heights(rpc, chunk, stop).await.ok() };
            match refetched {
                Some(batch) => match remove_fetched(idx, batch).await {
                    Ok(outcome) => {
                        for (h, named, o) in outcome {
                            walked.add(h, named, o);
                        }
                    }
                    Err(e) => {
                        walked.skipped.extend_from_slice(chunk);
                        walked.ending = Ending::Failed { next: chunk[0], error: format!("{:?}", e) };
                        return Ok(walked);
                    }
                },
                None => walked.skipped.extend_from_slice(chunk),
            }
        }
    }
    Ok(walked)
}


fn conclude(walked: &Walk, store: &str) -> (Vec<String>, bool) {
    let v = verdict(&walked.blocks);
    let complete = walked.ending == Ending::Done;
    let mut lines = vec![match walked.through {
        Some(through) if complete => format!("walked heights {}..={}", walked.start, through),
        Some(through) => format!("walked heights {}..={} of {}..={}", walked.start, through, walked.start, walked.end),
        None => format!("walked no height of {}..={}", walked.start, walked.end),
    }];
    if walked.end < walked.tip {
        lines.push(format!(
            "the end {} is below the index tip {}, so heights {}..={} were not walked",
            walked.end,
            walked.tip,
            walked.end + 1,
            walked.tip
        ));
    }
    match v.first {
        Some(first) => {
            lines.push(format!(
                "{} coins named before height {}, where removals start; from {} on, {} named and {} removed",
                v.named_before, first, first, v.named_since, v.removed_since
            ));
            if v.named_before == 0 {
                lines.push("nothing was named before the first removal, so the walk may have started after the index was built".to_string());
            }
        }
        None if complete => lines.push("no leftovers found".to_string()),
        None => lines.push("no leftovers found so far".to_string()),
    }
    if !walked.skipped.is_empty() {
        lines.push(format!(
            "still skipped, the index holds another block there (the daemon rewinds it at start): {:?}",
            walked.skipped
        ));
    }
    if !v.odd.is_empty() {
        lines.push(format!(
            "FAILED: these heights had coins named but not all removed, look at them before any rerun: {:?}",
            v.odd
        ));
    }
    match &walked.ending {
        Ending::Done => {}
        Ending::Stopped { next } => lines.push(format!("STOPPED before {}", next)),
        Ending::Failed { next, error } => lines.push(format!("FAILED at {}: {}", next, error)),
    }
    if !complete && v.odd.is_empty() {
        lines.push(format!(
            "rerun `indexd repair-same-block {} {} {}` the same way; the batches already done find nothing again",
            store, walked.start, walked.end
        ));
    }
    let ok = v.odd.is_empty() && complete;
    if ok {
        lines.push(match v.first {
            Some(first) => format!("repair complete: every block from {} on lost all the coins it named", first),
            None => "repair complete: nothing to remove".to_string(),
        });
    }
    (lines, ok)
}

pub async fn run_cli(store: &str, from: Option<u32>, until: Option<u32>) -> Result<()> {
    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
            let terminated = async {
                match term.as_mut() {
                    Some(t) => {
                        t.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminated => {}
            }
            stop.store(true, Ordering::Relaxed);
            println!("stopping after the batch in progress");
        });
    }
    let rpc = BtcRpc::from_env("http://127.0.0.1:8332")?;
    println!("opening {} (a store that was not closed cleanly takes minutes to repair here)", store);
    let idx = BtcIndex::open(store)?;

    let started = Instant::now();
    let walked = walk(&idx, &rpc, from, until, &stop).await?;
    let (lines, ok) = conclude(&walked, store);
    for line in &lines {
        println!("{}", line);
    }
    println!("done in {:.0}s", started.elapsed().as_secs_f64());
    if !ok {
        bail!("the repair did not complete, see above");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btc_index::store::{META, META_TIP, UndoRow};
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const A: &str = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4";
    const B: &str = "12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX";

    fn temp_index() -> (BtcIndex, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "btc-repair-test-{}-{:?}.redb",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        (BtcIndex::open(path.to_str().unwrap()).unwrap(), path)
    }

    fn hex_rev(b: &[u8; 32]) -> String {
        b.iter().rev().map(|x| format!("{:02x}", x)).collect()
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{:02x}", x)).collect()
    }

    /// A 32-byte value whose bytes all differ.
    fn distinct(seed: u8) -> [u8; 32] {
        let mut b = [0u8; 32];
        for (i, x) in b.iter_mut().enumerate() {
            *x = seed.wrapping_add(i as u8 * 7);
        }
        b
    }

    fn txid(name: u8) -> [u8; 32] {
        distinct(name)
    }

    /// BB spends an older coin, CC spends BB:1 and DD spends CC:0.
    fn block_json(spk_a: &[u8], spk_b: &[u8]) -> Value {
        json!({
            "tx": [
                { "txid": hex_rev(&txid(0xCB)), "vin": [{ "coinbase": "aa" }],
                  "vout": [{ "n": 0, "value": 3.125, "scriptPubKey": { "hex": hex(spk_b) } }] },
                { "txid": hex_rev(&txid(0xBB)), "vin": [{ "txid": hex_rev(&txid(0xAA)), "vout": 0 }],
                  "vout": [
                      { "n": 0, "value": 0.0003, "scriptPubKey": { "hex": hex(spk_b) } },
                      { "n": 1, "value": 0.00019, "scriptPubKey": { "hex": hex(spk_a) } },
                      { "n": 2, "value": 0.0, "scriptPubKey": { "hex": "6a04deadbeef" } }
                  ] },
                { "txid": hex_rev(&txid(0xCC)), "vin": [{ "txid": hex_rev(&txid(0xBB)), "vout": 1 }],
                  "vout": [{ "n": 0, "value": 0.00018, "scriptPubKey": { "hex": hex(spk_b) } }] },
                { "txid": hex_rev(&txid(0xDD)), "vin": [{ "txid": hex_rev(&txid(0xCC)), "vout": 0 }],
                  "vout": [{ "n": 0, "value": 0.00017, "scriptPubKey": { "hex": hex(spk_a) } }] }
            ]
        })
    }

    fn block(spk_a: &[u8], spk_b: &[u8]) -> BlockTxs {
        serde_json::from_value(block_json(spk_a, spk_b)).unwrap()
    }

    fn left_overs(height: u32, spk_a: &[u8], spk_b: &[u8]) -> Vec<(Vec<u8>, UtxoRecord)> {
        vec![
            (spk_a.to_vec(), UtxoRecord { txid: txid(0xBB), vout: 1, sats: 19_000, height }),
            (spk_b.to_vec(), UtxoRecord { txid: txid(0xCC), vout: 0, sats: 18_000, height }),
        ]
    }

    fn block_hash(height: u32) -> [u8; 32] {
        distinct(height as u8)
    }

    fn set_undo(idx: &BtcIndex, height: u32, this_hash: [u8; 32]) {
        let write = idx.db.begin_write().unwrap();
        {
            let mut undo = write.open_table(UNDO).unwrap();
            let row = UndoRow { this_hash, prev_hash: [0; 32], spent: vec![], created: vec![] };
            undo.insert(height, row.encode().as_slice()).unwrap();
        }
        write.commit().unwrap();
    }

    fn set_tip(idx: &BtcIndex, height: u32) {
        let write = idx.db.begin_write().unwrap();
        {
            let mut meta = write.open_table(META).unwrap();
            let mut tip = height.to_le_bytes().to_vec();
            tip.extend_from_slice(&block_hash(height));
            meta.insert(META_TIP, tip.as_slice()).unwrap();
        }
        write.commit().unwrap();
    }

    #[test]
    fn a_chain_spent_within_the_block_is_named_link_by_link() {
        let spk_a = BtcIndex::script_pubkey_for(A).unwrap();
        let spk_b = BtcIndex::script_pubkey_for(B).unwrap();
        assert_eq!(
            same_block_coins(101, &block(&spk_a, &spk_b)).unwrap(),
            left_overs(101, &spk_a, &spk_b),
            "AA:0 is older than the block; CB:0, BB:0 and DD:0 are unspent"
        );
    }

    #[test]
    fn a_block_loses_its_leftovers_only_if_the_index_holds_that_block() {
        let (idx, path) = temp_index();
        let spk_a = BtcIndex::script_pubkey_for(A).unwrap();
        let spk_b = BtcIndex::script_pubkey_for(B).unwrap();
        let kept = UtxoRecord { txid: txid(0xEE), vout: 0, sats: 7, height: 99 };
        idx.insert_for_test(&spk_a, &kept).unwrap();
        for h in [101, 102, 103] {
            for (spk, rec) in left_overs(h, &spk_a, &spk_b) {
                idx.insert_for_test(&spk, &rec).unwrap();
            }
        }
        set_undo(&idx, 101, block_hash(101));
        set_undo(&idx, 102, block_hash(102));
        let fetched = |height: u32, hash: [u8; 32]| Fetched {
            height,
            hash,
            coins: same_block_coins(height, &block(&spk_a, &spk_b)).unwrap(),
        };
        let mut reversed = block_hash(101);
        reversed.reverse();

        let outcome = remove(&idx, &[fetched(101, reversed), fetched(102, block_hash(102)), fetched(103, block_hash(3))]).unwrap();
        assert_eq!(outcome, vec![None, Some(2), Some(2)], "101 in the wrong byte order is another block; 103 is below the undo window");
        assert_eq!(remove(&idx, &[fetched(101, block_hash(101))]).unwrap(), vec![Some(2)]);
        assert_eq!(idx.utxos_for_spk(&spk_a).unwrap(), vec![kept]);
        assert_eq!(remove(&idx, &[fetched(101, block_hash(101))]).unwrap(), vec![Some(0)], "a second walk finds nothing");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn the_verdict_wants_every_coin_removed_from_the_first_removal_on() {
        let clean = verdict(&[(945_254, 3, 0), (961_700, 2, 2), (961_702, 1, 1)]);
        assert_eq!(clean, Verdict { first: Some(961_700), named_before: 3, named_since: 3, removed_since: 3, odd: vec![] });
        let odd = verdict(&[(10, 1, 0), (20, 2, 2), (21, 2, 1), (22, 1, 0), (30, 1, 1), (31, 3, 2)]);
        assert_eq!(odd.first, Some(20));
        assert_eq!(odd.odd, vec![(21, 22), (31, 31)]);
        assert_eq!(verdict(&[(5, 4, 0)]).first, None);
    }

    #[test]
    fn a_run_succeeds_only_when_it_ends_and_every_coin_from_the_first_removal_on_went() {
        let walk = |blocks: Vec<(u32, u64, u64)>, ending: Ending, through: Option<u32>, tip: u32| Walk {
            start: 1,
            end: 9,
            tip,
            through,
            blocks,
            skipped: vec![],
            ending,
        };
        let (lines, ok) = conclude(&walk(vec![(1, 1, 0), (5, 2, 2)], Ending::Done, Some(9), 9), "s");
        assert!(ok && lines.last().is_some_and(|l| l.starts_with("repair complete")), "{:?}", lines);
        assert!(!conclude(&walk(vec![(5, 2, 2), (6, 2, 1)], Ending::Done, Some(9), 9), "s").1);

        let (lines, ok) = conclude(&walk(vec![(5, 2, 2)], Ending::Stopped { next: 7 }, Some(6), 9), "s");
        assert!(!ok);
        assert_eq!(lines[0], "walked heights 1..=6 of 1..=9");
        assert!(lines.iter().any(|l| l.contains("`indexd repair-same-block s 1 9`")), "the rerun keeps the start and the end: {:?}", lines);
        assert!(lines.iter().any(|l| l.contains("may have started after the index was built")));

        let (lines, ok) = conclude(&walk(vec![(5, 2, 1)], Ending::Stopped { next: 7 }, Some(6), 9), "s");
        assert!(!ok && !lines.iter().any(|l| l.contains("rerun `")), "failed heights come before any rerun: {:?}", lines);

        let (lines, ok) = conclude(&walk(vec![], Ending::Failed { next: 3, error: "no answer".into() }, None, 9), "s");
        assert!(!ok && lines.contains(&"no leftovers found so far".to_string()) && lines[0] == "walked no height of 1..=9");

        let (lines, _) = conclude(&walk(vec![], Ending::Done, Some(9), 12), "s");
        assert!(lines.iter().any(|l| l.contains("heights 10..=12 were not walked")));
    }

    async fn fake_node(blocks: HashMap<u32, Value>, prune: u32, tip: u32, flaky: Vec<u32>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let blocks = Arc::new(blocks);
        let flaky = Arc::new(std::sync::Mutex::new(flaky));
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let blocks = Arc::clone(&blocks);
                let flaky = Arc::clone(&flaky);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let body = loop {
                        let Ok(n) = sock.read(&mut chunk).await else { return };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        let text = String::from_utf8_lossy(&buf).to_string();
                        if let Some(at) = text.find("\r\n\r\n") {
                            let length = text[..at]
                                .lines()
                                .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse::<usize>().unwrap_or(0)))
                                .unwrap_or(0);
                            if buf.len() >= at + 4 + length {
                                break buf[at + 4..at + 4 + length].to_vec();
                            }
                        }
                    };
                    let req: Value = serde_json::from_slice(&body).unwrap_or_default();
                    let result = match req["method"].as_str().unwrap_or_default() {
                        "getblockcount" => json!(tip),
                        "getblockchaininfo" => json!({ "pruneheight": prune }),
                        "getblockhash" => {
                            let height = req["params"][0].as_u64().unwrap_or(0) as u32;
                            let mut hash = block_hash(height);
                            let mut flaky = flaky.lock().unwrap();
                            if let Some(at) = flaky.iter().position(|h| *h == height) {
                                flaky.remove(at);
                                hash.reverse();
                            }
                            json!(hex_rev(&hash))
                        }
                        "getblock" => {
                            let internal = txid_bytes(req["params"][0].as_str().unwrap_or_default()).unwrap_or_default();
                            blocks.get(&(internal[0] as u32)).cloned().unwrap_or(json!({ "tx": [] }))
                        }
                        _ => Value::Null,
                    };
                    let text = json!({ "result": result, "error": null, "id": req["id"] }).to_string();
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

    #[tokio::test]
    async fn a_walk_removes_every_leftover_between_the_prune_height_and_the_index_tip() {
        let (idx, path) = temp_index();
        let spk_a = BtcIndex::script_pubkey_for(A).unwrap();
        let spk_b = BtcIndex::script_pubkey_for(B).unwrap();
        let kept = UtxoRecord { txid: txid(0xEE), vout: 0, sats: 7, height: 99 };
        idx.insert_for_test(&spk_a, &kept).unwrap();
        for h in [101, 102] {
            for (spk, rec) in left_overs(h, &spk_a, &spk_b) {
                idx.insert_for_test(&spk, &rec).unwrap();
            }
        }
        set_tip(&idx, 102);
        set_undo(&idx, 101, block_hash(101));
        set_undo(&idx, 102, block_hash(102));
        let blocks: HashMap<u32, Value> =
            [98, 99, 100, 101, 102, 103].into_iter().map(|h| (h, block_json(&spk_a, &spk_b))).collect();
        let rpc = BtcRpc::for_test(&fake_node(blocks, 99, 104, vec![]).await);

        let walked = walk(&idx, &rpc, Some(90), None, &AtomicBool::new(false)).await.unwrap();
        assert_eq!((walked.start, walked.end), (99, 102), "from the prune height to the index tip");
        let v = verdict(&walked.blocks);
        assert_eq!((v.first, v.named_before, v.named_since, v.removed_since), (Some(101), 4, 4, 4));
        assert!(v.odd.is_empty() && walked.skipped.is_empty() && walked.ending == Ending::Done);
        assert_eq!(idx.utxos_for_spk(&spk_a).unwrap(), vec![kept]);
        assert!(idx.utxos_for_spk(&spk_b).unwrap().is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn a_walk_refuses_what_it_cannot_cover() {
        let (idx, path) = temp_index();
        set_tip(&idx, 120);
        set_undo(&idx, 119, block_hash(119));
        let behind = BtcRpc::for_test(&fake_node(HashMap::new(), 100, 118, vec![]).await);
        let refused = |r: Result<Walk>| r.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(refused(walk(&idx, &behind, None, None, &AtomicBool::new(false)).await).contains("below the walk's end"));
        assert!(refused(walk(&idx, &behind, None, Some(110), &AtomicBool::new(false)).await).contains("below the index's undo window"));
        let level = BtcRpc::for_test(&fake_node(HashMap::new(), 100, 125, vec![]).await);
        assert!(refused(walk(&idx, &level, Some(121), None, &AtomicBool::new(false)).await).contains("nothing to walk"));
        set_undo(&idx, 118, distinct(1));
        assert!(refused(walk(&idx, &level, None, None, &AtomicBool::new(false)).await).contains("rebuild the index"));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn a_height_skipped_for_another_block_is_fetched_again_at_the_end() {
        let (idx, path) = temp_index();
        let spk_a = BtcIndex::script_pubkey_for(A).unwrap();
        let spk_b = BtcIndex::script_pubkey_for(B).unwrap();
        for (spk, rec) in left_overs(102, &spk_a, &spk_b) {
            idx.insert_for_test(&spk, &rec).unwrap();
        }
        set_tip(&idx, 102);
        set_undo(&idx, 101, block_hash(101));
        set_undo(&idx, 102, block_hash(102));
        let blocks: HashMap<u32, Value> = [101, 102].into_iter().map(|h| (h, block_json(&spk_a, &spk_b))).collect();
        let rpc = BtcRpc::for_test(&fake_node(blocks, 101, 102, vec![102]).await);

        let walked = walk(&idx, &rpc, None, None, &AtomicBool::new(false)).await.unwrap();
        assert!(walked.skipped.is_empty(), "the second fetch finds the index's block: {:?}", walked.skipped);
        assert_eq!(walked.blocks.iter().find(|b| b.0 == 102).map(|b| b.2), Some(2));
        assert!(idx.utxos_for_spk(&spk_a).unwrap().is_empty());
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn a_stop_request_ends_the_walk_before_the_next_batch() {
        let (idx, path) = temp_index();
        set_tip(&idx, 130);
        let rpc = BtcRpc::for_test(&fake_node(HashMap::new(), 100, 130, vec![]).await);
        let walked = walk(&idx, &rpc, None, None, &AtomicBool::new(true)).await.unwrap();
        assert_eq!(walked.ending, Ending::Stopped { next: 100 });
        assert!(walked.blocks.is_empty());
        let _ = std::fs::remove_file(path);
    }
}

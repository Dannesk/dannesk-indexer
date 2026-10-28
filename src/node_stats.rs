
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tracing::{debug, info, warn};

use crate::btc_index::BtcIndex;
use crate::rpc::BtcRpc;
use crate::server::Ready;

const TICK: Duration = Duration::from_secs(30);

const WALK_FLOOR: Duration = Duration::from_secs(120);

const RING: u32 = 144;

const DEPTHS: [u64; 3] = [500_000, 2_500_000, 5_500_000];

const TRAIN: usize = 13;

const BANDS: usize = 14;

const TOP_DEPTH: u64 = 50_000;

const EPOCH: u32 = 2016;

const NEXT_BLOCK_VSIZE: u64 = 1_000_000;

#[derive(Clone, Debug, PartialEq)]
pub struct BlockStat {
    pub height: u32,
    pub at: u64,
    pub txs: u64,
    pub weight: u64,
    pub feerate: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Band {
    pub from: f64,
    pub vsize: u64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeStats {
    pub sync: Option<f64>,
    pub peers: Option<u64>,
    pub tip_height: Option<u32>,
    pub tip_at: Option<u64>,
    pub avg_interval: Option<f64>,
    pub tiers: Option<[f64; 4]>,
    pub fee_scale_top: Option<f64>,
    pub incremental_sat_vb: Option<f64>,
    pub mempool_txs: Option<u64>,
    pub next_block_txs: Option<u64>,
    pub pending_vsize: Option<u64>,
    pub bands: Vec<Band>,
    pub blocks: Vec<BlockStat>,
    pub tip_time: Option<u64>,
    pub epoch_start_time: Option<u64>,
    pub node_height: Option<u32>,
    pub headers: Option<u32>,
    pub behind_secs: Option<u64>,
}

impl NodeStats {
    pub fn frame(&self) -> String {
        json!({
            "type": "node_stats",
            "sync": self.sync,
            "peers": self.peers,
            "tip_height": self.tip_height,
            "tip_at": self.tip_at,
            "avg_interval": self.avg_interval,
            "tiers": self.tiers.map(|t| t.to_vec()),
            "fee_scale_top": self.fee_scale_top,
            "incremental_sat_vb": self.incremental_sat_vb,
            "mempool_txs": self.mempool_txs,
            "next_block_txs": self.next_block_txs,
            "pending_vsize": self.pending_vsize,
            "bands": self.bands.iter().map(|b| json!({
                "from": (b.from * 1000.0).round() / 1000.0,
                "vsize": b.vsize,
            })).collect::<Vec<_>>(),
            "blocks": self.blocks.iter().map(|b| json!({
                "height": b.height,
                "at": b.at,
                "txs": b.txs,
                "weight": b.weight,
                "feerate": b.feerate,
            })).collect::<Vec<_>>(),
            "node_height": self.node_height,
            "headers": self.headers,
            "behind_secs": self.behind_secs,
            "tip_time": self.tip_time,
            "epoch_start_time": self.epoch_start_time,
        })
        .to_string()
    }
}

fn behind_for(
    headers: Option<u32>,
    blocks: Option<u32>,
    since: &mut Option<std::time::Instant>,
    now: std::time::Instant,
) -> Option<u64> {
    let (Some(h), Some(b)) = (headers, blocks) else {
        return None;
    };
    if h > b {
        let at = *since.get_or_insert(now);
        Some(now.saturating_duration_since(at).as_secs())
    } else {
        *since = None;
        Some(0)
    }
}

fn remember(blocks: &mut Vec<BlockStat>, b: BlockStat) {
    blocks.retain(|x| x.height != b.height);
    let at = blocks
        .iter()
        .position(|x| x.height < b.height)
        .unwrap_or(blocks.len());
    blocks.insert(at, b);
    blocks.truncate(TRAIN);
}

pub fn spawn(
    rpc: BtcRpc,
    ready_rx: watch::Receiver<Option<Ready>>,
    blocks: broadcast::Sender<String>,
    stats_tx: watch::Sender<NodeStats>,
) {
    tokio::spawn(run(rpc, ready_rx, blocks, stats_tx));
}

async fn run(
    rpc: BtcRpc,
    ready_rx: watch::Receiver<Option<Ready>>,
    blocks: broadcast::Sender<String>,
    stats_tx: watch::Sender<NodeStats>,
) {
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut blocks_rx = blocks.subscribe();

    let mut stats = NodeStats::default();
    let mut floor: Option<f64> = None;
    let mut behind_since: Option<std::time::Instant> = None;
    let mut last_walk: Option<tokio::time::Instant> = None;
    let mut ring_at: Option<u32> = None;
    let mut epoch_at: Option<u32> = None;

    loop {
        let block_edge = tokio::select! {
            _ = tick.tick() => false,
            event = blocks_rx.recv() => match event {
                Ok(_) => true,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    debug!("node_stats lagged {n} block frames");
                    true
                }
                Err(broadcast::error::RecvError::Closed) => {
                    warn!("block channel closed — node_stats runs on its tick only");
                    tick.tick().await;
                    false
                }
            },
        };

        cheap_probes(&rpc, &ready_rx, &mut stats, &mut floor, &mut behind_since).await;

        let due = last_walk.is_none_or(|t| t.elapsed() >= WALK_FLOOR);
        if block_edge || due {
            match walk_mempool(&rpc, floor).await {
                Ok(walk) => {
                    if stats.tiers.is_none() {
                        info!(
                            "node_stats live: tiers floor={:.3} low={:.3} med={:.3} high={:.3} sat/vB, {} tx in mempool, {} fit the next block",
                            walk.tiers[0], walk.tiers[1], walk.tiers[2], walk.tiers[3],
                            walk.mempool_txs, walk.next_block_txs
                        );
                    }
                    stats.tiers = Some(walk.tiers);
                    stats.mempool_txs = Some(walk.mempool_txs);
                    stats.next_block_txs = Some(walk.next_block_txs);
                    stats.pending_vsize = Some(walk.pending_vsize);
                    stats.bands = walk.bands;
                }
                Err(e) => {
                    debug!("mempool walk failed: {e:?}");
                }
            }
            last_walk = Some(tokio::time::Instant::now());
        }

        if let Some(h) = stats.tip_height {
            if ring_at != Some(h) {
                match block_window(&rpc, h).await {
                    Ok((avg, tip)) => {
                        stats.avg_interval = avg;
                        stats.fee_scale_top = tip.as_ref().map(|t| t.feerate);
                        let now = now_secs();
                        let tip_time = tip.as_ref().map(|t| t.time);
                        stats.tip_time = tip_time;
                        stats.tip_at = if block_edge {
                            Some(now)
                        } else {
                            tip_time.map(|t| t.min(now)).or(stats.tip_at)
                        };
                        stats.blocks.retain(|b| b.height < h);
                        if let Some(t) = tip {
                            remember(
                                &mut stats.blocks,
                                BlockStat {
                                    height: h,
                                    at: stats.tip_at.unwrap_or(now),
                                    txs: t.txs,
                                    weight: t.weight,
                                    feerate: t.feerate,
                                },
                            );
                        }
                        ring_at = Some(h);
                    }
                    Err(e) => warn!("block window failed at {h}: {e:?}"),
                }
                backfill(&rpc, h, &mut stats.blocks).await;
            }
            let start = h - h % EPOCH;
            if epoch_at != Some(start) {
                match header_time(&rpc, start).await {
                    Ok(t) => {
                        stats.epoch_start_time = t;
                        epoch_at = Some(start);
                    }
                    Err(e) => warn!("epoch header failed at {start}: {e:?}"),
                }
            }
        }

        stats_tx.send_if_modified(|cur| {
            if *cur == stats {
                false
            } else {
                *cur = stats.clone();
                true
            }
        });
    }
}

async fn cheap_probes(
    rpc: &BtcRpc,
    ready_rx: &watch::Receiver<Option<Ready>>,
    stats: &mut NodeStats,
    floor: &mut Option<f64>,
    behind_since: &mut Option<std::time::Instant>,
) {
    stats.peers = rpc
        .call("getconnectioncount", json!([]))
        .await
        .ok()
        .and_then(|v| v.as_u64());

    let pool = rpc.call("getmempoolinfo", json!([])).await.ok();
    stats.incremental_sat_vb = pool
        .as_ref()
        .and_then(|v| v["incrementalrelayfee"].as_f64())
        .map(|f| f * 1e8 / 1000.0);
    if let Some(f) = pool
        .as_ref()
        .and_then(|v| v["mempoolminfee"].as_f64())
        .map(|f| f * 1e8 / 1000.0)
    {
        *floor = Some(f);
        if let Some(t) = stats.tiers.as_mut() {
            t[0] = f;
            for v in t.iter_mut() {
                if *v < f {
                    *v = f;
                }
            }
        }
    }

    let chain = rpc.call("getblockchaininfo", json!([])).await.ok();
    let node_h = chain.as_ref().and_then(|v| v["blocks"].as_u64());
    stats.node_height = node_h.map(|h| h as u32);
    stats.headers = chain.as_ref().and_then(|v| v["headers"].as_u64()).map(|h| h as u32);
    stats.behind_secs = behind_for(stats.headers, stats.node_height, behind_since, std::time::Instant::now());

    let our_h = ready_rx
        .borrow()
        .as_ref()
        .and_then(|r| index_tip(&r.idx));

    stats.tip_height = our_h;
    stats.sync = match (our_h, node_h) {
        (Some(o), Some(n)) if n > 0 => Some((o as f64 / n as f64).clamp(0.0, 1.0)),
        _ => None,
    };
}

fn index_tip(idx: &BtcIndex) -> Option<u32> {
    idx.tip().ok().flatten().map(|(h, _)| h)
}

async fn walk_mempool(rpc: &BtcRpc, floor: Option<f64>) -> anyhow::Result<Walk> {
    let mp = rpc.call("getrawmempool", json!([true])).await?;
    let rates = sorted_rates(&mp);
    let tiers = tiers_from(&rates, floor.unwrap_or(0.0));
    let next_block_txs = fits_in(&rates, NEXT_BLOCK_VSIZE);
    let (bands, pending_vsize) = histogram(&rates, floor.unwrap_or(0.0));
    debug!(
        "mempool walk: {} tx / {} vB, tiers floor={:.3} low={:.3} med={:.3} high={:.3}, {} fit the next block",
        rates.len(),
        pending_vsize,
        tiers[0],
        tiers[1],
        tiers[2],
        tiers[3],
        next_block_txs
    );
    Ok(Walk { tiers, mempool_txs: rates.len() as u64, next_block_txs, pending_vsize, bands })
}

struct Walk {
    tiers: [f64; 4],
    mempool_txs: u64,
    next_block_txs: u64,
    pending_vsize: u64,
    bands: Vec<Band>,
}

fn histogram(rates: &[(String, f64, u64)], floor: f64) -> (Vec<Band>, u64) {
    let pending: u64 = rates.iter().map(|(_, _, v)| v).sum();
    if rates.is_empty() {
        return (Vec::new(), 0);
    }
    let cheapest = rates.last().map(|r| r.1).unwrap_or(0.0);
    let left = if floor > 0.0 { floor } else { cheapest }.max(1e-3);
    let [top] = rates_at_depths(rates, &[TOP_DEPTH], left);
    let top = top.max(left * 2.0);
    let ratio = top / left;
    let edges: Vec<f64> = (0..BANDS)
        .map(|i| left * ratio.powf(i as f64 / (BANDS - 1) as f64))
        .collect();
    let mut sums = vec![0u64; BANDS];
    for (_, rate, vsize) in rates {
        let i = edges.iter().rposition(|e| *rate >= *e).unwrap_or(0);
        sums[i] += vsize;
    }
    let bands = edges
        .into_iter()
        .zip(sums)
        .map(|(from, vsize)| Band { from, vsize })
        .collect();
    (bands, pending)
}

fn fits_in(rates: &[(String, f64, u64)], cap: u64) -> u64 {
    let mut acc = 0u64;
    let mut n = 0u64;
    for (_, _, vsize) in rates {
        acc += vsize;
        if acc > cap {
            break;
        }
        n += 1;
    }
    n
}

fn sorted_rates(mp: &Value) -> Vec<(String, f64, u64)> {
    let mut rates: Vec<(String, f64, u64)> = mp
        .as_object()
        .map(|o| {
            o.iter()
                .filter_map(|(txid, e)| {
                    // The sigop-adjusted size miners rank by: vsize_adjusted since Core 32, vsize before.
                    let vsize = e["vsize_adjusted"].as_u64().or_else(|| e["vsize"].as_u64()).filter(|v| *v > 0)?;
                    let base = e["fees"]["base"].as_f64()?;
                    Some((txid.clone(), base * 1e8 / vsize as f64, vsize))
                })
                .collect()
        })
        .unwrap_or_default();
    rates.sort_by(|a, b| b.1.total_cmp(&a.1));
    rates
}

fn rates_at_depths<const N: usize>(
    rates: &[(String, f64, u64)],
    depths: &[u64; N],
    floor: f64,
) -> [f64; N] {
    let mut out = [floor; N];
    let mut acc: u64 = 0;
    let mut depth = 0usize;
    for (_, rate, vsize) in rates {
        acc += vsize;
        while depth < N && acc >= depths[depth] {
            out[depth] = rate.max(floor);
            depth += 1;
        }
        if depth == N {
            break;
        }
    }
    out
}

fn tiers_from(rates: &[(String, f64, u64)], floor: f64) -> [f64; 4] {
    let [high, med, low] = rates_at_depths(rates, &DEPTHS, floor);
    [floor, low, med, high]
}

async fn block_window(
    rpc: &BtcRpc,
    tip: u32,
) -> anyhow::Result<(Option<f64>, Option<BlockFacts>)> {
    let back = tip.saturating_sub(RING - 1);
    let tip_facts = block_stats(rpc, tip).await?;
    let tip_time = tip_facts.as_ref().map(|f| f.time);
    let avg = match (tip_time, header_time(rpc, back).await?) {
        (Some(t_now), Some(t_then)) if tip > back && t_now > t_then => {
            Some((t_now - t_then) as f64 / (tip - back) as f64)
        }
        _ => None,
    };
    Ok((avg, tip_facts))
}

async fn backfill(rpc: &BtcRpc, tip: u32, blocks: &mut Vec<BlockStat>) {
    let now = now_secs();
    for back in 1..TRAIN as u32 {
        let Some(h) = tip.checked_sub(back) else { break };
        if blocks.iter().any(|b| b.height == h) {
            continue;
        }
        match block_stats(rpc, h).await {
            Ok(Some(f)) => remember(
                blocks,
                BlockStat { height: h, at: f.time.min(now), txs: f.txs, weight: f.weight, feerate: f.feerate },
            ),
            Ok(None) => {}
            Err(e) => {
                warn!("block stats failed at {h}: {e:?}");
                break;
            }
        }
    }
}

async fn header_time(rpc: &BtcRpc, height: u32) -> anyhow::Result<Option<u64>> {
    let hash = rpc.call("getblockhash", json!([height])).await?;
    let Some(hash) = hash.as_str() else {
        return Ok(None);
    };
    let header = rpc.call("getblockheader", json!([hash])).await?;
    Ok(header["time"].as_u64())
}

struct BlockFacts {
    txs: u64,
    weight: u64,
    feerate: f64,
    time: u64,
}

async fn block_stats(rpc: &BtcRpc, height: u32) -> anyhow::Result<Option<BlockFacts>> {
    let stats = rpc
        .call("getblockstats", json!([height, ["txs", "total_weight", "totalfee", "time"]]))
        .await?;
    let txs = stats["txs"].as_u64();
    let weight = stats["total_weight"].as_u64().filter(|w| *w > 0);
    let total_fee = stats["totalfee"].as_f64();
    let time = stats["time"].as_u64();
    Ok(match (txs, weight, total_fee, time) {
        (Some(txs), Some(weight), Some(fee), Some(time)) => Some(BlockFacts {
            txs,
            weight,
            feerate: fee / (weight as f64 / 4.0),
            time,
        }),
        _ => None,
    })
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn mempool(items: &[(f64, u64)]) -> Value {
        let mut o = serde_json::Map::new();
        for (i, (rate, vsize)) in items.iter().enumerate() {
            o.insert(
                format!("tx{i}"),
                json!({ "vsize_adjusted": vsize, "fees": { "base": rate * *vsize as f64 / 1e8 } }),
            );
        }
        Value::Object(o)
    }

    #[test]
    fn the_adjusted_size_is_read_first_and_the_old_key_still_counts() {
        let mut o = serde_json::Map::new();
        o.insert("core32".into(), json!({ "vsize_adjusted": 1_000, "vsize": 500, "fees": { "base": 0.00001 } }));
        o.insert("core31".into(), json!({ "vsize": 400, "fees": { "base": 0.00002 } }));
        let rates = sorted_rates(&Value::Object(o));
        assert_eq!(rates.len(), 2);
        assert_eq!((rates[0].0.as_str(), rates[0].2), ("core31", 400), "the old key alone is still a size");
        assert!((rates[0].1 - 5.0).abs() < 1e-9);
        assert_eq!((rates[1].0.as_str(), rates[1].2), ("core32", 1_000), "vsize_adjusted wins over vsize");
        assert!((rates[1].1 - 1.0).abs() < 1e-9);
    }

    fn tiers(mp: &Value, floor: f64) -> [f64; 4] {
        tiers_from(&sorted_rates(mp), floor)
    }

    #[test]
    fn tiers_are_read_off_the_right_depths() {
        let items: Vec<(f64, u64)> = (0..7)
            .map(|i| (10.0 - i as f64, 1_000_000u64))
            .collect();
        let t = tiers(&mempool(&items), 0.1);
        assert_eq!(t[3], 10.0, "high (next block)");
        assert_eq!(t[2], 8.0, "med");
        assert_eq!(t[1], 5.0, "low");
        assert_eq!(t[0], 0.1, "floor is the relay minimum, not a walked depth");
        assert!(t[0] <= t[1] && t[1] <= t[2] && t[2] <= t[3]);
    }

    #[test]
    fn shallow_mempool_collapses_to_the_floor() {
        let t = tiers(&mempool(&[(5.0, 400_000)]), 0.1);
        assert_eq!(t, [0.1, 0.1, 0.1, 0.1]);

        let items: Vec<(f64, u64)> = vec![(5.0, 1_000_000), (2.0, 1_000_000)];
        let t = tiers(&mempool(&items), 0.1);
        assert_eq!(t[3], 5.0);
        assert_eq!(t[2], 0.1);
        assert_eq!(t[1], 0.1);
    }

    #[test]
    fn empty_mempool_is_all_floor() {
        assert_eq!(tiers(&mempool(&[]), 0.25), [0.25; 4]);
    }

    #[test]
    fn every_tier_is_clamped_up_to_the_floor() {
        let items: Vec<(f64, u64)> = (0..7).map(|_| (0.001, 1_000_000u64)).collect();
        let t = tiers(&mempool(&items), 0.5);
        assert_eq!(t, [0.5; 4]);
    }

    #[test]
    fn malformed_entries_are_skipped_not_fatal() {
        let mut o = serde_json::Map::new();
        o.insert("good".into(), json!({ "vsize_adjusted": 1_000_000, "fees": { "base": 0.05 } }));
        o.insert("no_vsize".into(), json!({ "fees": { "base": 0.01 } }));
        o.insert("zero_vsize".into(), json!({ "vsize": 0, "fees": { "base": 0.01 } }));
        o.insert("no_fees".into(), json!({ "vsize": 500 }));
        let t = tiers(&Value::Object(o), 0.1);
        assert_eq!(t[3], 5.0);
        assert_eq!(t[2], 0.1);
    }

    #[test]
    fn absent_fields_serialise_as_null_not_omitted() {
        let f = NodeStats::default().frame();
        let v: Value = serde_json::from_str(&f).unwrap();
        assert_eq!(v["type"], "node_stats");
        for k in [
            "sync", "peers", "tip_height", "tip_at", "avg_interval", "tiers", "fee_scale_top",
            "mempool_txs", "next_block_txs", "pending_vsize", "node_height", "headers", "behind_secs",
            "tip_time", "epoch_start_time",
        ] {
            assert!(v.get(k).is_some(), "{k} must be present");
            assert!(v[k].is_null(), "{k} must be null when unmeasured");
        }
        assert_eq!(v["blocks"], json!([]));
        assert_eq!(v["bands"], json!([]));
    }

    #[test]
    fn populated_frame_round_trips() {
        let s = NodeStats {
            sync: Some(1.0),
            peers: Some(10),
            tip_height: Some(961_917),
            tip_at: Some(1_760_000_000),
            avg_interval: Some(589.2),
            tiers: Some([0.1, 0.13, 0.343, 1.085]),
            fee_scale_top: Some(4.86),
            incremental_sat_vb: None,
            mempool_txs: Some(21_917),
            next_block_txs: Some(3_104),
            pending_vsize: Some(11_700_000),
            bands: vec![Band { from: 0.1, vsize: 9_000_000 }, Band { from: 0.12345678, vsize: 2_700_000 }],
            blocks: vec![BlockStat { height: 961_917, at: 1_760_000_000, txs: 4_377, weight: 3_995_481, feerate: 1.77 }],
            node_height: Some(961_917),
            headers: Some(961_918),
            behind_secs: Some(31),
            tip_time: Some(1_759_999_900),
            epoch_start_time: Some(1_759_000_000),
        };
        let v: Value = serde_json::from_str(&s.frame()).unwrap();
        assert_eq!(v["peers"], 10);
        assert_eq!(v["tip_height"], 961_917);
        assert_eq!(v["tiers"][3].as_f64().unwrap(), 1.085);
        assert_eq!(v["tiers"].as_array().unwrap().len(), 4);
        assert_eq!(v["mempool_txs"], 21_917);
        assert_eq!(v["next_block_txs"], 3_104);
        assert_eq!(v["blocks"][0]["height"], 961_917);
        assert_eq!(v["blocks"][0]["weight"], 3_995_481);
        assert_eq!(v["blocks"][0]["feerate"].as_f64().unwrap(), 1.77);
        assert_eq!(v["node_height"], 961_917);
        assert_eq!(v["headers"], 961_918);
        assert_eq!(v["behind_secs"], 31);
        assert_eq!(v["pending_vsize"], 11_700_000);
        assert_eq!(v["bands"][0]["from"].as_f64().unwrap(), 0.1);
        assert_eq!(v["bands"][1]["from"].as_f64().unwrap(), 0.123, "edges round to a thousandth");
        assert_eq!(v["bands"][1]["vsize"], 2_700_000);
        assert_eq!(v["tip_time"], 1_759_999_900);
        assert_eq!(v["epoch_start_time"], 1_759_000_000);
    }

    #[test]
    fn the_histogram_spreads_a_sub_sat_mempool_across_bands() {
        let mut items: Vec<(f64, u64)> = Vec::new();
        for i in 0..25 {
            items.push((0.1 + i as f64 * 0.01, 400_000));
        }
        items.push((1.0, 40_000));
        items.push((3.0, 60_000));
        items.push((150.0, 200));
        let rates = sorted_rates(&mempool(&items));
        let (bands, pending) = histogram(&rates, 0.1);
        assert_eq!(bands.len(), BANDS);
        assert_eq!(pending, 10_000_000 + 40_000 + 60_000 + 200);
        assert_eq!(bands.iter().map(|b| b.vsize).sum::<u64>(), pending, "every byte in exactly one band");
        assert_eq!(bands[0].from, 0.1, "the left edge is the floor");
        assert!(bands.windows(2).all(|w| w[0].from < w[1].from), "edges rise");
        assert!((bands[BANDS - 1].from - 3.0).abs() < 1e-9, "{}", bands[BANDS - 1].from);
        let occupied = bands.iter().filter(|b| b.vsize > 0).count();
        assert!(occupied >= 6, "the cheap mass spreads: {occupied} bands occupied");
        assert!(bands[0].vsize < pending / 2, "the first band does not hold everything");
    }

    #[test]
    fn a_collapsed_or_empty_mempool_still_draws_honestly() {
        let (bands, pending) = histogram(&sorted_rates(&mempool(&[(0.1, 400_000)])), 0.1);
        assert_eq!(pending, 400_000);
        assert_eq!(bands[0].vsize, 400_000, "everything sits at the floor");
        assert!((bands[BANDS - 1].from - 0.2).abs() < 1e-9, "a nominal one-octave span");

        assert_eq!(histogram(&[], 0.1), (Vec::new(), 0));

        let (bands, _) = histogram(&sorted_rates(&mempool(&[(0.05, 1_000), (5.0, 60_000)])), 0.5);
        assert_eq!(bands[0].vsize, 1_000, "under the floor is the floor's band");
    }

    #[test]
    fn behind_is_timed_from_the_first_reading_and_cleared_on_agreement() {
        use std::time::{Duration, Instant};
        let t0 = Instant::now();
        let mut since = None;
        assert_eq!(behind_for(Some(100), Some(100), &mut since, t0), Some(0));
        assert!(since.is_none());
        assert_eq!(behind_for(Some(101), Some(100), &mut since, t0), Some(0));
        assert_eq!(since, Some(t0));
        assert_eq!(behind_for(Some(101), Some(100), &mut since, t0 + Duration::from_secs(45)), Some(45));
        assert_eq!(behind_for(Some(102), Some(100), &mut since, t0 + Duration::from_secs(90)), Some(90));
        assert_eq!(behind_for(Some(102), Some(102), &mut since, t0 + Duration::from_secs(91)), Some(0));
        assert!(since.is_none());
        since = Some(t0);
        assert_eq!(behind_for(None, Some(102), &mut since, t0), None);
        assert_eq!(behind_for(Some(103), None, &mut since, t0), None);
        assert_eq!(since, Some(t0));
    }

    #[test]
    fn the_next_block_count_fills_one_block_by_vsize() {
        let items: Vec<(f64, u64)> = vec![(10.0, 300_000), (9.0, 300_000), (8.0, 300_000), (7.0, 300_000)];
        assert_eq!(fits_in(&sorted_rates(&mempool(&items)), NEXT_BLOCK_VSIZE), 3);
        assert_eq!(fits_in(&sorted_rates(&mempool(&[(5.0, 1_200_000)])), NEXT_BLOCK_VSIZE), 0);
        assert_eq!(fits_in(&sorted_rates(&mempool(&[(5.0, 400_000)])), NEXT_BLOCK_VSIZE), 1);
        assert_eq!(fits_in(&[], NEXT_BLOCK_VSIZE), 0);
    }

    fn block(height: u32) -> BlockStat {
        BlockStat { height, at: 1_000 + height as u64, txs: 1, weight: 4_000_000, feerate: 1.0 }
    }

    fn heights(blocks: &[BlockStat]) -> Vec<u32> {
        blocks.iter().map(|b| b.height).collect()
    }

    #[test]
    fn the_train_is_newest_first_one_per_height_and_capped() {
        let mut blocks = Vec::new();
        remember(&mut blocks, block(100));
        remember(&mut blocks, block(99));
        remember(&mut blocks, block(98));
        assert_eq!(heights(&blocks), vec![100, 99, 98]);
        let mut again = block(99);
        again.txs = 7;
        remember(&mut blocks, again);
        assert_eq!(heights(&blocks), vec![100, 99, 98]);
        assert_eq!(blocks[1].txs, 7);
        for h in 101..=120 {
            remember(&mut blocks, block(h));
        }
        assert_eq!(blocks.len(), TRAIN);
        assert_eq!(heights(&blocks), (108..=120).rev().collect::<Vec<u32>>());
    }

    #[test]
    fn a_lower_tip_drops_its_orphans() {
        let mut blocks: Vec<BlockStat> = (95..=100).rev().map(block).collect();
        let new_tip = 99;
        blocks.retain(|b| b.height < new_tip);
        remember(&mut blocks, block(new_tip));
        assert_eq!(heights(&blocks), vec![99, 98, 97, 96, 95]);
    }
}

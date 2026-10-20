
use anyhow::{Context, Result};
use serde_json::json;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, error, warn};

use super::{BtcIndex, IndexState};
use crate::rpc::BtcRpc;

const AUDIT_INTERVAL: Duration = Duration::from_secs(15 * 60);
const SCAN_TIMEOUT: Duration = Duration::from_secs(180);

pub fn spawn(idx: BtcIndex, rpc: BtcRpc, state_rx: watch::Receiver<IndexState>) {
    tokio::spawn(run(idx, rpc, state_rx));
}

async fn run(idx: BtcIndex, rpc: BtcRpc, state_rx: watch::Receiver<IndexState>) {
    let mut tick = tokio::time::interval(AUDIT_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    loop {
        tick.tick().await;
        if *state_rx.borrow() != IndexState::Live {
            debug!("audit: index not Live, skipping tick");
            continue;
        }
        if let Err(e) = audit_one(&idx, &rpc).await {
            warn!("audit tick failed (not a mismatch): {:?}", e);
        }
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

async fn audit_one(idx: &BtcIndex, rpc: &BtcRpc) -> Result<()> {
    let seed = splitmix64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
    );
    let Some(spk) = idx.sample_spk(seed)? else {
        return Ok(());
    };

    let (tip_before, _) = idx.tip()?.context("no tip")?;
    let spk_hex: String = spk.iter().map(|b| format!("{:02x}", b)).collect();
    let scan = rpc
        .call_with_timeout(
            "scantxoutset",
            json!(["start", [format!("raw({})", spk_hex)]]),
            SCAN_TIMEOUT,
        )
        .await?;
    let scan_height = scan["height"].as_u64().context("scan: no height")? as u32;
    let (tip_after, _) = idx.tip()?.context("no tip")?;

    if tip_before != tip_after || scan_height != tip_after {
        debug!(
            "audit: heights moved (index {}→{}, scan {}), skipping round",
            tip_before, tip_after, scan_height
        );
        return Ok(());
    }

    let expected = idx.query_spk(&spk)?;
    let got_sats = (scan["total_amount"].as_f64().unwrap_or(0.0) * 100_000_000.0).round() as u64;
    let got_count = scan["unspents"].as_array().map(|u| u.len()).unwrap_or(0);

    if expected.sats != got_sats || expected.utxo_count != got_count {
        error!(
            "AUDIT MISMATCH at height {} for spk {}: index says {} sats/{} utxos, chainstate says {} sats/{} utxos — rebuild the index from a fresh dump",
            scan_height, spk_hex, expected.sats, expected.utxo_count, got_sats, got_count
        );
    } else {
        debug!(
            "audit ok at height {}: spk {}… {} sats/{} utxos match chainstate",
            scan_height,
            &spk_hex[..spk_hex.len().min(16)],
            got_sats,
            got_count
        );
    }
    Ok(())
}

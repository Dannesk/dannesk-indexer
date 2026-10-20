
use anyhow::{Context, Result};
use dannesk_btc_codec::tx::Transaction;
use dashmap::DashMap;
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::rpc::BtcRpc;

const TXID_CAP: usize = 300_000;
const WARMUP_LIST_TIMEOUT: Duration = Duration::from_secs(60);
const RETRY_DELAY: Duration = Duration::from_secs(5);
const QUIET_RETRIES: Duration = Duration::from_secs(600);

#[derive(Default)]
pub struct MempoolIndex {
    by_spk: DashMap<Vec<u8>, HashSet<String>>,
    by_txid: DashMap<String, Vec<Vec<u8>>>,
    warm: AtomicBool,
}

impl MempoolIndex {
    pub fn insert(&self, txid: &str, spks: Vec<Vec<u8>>) {
        if spks.is_empty() || self.by_txid.contains_key(txid) {
            return;
        }
        if self.by_txid.len() >= TXID_CAP {
            warn!(
                "mempool spk index hit {} txids — clearing (eviction leak?); \
                 receive detection degrades until it repopulates",
                TXID_CAP
            );
            self.by_txid.clear();
            self.by_spk.clear();
        }
        for spk in &spks {
            self.by_spk
                .entry(spk.clone())
                .or_default()
                .insert(txid.to_string());
        }
        self.by_txid.insert(txid.to_string(), spks);
    }

    pub fn evict(&self, txid: &str) -> Vec<Vec<u8>> {
        let Some((_, spks)) = self.by_txid.remove(txid) else {
            return Vec::new();
        };
        for spk in &spks {
            let emptied = match self.by_spk.get_mut(spk) {
                Some(mut set) => {
                    set.remove(txid);
                    set.is_empty()
                }
                None => false,
            };
            if emptied {
                self.by_spk.remove_if(spk, |_, set| set.is_empty());
            }
        }
        spks
    }

    pub fn txids_for_spk(&self, spk: &[u8]) -> Vec<String> {
        self.by_spk
            .get(spk)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn is_warm(&self) -> bool {
        self.warm.load(Ordering::Relaxed)
    }

    fn set_warm(&self) {
        self.warm.store(true, Ordering::Relaxed);
    }

    fn contains(&self, txid: &str) -> bool {
        self.by_txid.contains_key(txid)
    }
}

pub fn output_spks(tx: &Transaction) -> Vec<Vec<u8>> {
    tx.output
        .iter()
        .filter_map(|out| {
            let bytes = out.script_pubkey.as_slice();
            if bytes.first() == Some(&0x6a) || bytes.len() > 10_000 {
                None
            } else {
                Some(bytes.to_vec())
            }
        })
        .collect()
}

pub fn spawn_warmup(index: Arc<MempoolIndex>, rpc: BtcRpc) {
    tokio::spawn(async move {
        let mut quiet_until: Option<tokio::time::Instant> = None;
        loop {
            match warm(&index, &rpc).await {
                Ok(n) => {
                    index.set_warm();
                    info!("mempool spk index warm ({} transactions indexed)", n);
                    return;
                }
                Err(e) => {
                    let now = tokio::time::Instant::now();
                    if quiet_until.is_none_or(|t| now >= t) {
                        warn!(
                            "mempool index warmup failed (retrying every {:?}, repeats at debug for 10 min): {:?}",
                            RETRY_DELAY, e
                        );
                        quiet_until = Some(now + QUIET_RETRIES);
                    } else {
                        debug!("mempool index warmup failed, retrying: {:?}", e);
                    }
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    });
}

async fn warm(index: &MempoolIndex, rpc: &BtcRpc) -> Result<usize> {
    let txids = rpc
        .call_with_timeout("getrawmempool", json!([]), WARMUP_LIST_TIMEOUT)
        .await?;
    let txids = txids.as_array().context("getrawmempool: not an array")?;
    let mut indexed = 0usize;
    for entry in txids {
        let Some(txid) = entry.as_str() else { continue };
        if index.contains(txid) {
            indexed += 1;
            continue;
        }
        let Ok(hex) = rpc.call("getrawtransaction", json!([txid])).await else {
            continue;
        };
        let Some(tx) = hex
            .as_str()
            .and_then(|h| Transaction::from_hex(h).ok())
        else {
            continue;
        };
        index.insert(txid, output_spks(&tx));
        indexed += 1;
    }
    Ok(indexed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spk(b: u8) -> Vec<u8> {
        vec![0x00, 0x14, b]
    }

    #[test]
    fn insert_lookup_evict_roundtrip() {
        let idx = MempoolIndex::default();
        idx.insert("t1", vec![spk(1), spk(2)]);
        idx.insert("t2", vec![spk(2)]);
        assert_eq!(idx.txids_for_spk(&spk(1)), vec!["t1".to_string()]);
        let mut both = idx.txids_for_spk(&spk(2));
        both.sort();
        assert_eq!(both, vec!["t1".to_string(), "t2".to_string()]);

        idx.evict("t1");
        assert!(idx.txids_for_spk(&spk(1)).is_empty());
        assert_eq!(idx.txids_for_spk(&spk(2)), vec!["t2".to_string()]);
        assert!(idx.by_spk.get(&spk(1)).is_none());

        idx.evict("t1");
        idx.evict("never-seen");
        assert_eq!(idx.by_txid.len(), 1);
    }

    #[test]
    fn reinsert_is_idempotent() {
        let idx = MempoolIndex::default();
        idx.insert("t1", vec![spk(1)]);
        idx.insert("t1", vec![spk(1), spk(9)]);
        assert_eq!(idx.by_txid.len(), 1);
        assert!(idx.txids_for_spk(&spk(9)).is_empty(), "second insert must not win");
    }

    #[test]
    fn empty_output_sets_are_never_stored() {
        let idx = MempoolIndex::default();
        idx.insert("t1", vec![]);
        assert!(idx.by_txid.is_empty());
    }
}

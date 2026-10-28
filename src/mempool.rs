
use anyhow::{Context, Result};
use dannesk_btc_codec::tx::{OutPoint, Transaction};
use dashmap::DashMap;
use futures_util::{StreamExt, stream};
use serde_json::json;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use tracing::{debug, info, warn};

use crate::rpc::{BtcRpc, NodeError};

const TXID_CAP: usize = 1_000_000;
const LIST_TIMEOUT: Duration = Duration::from_secs(60);
const RETRY_DELAY: Duration = Duration::from_secs(5);
const QUIET_RETRIES: Duration = Duration::from_secs(600);
const RECONCILE_EVERY: Duration = Duration::from_secs(600);
const WALK_MIN_GAP: Duration = Duration::from_secs(30);
const FETCH_CONCURRENCY: usize = 4;
const RPC_NOT_IN_MEMPOOL: i64 = -5;

/// An outpoint as 36 bytes: the txid in the node's internal byte order.
pub type OutpointKey = [u8; 36];

pub struct MempoolIndex {
    by_spk: DashMap<Vec<u8>, HashSet<String>>,
    by_prevout: DashMap<OutpointKey, Arc<str>>,
    by_txid: DashMap<String, Entry>,
    held: AtomicUsize,
    generation: AtomicU64,
    warm: tokio::sync::watch::Sender<bool>,
    walk: Notify,
    dropped: OnceLock<mpsc::UnboundedSender<String>>,
}

struct Entry {
    spks: Vec<Vec<u8>>,
    prevouts: Vec<OutpointKey>,
    /// When the node last vouched for it; a reconciliation judges what is older than its listing.
    generation: u64,
}

impl Default for MempoolIndex {
    fn default() -> Self {
        Self {
            by_spk: DashMap::new(),
            by_prevout: DashMap::new(),
            by_txid: DashMap::new(),
            held: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            warm: tokio::sync::watch::channel(false).0,
            walk: Notify::new(),
            dropped: OnceLock::new(),
        }
    }
}

impl MempoolIndex {
    /// The node just accepted this exact transaction, so its claims on its inputs are current.
    pub fn insert_live(&self, txid: &str, spks: Vec<Vec<u8>>, prevouts: Vec<OutpointKey>) {
        if let Some(mut entry) = self.by_txid.get_mut(txid) {
            entry.generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
            let spender: Arc<str> = Arc::from(txid);
            for key in &prevouts {
                self.by_prevout.insert(*key, Arc::clone(&spender));
            }
            return;
        }
        self.store(txid, spks, prevouts, true);
    }

    /// From a listing of the mempool: older than anything the follower saw since, so it never displaces a claim.
    pub fn insert_listed(&self, txid: &str, spks: Vec<Vec<u8>>, prevouts: Vec<OutpointKey>) {
        if self.by_txid.contains_key(txid) {
            return;
        }
        self.store(txid, spks, prevouts, false);
    }

    fn store(&self, txid: &str, spks: Vec<Vec<u8>>, prevouts: Vec<OutpointKey>, claim: bool) {
        if spks.is_empty() && prevouts.is_empty() {
            return;
        }
        for spk in &spks {
            self.by_spk
                .entry(spk.clone())
                .or_default()
                .insert(txid.to_string());
        }
        let spender: Arc<str> = Arc::from(txid);
        for key in &prevouts {
            if claim {
                self.by_prevout.insert(*key, Arc::clone(&spender));
            } else {
                self.by_prevout.entry(*key).or_insert_with(|| Arc::clone(&spender));
            }
        }
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let entry = Entry { spks, prevouts, generation };
        if self.by_txid.insert(txid.to_string(), entry).is_none()
            && self.held.fetch_add(1, Ordering::Relaxed) + 1 == TXID_CAP
        {
            self.walk.notify_one();
        }
    }

    pub fn evict(&self, txid: &str) -> Vec<Vec<u8>> {
        let Some((_, entry)) = self.by_txid.remove(txid) else {
            return Vec::new();
        };
        self.held.fetch_sub(1, Ordering::Relaxed);
        for spk in &entry.spks {
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
        for key in &entry.prevouts {
            self.by_prevout.remove_if(key, |_, spender| &**spender == txid);
        }
        entry.spks
    }

    /// What the node vouched for no later than `generation` and a listing no longer names.
    fn gone_since(&self, generation: u64, listed: &HashSet<String>) -> Vec<String> {
        self.by_txid
            .iter()
            .filter(|e| e.value().generation <= generation && !listed.contains(e.key()))
            .map(|e| e.key().clone())
            .collect()
    }

    /// A vanished transaction goes to the watch side's drop queue, which undoes it as it would a ZMQ drop; without which it is simply evicted.
    fn drop_or_evict(&self, txid: String) {
        match self.dropped.get() {
            Some(queue) if queue.send(txid.clone()).is_ok() => {}
            _ => {
                self.evict(&txid);
            }
        }
    }

    /// Hands out the drop queue once and then the watch side drains it.
    pub fn drop_queue(&self) -> mpsc::UnboundedReceiver<String> {
        let (tx, rx) = mpsc::unbounded_channel();
        if self.dropped.set(tx).is_err() {
            warn!("mempool mirror drop queue requested twice; the first one stays");
        }
        rx
    }

    pub fn txids_for_spk(&self, spk: &[u8]) -> Vec<String> {
        self.by_spk
            .get(spk)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// The mempool transaction spending `outpoint` ("txid:vout", as the tracker keys it), if any.
    pub fn spender_of(&self, outpoint: &str) -> Option<String> {
        let key = outpoint_key(outpoint)?;
        self.by_prevout.get(&key).map(|e| e.value().to_string())
    }

    /// The transaction's parents that are themselves in the mempool, as displayed txids.
    pub fn parents_in_mempool(&self, txid: &str) -> Vec<String> {
        let candidates: Vec<String> = match self.by_txid.get(txid) {
            Some(entry) => entry.prevouts.iter().map(|key| display_txid(&key[..32])).collect(),
            None => return Vec::new(),
        };
        candidates
            .into_iter()
            .filter(|parent| self.by_txid.contains_key(parent))
            .collect()
    }

    pub fn is_warm(&self) -> bool {
        *self.warm.borrow()
    }

    pub async fn warmed(&self) {
        if self.is_warm() {
            return;
        }
        let _ = self.warm.subscribe().wait_for(|warm| *warm).await;
    }

    pub(crate) fn set_warm(&self) {
        self.warm.send_replace(true);
    }

    /// Asks the walker for a reconciliation; requests coalesce.
    pub fn request_walk(&self) {
        self.walk.notify_one();
    }

    fn contains(&self, txid: &str) -> bool {
        self.by_txid.contains_key(txid)
    }

    fn len(&self) -> usize {
        self.held.load(Ordering::Relaxed)
    }
}

/// The tracker's txid:vout as an index key.
pub fn outpoint_key(outpoint: &str) -> Option<OutpointKey> {
    let (txid, vout) = outpoint.split_once(':')?;
    let vout: u32 = vout.parse().ok()?;
    let txid = txid.as_bytes();
    if txid.len() != 64 {
        return None;
    }
    let mut key = [0u8; 36];
    for (i, pair) in txid.chunks(2).enumerate() {
        let byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
        key[31 - i] = byte;
    }
    key[32..].copy_from_slice(&vout.to_le_bytes());
    Some(key)
}

pub fn prevout_key(op: &OutPoint) -> OutpointKey {
    let mut key = [0u8; 36];
    key[..32].copy_from_slice(&op.txid.to_bytes());
    key[32..].copy_from_slice(&op.vout.to_le_bytes());
    key
}

/// A txid in the node's internal byte order, as it is displayed.
fn display_txid(internal: &[u8]) -> String {
    internal.iter().rev().map(|b| format!("{:02x}", b)).collect()
}

pub fn input_prevouts(tx: &Transaction) -> Vec<OutpointKey> {
    tx.input.iter().map(|i| prevout_key(&i.previous_output)).collect()
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

pub(crate) struct Walk {
    pub added: usize,
    pub evicted: usize,
    /// Listed transactions the node would not hand over: because the mirror has holes until the next walk.
    pub failed: usize,
    pub total: usize,
}

/// Keeps the mirror equal to the node's mempool.
pub fn spawn_walker(index: Arc<MempoolIndex>, rpc: BtcRpc) {
    tokio::spawn(async move {
        let mut quiet_until: Option<tokio::time::Instant> = None;
        let mut last_walk: Option<tokio::time::Instant> = None;
        loop {
            if let Some(t) = last_walk {
                tokio::select! {
                    _ = index.walk.notified() => {}
                    _ = tokio::time::sleep(RECONCILE_EVERY) => {}
                }
                let since = t.elapsed();
                if since < WALK_MIN_GAP {
                    tokio::time::sleep(WALK_MIN_GAP - since).await;
                }
            }
            loop {
                match reconcile(&index, &rpc).await {
                    Ok(walk) => {
                        if walk.failed > 0 {
                            debug!("mempool mirror: {} listed transactions not fetched, walking again", walk.failed);
                            index.request_walk();
                        } else if !index.is_warm() {
                            index.set_warm();
                            info!("mempool mirror warm ({} transactions)", walk.total);
                        }
                        if walk.added + walk.evicted > 0 {
                            debug!(
                                "mempool mirror reconciled: {} added, {} evicted, {} held",
                                walk.added, walk.evicted, walk.total
                            );
                        }
                        if walk.total >= TXID_CAP {
                            info!(
                                "mempool mirror holds {} transactions, past its {} mark: the node's mempool is that large",
                                walk.total, TXID_CAP
                            );
                        }
                        break;
                    }
                    Err(e) => {
                        let now = tokio::time::Instant::now();
                        if quiet_until.is_none_or(|t| now >= t) {
                            warn!(
                                "mempool mirror reconciliation failed (retrying every {:?}, repeats at debug for 10 min): {:?}",
                                RETRY_DELAY, e
                            );
                            quiet_until = Some(now + QUIET_RETRIES);
                        } else {
                            debug!("mempool mirror reconciliation failed, retrying: {:?}", e);
                        }
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                }
            }
            last_walk = Some(tokio::time::Instant::now());
        }
    });
}

fn gone_from_mempool(e: &anyhow::Error) -> bool {
    e.downcast_ref::<NodeError>()
        .is_some_and(|NodeError(err)| err["code"].as_i64() == Some(RPC_NOT_IN_MEMPOOL))
}

pub(crate) async fn reconcile(index: &MempoolIndex, rpc: &BtcRpc) -> Result<Walk> {
    let generation = index.generation.load(Ordering::Relaxed);
    let listed = rpc
        .call_with_timeout("getrawmempool", json!([]), LIST_TIMEOUT)
        .await?;
    let listed: HashSet<String> = listed
        .as_array()
        .context("getrawmempool: not an array")?
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    let gone = index.gone_since(generation, &listed);
    let evicted = gone.len();
    for txid in gone {
        index.drop_or_evict(txid);
    }
    let missing: Vec<String> = listed.into_iter().filter(|t| !index.contains(t)).collect();
    let (mut added, mut failed) = (0usize, 0usize);
    let mut fetched = stream::iter(missing)
        .map(|txid| async move {
            let hex = rpc.call("getrawtransaction", json!([&txid])).await;
            (txid, hex)
        })
        .buffer_unordered(FETCH_CONCURRENCY);
    while let Some((txid, hex)) = fetched.next().await {
        let hex = match hex {
            Ok(hex) => hex,
            Err(e) if gone_from_mempool(&e) => continue,
            Err(e) => {
                debug!("mempool mirror: {} not fetched: {:?}", txid, e);
                failed += 1;
                continue;
            }
        };
        let Some(tx) = hex
            .as_str()
            .and_then(|h| Transaction::from_hex(h).ok())
        else {
            debug!("mempool mirror: {} not decodable, skipped", txid);
            continue;
        };
        index.insert_listed(&txid, output_spks(&tx), input_prevouts(&tx));
        added += 1;
    }
    Ok(Walk { added, evicted, failed, total: index.len() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dannesk_btc_codec::tx::Txid;

    fn spk(b: u8) -> Vec<u8> {
        vec![0x00, 0x14, b]
    }

    fn op(b: u8, vout: u32) -> OutpointKey {
        let mut key = [b; 36];
        key[32..].copy_from_slice(&vout.to_le_bytes());
        key
    }

    fn name(key: &OutpointKey) -> String {
        format!("{}:{}", display_txid(&key[..32]), u32::from_le_bytes(key[32..].try_into().unwrap()))
    }

    #[test]
    fn insert_lookup_evict_roundtrip() {
        let idx = MempoolIndex::default();
        idx.insert_live("t1", vec![spk(1), spk(2)], vec![]);
        idx.insert_live("t2", vec![spk(2)], vec![]);
        assert_eq!(idx.txids_for_spk(&spk(1)), vec!["t1".to_string()]);
        let mut both = idx.txids_for_spk(&spk(2));
        both.sort();
        assert_eq!(both, vec!["t1".to_string(), "t2".to_string()]);
        assert_eq!(idx.len(), 2);

        idx.evict("t1");
        assert!(idx.txids_for_spk(&spk(1)).is_empty());
        assert_eq!(idx.txids_for_spk(&spk(2)), vec!["t2".to_string()]);
        assert!(idx.by_spk.get(&spk(1)).is_none());

        idx.evict("t1");
        idx.evict("never-seen");
        assert_eq!(idx.by_txid.len(), 1);
        assert_eq!(idx.len(), 1, "the count follows evictions, repeated ones included");
    }

    #[test]
    fn reinsert_is_idempotent() {
        let idx = MempoolIndex::default();
        idx.insert_live("t1", vec![spk(1)], vec![]);
        idx.insert_live("t1", vec![spk(1), spk(9)], vec![]);
        assert_eq!(idx.by_txid.len(), 1);
        assert_eq!(idx.len(), 1);
        assert!(idx.txids_for_spk(&spk(9)).is_empty(), "second insert must not win");
    }

    #[test]
    fn empty_transactions_are_never_stored() {
        let idx = MempoolIndex::default();
        idx.insert_live("t1", vec![], vec![]);
        idx.insert_listed("t2", vec![], vec![]);
        assert!(idx.by_txid.is_empty());
    }

    #[test]
    fn inputs_are_indexed_and_evicted_with_their_spender() {
        let idx = MempoolIndex::default();
        let (a0, a1) = (op(0xaa, 0), op(0xaa, 1));
        idx.insert_live("t1", vec![spk(1)], vec![a0, a1]);
        assert_eq!(idx.spender_of(&name(&a0)).as_deref(), Some("t1"));
        idx.insert_live("t2", vec![], vec![a0]);
        assert_eq!(idx.spender_of(&name(&a0)).as_deref(), Some("t2"), "the later live insert wins the outpoint");
        idx.evict("t1");
        assert_eq!(idx.spender_of(&name(&a0)).as_deref(), Some("t2"), "t1's eviction must not drop t2's claim");
        assert!(idx.spender_of(&name(&a1)).is_none());
        assert!(idx.txids_for_spk(&spk(1)).is_empty());
        idx.evict("t2");
        assert!(idx.by_prevout.is_empty());
        assert!(idx.by_txid.is_empty());
    }

    #[test]
    fn a_re_accepted_spender_reclaims_its_input() {
        let idx = MempoolIndex::default();
        let coin = op(0xcc, 0);
        idx.insert_live("a", vec![], vec![coin]);
        idx.insert_live("b", vec![], vec![coin]);
        idx.evict("b");
        assert!(idx.spender_of(&name(&coin)).is_none());
        idx.insert_live("a", vec![], vec![coin]);
        assert_eq!(idx.spender_of(&name(&coin)).as_deref(), Some("a"), "a known txid seen live again claims its inputs");
    }

    #[test]
    fn a_listing_never_displaces_a_live_claim() {
        let idx = MempoolIndex::default();
        let coin = op(0xdd, 2);
        idx.insert_live("live", vec![], vec![coin]);
        idx.insert_listed("listed", vec![spk(3)], vec![coin]);
        assert_eq!(idx.spender_of(&name(&coin)).as_deref(), Some("live"));
        assert!(idx.contains("listed"), "the listed transaction is still held");
        idx.evict("live");
        idx.insert_listed("listed", vec![spk(3)], vec![coin]);
        assert!(idx.spender_of(&name(&coin)).is_none(), "a known listing is not re-applied");
        idx.evict("listed");
        idx.insert_listed("listed", vec![spk(3)], vec![coin]);
        assert_eq!(idx.spender_of(&name(&coin)).as_deref(), Some("listed"), "a free outpoint takes the listed claim");
    }

    #[test]
    fn a_reconciliation_judges_only_what_predates_its_listing() {
        let idx = MempoolIndex::default();
        idx.insert_live("gone", vec![spk(1)], vec![op(1, 0)]);
        idx.insert_live("kept", vec![spk(2)], vec![op(2, 0)]);
        idx.insert_live("vouched-again", vec![spk(4)], vec![op(4, 0)]);
        let generation = idx.generation.load(Ordering::Relaxed);
        idx.insert_live("fresh", vec![spk(3)], vec![op(3, 0)]);
        idx.insert_live("vouched-again", vec![spk(4)], vec![op(4, 0)]);
        let listed: HashSet<String> = ["kept".to_string(), "unknown".to_string()].into_iter().collect();
        assert_eq!(idx.gone_since(generation, &listed), vec!["gone".to_string()]);
        idx.drop_or_evict("gone".to_string());
        assert!(!idx.contains("gone"));
        assert!(idx.contains("kept"));
        assert!(idx.contains("fresh"), "what arrived after the listing is not judged by it");
        assert!(idx.contains("vouched-again"), "a live frame after the listing outranks the listing");
        assert!(idx.spender_of(&name(&op(1, 0))).is_none());
    }

    #[tokio::test]
    async fn a_drop_queue_takes_what_vanished_instead_of_a_plain_eviction() {
        let idx = MempoolIndex::default();
        let mut queue = idx.drop_queue();
        idx.insert_live("vanished", vec![spk(1)], vec![]);
        idx.drop_or_evict("vanished".to_string());
        assert_eq!(queue.recv().await.as_deref(), Some("vanished"));
        assert!(idx.contains("vanished"), "the watch side evicts, through its undo path");
        drop(queue);
        idx.drop_or_evict("vanished".to_string());
        assert!(!idx.contains("vanished"), "with nobody draining the queue, the mirror evicts itself");
    }

    #[test]
    fn parents_are_found_through_the_codec_byte_order() {
        let idx = MempoolIndex::default();
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = 0x40 + i as u8;
        }
        let parent = Txid::from_bytes(bytes);
        let parent_id = parent.to_string();
        idx.insert_live(&parent_id, vec![spk(1)], vec![op(9, 0)]);
        idx.insert_live("child", vec![spk(2)], vec![prevout_key(&OutPoint { txid: parent, vout: 0 }), op(8, 1)]);
        assert_eq!(idx.parents_in_mempool("child"), vec![parent_id.clone()]);
        assert!(idx.parents_in_mempool(&parent_id).is_empty(), "a parent whose own input is confirmed has none");
        idx.evict(&parent_id);
        assert!(idx.parents_in_mempool("child").is_empty());
        assert!(idx.parents_in_mempool("unknown").is_empty());
    }

    #[test]
    fn the_outpoint_key_matches_the_codec() {
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = i as u8;
        }
        let txid = Txid::from_bytes(bytes);
        let expected = prevout_key(&OutPoint { txid, vout: 258 });
        assert_eq!(outpoint_key(&format!("{}:258", txid)), Some(expected));
        assert_eq!(&expected[..32], &bytes, "the key holds the node's internal order");
        assert_eq!(display_txid(&expected[..32]), txid.to_string());
        assert!(outpoint_key("abc:1").is_none());
        assert!(outpoint_key(&format!("{}:x", txid)).is_none());
    }

    #[tokio::test]
    async fn warmed_returns_once_the_walker_says_so() {
        let idx = Arc::new(MempoolIndex::default());
        assert!(!idx.is_warm());
        let waiter = {
            let idx = Arc::clone(&idx);
            tokio::spawn(async move { idx.warmed().await })
        };
        idx.set_warm();
        tokio::time::timeout(Duration::from_secs(5), waiter).await.unwrap().unwrap();
        assert!(idx.is_warm());
        idx.warmed().await;
    }
}


use anyhow::{Context, Result, bail};
use dannesk_btc_codec::tx::{Block, BlockHash};
use futures_util::StreamExt;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde_json::{Value, json};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

use crate::rpc::BtcRpc;

pub const USED: TableDefinition<&[u8], ()> = TableDefinition::new("used");
pub const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
pub const META_CURSOR: &str = "cursor";
pub const META_LOADED: &str = "loaded";
pub const HASHES: TableDefinition<u32, &[u8]> = TableDefinition::new("hashes");
pub const HASH_DEPTH: u32 = 2000;

struct Template {
    name: &'static str,
    len: usize,
    hash_at: usize,
    matches: fn(&[u8]) -> bool,
}

const TEMPLATES: [Template; 4] = [
    Template { name: "p2wpkh", len: 22, hash_at: 2, matches: |s| s[0] == 0x00 && s[1] == 0x14 },
    Template { name: "p2tr", len: 34, hash_at: 2, matches: |s| s[0] == 0x51 && s[1] == 0x20 },
    Template {
        name: "p2pkh",
        len: 25,
        hash_at: 3,
        matches: |s| s[0] == 0x76 && s[1] == 0xa9 && s[2] == 0x14 && s[23] == 0x88 && s[24] == 0xac,
    },
    Template { name: "p2sh", len: 23, hash_at: 2, matches: |s| s[0] == 0xa9 && s[1] == 0x14 && s[22] == 0x87 },
];
const SHARDS_PER_TEMPLATE: usize = 64;
const SHARD_COUNT: usize = TEMPLATES.len() * SHARDS_PER_TEMPLATE;

fn template_of(spk: &[u8]) -> Option<usize> {
    TEMPLATES.iter().position(|t| t.len == spk.len() && (t.matches)(spk))
}

pub fn is_address_script(spk: &[u8]) -> bool {
    template_of(spk).is_some()
}

fn shard_of(spk: &[u8]) -> Option<usize> {
    let t = template_of(spk)?;
    Some(t * SHARDS_PER_TEMPLATE + (spk[TEMPLATES[t].hash_at] >> 2) as usize)
}

pub struct AppliedBlock {
    pub height: u32,
    pub hash: [u8; 32],
    pub prev: [u8; 32],
    pub scripts: Vec<Vec<u8>>,
}

pub fn parse_block(height: u32, hex: &str) -> Result<AppliedBlock> {
    let block = Block::from_hex(hex)
        .map_err(anyhow::Error::msg)
        .with_context(|| format!("block {height}: decode failed"))?;
    let mut scripts: Vec<Vec<u8>> = block
        .txdata
        .iter()
        .flat_map(|tx| tx.output.iter())
        .map(|out| out.script_pubkey.as_slice())
        .filter(|s| is_address_script(s))
        .map(<[u8]>::to_vec)
        .collect();
    scripts.sort_unstable();
    scripts.dedup();
    Ok(AppliedBlock {
        height,
        hash: block.block_hash().to_bytes(),
        prev: block.header.prev_blockhash,
        scripts,
    })
}

fn hash_bytes(display_hex: &str) -> Result<[u8; 32]> {
    Ok(BlockHash::from_str(display_hex).map_err(anyhow::Error::msg).context("bad block hash")?.to_bytes())
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

#[derive(Clone)]
pub struct UsedSet {
    db: Arc<Database>,
}

impl UsedSet {
    pub fn open(path: &str) -> Result<Self> {
        let db = Database::create(path).with_context(|| format!("Failed to open used-set store at {path}"))?;
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

    fn loaded(&self) -> Result<u32> {
        Ok(self
            .meta(META_LOADED)?
            .and_then(|b| b.as_slice().try_into().ok().map(u32::from_le_bytes))
            .unwrap_or(0))
    }

    pub fn contains_batch(&self, spks: &[Vec<u8>]) -> Result<Vec<bool>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(USED) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(vec![false; spks.len()]),
            Err(e) => return Err(e.into()),
        };
        spks.iter().map(|s| Ok(table.get(s.as_slice())?.is_some())).collect()
    }

    pub fn hash_at(&self, height: u32) -> Result<Option<[u8; 32]>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(HASHES) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(table.get(height)?.and_then(|g| g.value().try_into().ok()))
    }

    pub fn apply(&self, blocks: &[AppliedBlock]) -> Result<u64> {
        let Some(first) = blocks.first() else { return Ok(0) };
        let mut inserted = 0u64;
        let write = self.db.begin_write()?;
        {
            let mut meta = write.open_table(META)?;
            let mut used = write.open_table(USED)?;
            let mut hashes = write.open_table(HASHES)?;
            let mut cursor = meta.get(META_CURSOR)?.map(|g| g.value().to_vec()).as_deref().and_then(cursor_parse);
            match cursor {
                None if first.height != 0 => bail!("used set is empty and block {} is not genesis", first.height),
                _ => {}
            }
            for b in blocks {
                if let Some((h, hash)) = cursor {
                    if b.height <= h {
                        let known = hashes.get(b.height)?.map(|g| g.value().to_vec());
                        if known.as_deref() == Some(&b.hash[..]) {
                            continue;
                        }
                        bail!("used set already past block {} on a different chain (cursor {}) — rewind first", b.height, h);
                    }
                    if b.height != h + 1 || b.prev != hash {
                        bail!("used set out of order: block {} on prev {:02x?}.., cursor at {}", b.height, &b.prev[..4], h);
                    }
                }
                for s in &b.scripts {
                    if used.get(s.as_slice())?.is_none() {
                        used.insert(s.as_slice(), ())?;
                        inserted += 1;
                    }
                }
                hashes.insert(b.height, &b.hash[..])?;
                if let Some(old) = b.height.checked_sub(HASH_DEPTH + 1) {
                    hashes.remove(old)?;
                }
                cursor = Some((b.height, b.hash));
            }
            let (h, hash) = cursor.expect("at least one block");
            meta.insert(META_CURSOR, cursor_bytes(h, &hash).as_slice())?;
        }
        write.commit()?;
        Ok(inserted)
    }

    pub fn set_cursor(&self, height: u32, hash: &[u8; 32]) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut meta = write.open_table(META)?;
            meta.insert(META_CURSOR, cursor_bytes(height, hash).as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    fn load_shard(&self, idx: u32, records: &[&[u8]]) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut used = write.open_table(USED)?;
            for r in records {
                used.insert(*r, ())?;
            }
            let mut meta = write.open_table(META)?;
            meta.insert(META_LOADED, (idx + 1).to_le_bytes().as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    fn finish_load(&self, height: u32, hash: &[u8; 32], tail: &[(u32, [u8; 32])]) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut meta = write.open_table(META)?;
            meta.insert(META_CURSOR, cursor_bytes(height, hash).as_slice())?;
            let mut hashes = write.open_table(HASHES)?;
            for (h, hh) in tail {
                hashes.insert(*h, &hh[..])?;
            }
            write.open_table(USED)?;
        }
        write.commit()?;
        Ok(())
    }
}

const GETBLOCK_TIMEOUT: Duration = Duration::from_secs(60);

async fn fetch_block(rpc: &BtcRpc, height: u32) -> Result<AppliedBlock> {
    let hash = rpc.call("getblockhash", json!([height])).await?;
    let hash = hash.as_str().context("getblockhash: not a string")?.to_string();
    let hex = rpc
        .call_with_timeout("getblock", json!([hash, 0]), GETBLOCK_TIMEOUT)
        .await
        .map_err(|e| anyhow::anyhow!("getblock {height} failed — if pruned away, the used set needs a full resync: {e:?}"))?;
    let hex = hex.as_str().context("getblock: not a string")?.to_string();
    tokio::task::spawn_blocking(move || parse_block(height, &hex)).await?
}

pub async fn catch_up(used: &UsedSet, rpc: &BtcRpc, target: u32) -> Result<()> {
    let Some((mut h, mut hash)) = used.cursor()? else {
        tracing::warn!("used set has no cursor — run `indexd build-used` (daemon stopped); imports are refused until then");
        return Ok(());
    };
    let mut moved = false;
    loop {
        let node = hash_bytes(rpc.call("getblockhash", json!([h])).await?.as_str().context("getblockhash")?)?;
        if node == hash {
            break;
        }
        if h == 0 {
            bail!("used set: genesis hash mismatch — wrong network?");
        }
        h -= 1;
        hash = used
            .hash_at(h)?
            .with_context(|| format!("used set: reorg deeper than {HASH_DEPTH} blocks (no hash at {h}) — rebuild with `indexd build-used`"))?;
        moved = true;
    }
    if moved {
        used.set_cursor(h, &hash)?;
        tracing::warn!("used set cursor stepped back to {} (reorg while down)", h);
    }
    let mut applied = 0u32;
    while h < target {
        let b = fetch_block(rpc, h + 1).await?;
        let u = used.clone();
        tokio::task::spawn_blocking(move || u.apply(&[b])).await??;
        h += 1;
        applied += 1;
    }
    if applied > 0 {
        tracing::info!("used set caught up {} blocks to {}", applied, h);
    }
    Ok(())
}

const PIPELINE: usize = 64;
const FETCH_CONCURRENCY: usize = 6;
const CHUNK: u32 = 512;
const CHECKPOINT: Duration = Duration::from_secs(10);
const SHARD_BUF: usize = 256 << 10;
const NODE_POLL: Duration = Duration::from_secs(5);
const RPC_RETRIES: u32 = 40;
const RPC_RETRY_DELAY: Duration = Duration::from_secs(15);

async fn with_retry<T, F, Fut>(what: &str, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < RPC_RETRIES && e.downcast_ref::<crate::rpc::NodeError>().is_none() => {
                attempt += 1;
                println!("walk: {what} got no answer ({e:#}) — retry {attempt}/{RPC_RETRIES} in {}s", RPC_RETRY_DELAY.as_secs());
                tokio::time::sleep(RPC_RETRY_DELAY).await;
            }
            Err(e) => return Err(e),
        }
    }
}

struct Parts {
    dir: PathBuf,
}

impl Parts {
    fn new(store: &str) -> Self {
        Self { dir: PathBuf::from(format!("{store}.parts")) }
    }
    fn shard(&self, i: usize) -> PathBuf {
        self.dir.join(format!("{i:04}.bin"))
    }
    fn hashes(&self) -> PathBuf {
        self.dir.join("hashes.bin")
    }
    fn cursor(&self) -> PathBuf {
        self.dir.join("cursor")
    }
    fn done(&self) -> PathBuf {
        self.dir.join("walk.done")
    }
    fn read_cursor(&self) -> Result<Option<(u32, [u8; 32])>> {
        match fs::read(self.cursor()) {
            Ok(b) => Ok(cursor_parse(&b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    fn write_cursor(&self, height: u32, hash: &[u8; 32]) -> Result<()> {
        let tmp = self.dir.join("cursor.tmp");
        let mut f = File::create(&tmp)?;
        f.write_all(&cursor_bytes(height, hash))?;
        f.sync_all()?;
        fs::rename(&tmp, self.cursor())?;
        Ok(())
    }
}

struct Shards {
    files: Vec<BufWriter<File>>,
    hashes: BufWriter<File>,
    bytes: u64,
}

impl Shards {
    fn open(parts: &Parts, cursor: Option<(u32, [u8; 32])>) -> Result<Self> {
        fs::create_dir_all(&parts.dir)?;
        let mut files = Vec::with_capacity(SHARD_COUNT);
        let mut bytes = 0u64;
        for i in 0..SHARD_COUNT {
            let rec = TEMPLATES[i / SHARDS_PER_TEMPLATE].len as u64;
            let path = parts.shard(i);
            let f = OpenOptions::new().create(true).append(true).read(true).open(&path)?;
            let len = f.metadata()?.len();
            let keep = len - len % rec;
            if keep != len {
                f.set_len(keep)?;
            }
            bytes += keep;
            files.push(BufWriter::with_capacity(SHARD_BUF, f));
        }
        let hashes = OpenOptions::new().create(true).append(true).read(true).open(parts.hashes())?;
        let keep = match cursor {
            Some((h, _)) => (h as u64 + 1) * 32,
            None => 0,
        };
        if hashes.metadata()?.len() != keep {
            hashes.set_len(keep)?;
        }
        Ok(Self { files, hashes: BufWriter::with_capacity(64 << 10, hashes), bytes })
    }

    fn push(&mut self, b: &AppliedBlock) -> Result<()> {
        for s in &b.scripts {
            let i = shard_of(s).expect("parse_block keeps address scripts only");
            self.files[i].write_all(s)?;
            self.bytes += s.len() as u64;
        }
        self.hashes.write_all(&b.hash)?;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        for f in &mut self.files {
            f.flush()?;
            f.get_ref().sync_data()?;
        }
        self.hashes.flush()?;
        self.hashes.get_ref().sync_data()?;
        Ok(())
    }
}

fn hash_tail(parts: &Parts, tip: u32) -> Result<Vec<(u32, [u8; 32])>> {
    let from = tip.saturating_sub(HASH_DEPTH - 1);
    (from..=tip).map(|h| Ok((h, chain_hash(parts, h)?))).collect()
}

fn chain_hash(parts: &Parts, height: u32) -> Result<[u8; 32]> {
    use std::io::{Seek, SeekFrom};
    let mut f = File::open(parts.hashes())?;
    f.seek(SeekFrom::Start(height as u64 * 32))?;
    let mut b = [0u8; 32];
    f.read_exact(&mut b)?;
    Ok(b)
}

async fn chain_info(rpc: &BtcRpc) -> Result<(u32, u32, bool)> {
    let v: Value = rpc.call("getblockchaininfo", json!([])).await?;
    let blocks = v["blocks"].as_u64().context("getblockchaininfo: no blocks")? as u32;
    let prune = v["pruneheight"].as_u64().unwrap_or(0) as u32;
    let ibd = v["initialblockdownload"].as_bool().unwrap_or(false);
    Ok((blocks, prune, ibd))
}

fn hard_stop(next: u32, prune: u32) -> anyhow::Error {
    anyhow::anyhow!(
        "\n\n  HARD STOP: block {next} was pruned (pruneheight {prune}) before the walk reached it.\n  \
         The used set cannot be completed from this node — raise `prune=` and resync bitcoind from genesis, then rerun.\n"
    )
}

async fn align(parts: &Parts, rpc: &BtcRpc) -> Result<u32> {
    let Some((mut h, mut hash)) = parts.read_cursor()? else { return Ok(0) };
    loop {
        let (blocks, _, _) = with_retry("getblockchaininfo", || chain_info(rpc)).await?;
        if blocks < h {
            println!("walk: node at {blocks}, cursor at {h} — waiting for the node");
            tokio::time::sleep(NODE_POLL).await;
            continue;
        }
        let node = with_retry("getblockhash", || rpc.call("getblockhash", json!([h]))).await?;
        let node = hash_bytes(node.as_str().context("getblockhash")?)?;
        if node == hash {
            return Ok(h + 1);
        }
        if h == 0 {
            bail!("genesis hash mismatch — wrong network?");
        }
        h -= 1;
        hash = chain_hash(parts, h)?;
        parts.write_cursor(h, &hash)?;
        println!("walk: node's chain left our block {} — stepping back", h + 1);
    }
}

async fn produce(rpc: BtcRpc, mut next: u32, tx: mpsc::Sender<AppliedBlock>, stop: watch::Receiver<bool>) -> Result<()> {
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let (blocks, prune, ibd) = with_retry("getblockchaininfo", || chain_info(&rpc)).await?;
        if prune > next {
            return Err(hard_stop(next, prune));
        }
        if next > blocks {
            if !ibd {
                return Ok(());
            }
            tokio::time::sleep(NODE_POLL).await;
            continue;
        }
        let end = blocks.min(next + CHUNK - 1);
        let mut stream = futures_util::stream::iter(next..=end)
            .map(|h| {
                let rpc = rpc.clone();
                async move { with_retry("getblock", || fetch_block(&rpc, h)).await }
            })
            .buffered(FETCH_CONCURRENCY);
        while let Some(res) = stream.next().await {
            let b = res?;
            if tx.send(b).await.is_err() {
                return Ok(());
            }
        }
        next = end + 1;
    }
}

fn fmt_dur(d: Duration) -> String {
    let s = d.as_secs();
    format!("{}h{:02}m{:02}s", s / 3600, (s / 60) % 60, s % 60)
}

async fn walk(parts: &Parts, rpc: &BtcRpc) -> Result<(u32, [u8; 32])> {
    let start = align(parts, rpc).await?;
    let mut cursor = parts.read_cursor()?;
    let mut shards = Shards::open(parts, cursor)?;
    println!(
        "walk: from height {start} ({} shard files, {:.1} G already on disk)",
        SHARD_COUNT,
        shards.bytes as f64 / 1e9
    );

    let (tx, mut rx) = mpsc::channel::<AppliedBlock>(PIPELINE);
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut producer = tokio::spawn(produce(rpc.clone(), start, tx, stop_rx));
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let t0 = Instant::now();
    let mut last_checkpoint = Instant::now();
    let mut last_report = Instant::now();
    let mut scripts = 0u64;
    let mut blocks_done = 0u64;
    let mut interrupted = false;

    let mut dirty = false;
    loop {
        enum Tick {
            Block(AppliedBlock),
            Idle,
            Closed,
            Stop,
        }
        let tick = tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => Tick::Stop,
            _ = sigterm.recv() => Tick::Stop,
            r = tokio::time::timeout(CHECKPOINT, rx.recv()) => match r {
                Ok(Some(b)) => Tick::Block(b),
                Ok(None) => Tick::Closed,
                Err(_) => Tick::Idle,
            },
        };
        let b = match tick {
            Tick::Block(b) => b,
            Tick::Closed => break,
            Tick::Stop => {
                interrupted = true;
                break;
            }
            Tick::Idle => {
                if dirty {
                    if let Some((h, hash)) = cursor {
                        shards.sync()?;
                        parts.write_cursor(h, &hash)?;
                        last_checkpoint = Instant::now();
                        dirty = false;
                        println!("walk: at height {h}, waiting for the node");
                    }
                }
                continue;
            }
        };        scripts += b.scripts.len() as u64;
        blocks_done += 1;
        shards.push(&b)?;
        cursor = Some((b.height, b.hash));
        dirty = true;

        if last_checkpoint.elapsed() >= CHECKPOINT {
            shards.sync()?;
            parts.write_cursor(b.height, &b.hash)?;
            last_checkpoint = Instant::now();
            dirty = false;
        }
        if last_report.elapsed() >= Duration::from_secs(30) {
            let el = t0.elapsed();
            println!(
                "walk: height {} · {} scripts · {:.1} G scratch · {:.0} blk/s · {}",
                b.height,
                scripts,
                shards.bytes as f64 / 1e9,
                blocks_done as f64 / el.as_secs_f64().max(1.0),
                fmt_dur(el)
            );
            last_report = Instant::now();
        }
    }

    let _ = stop_tx.send(true);
    shards.sync()?;
    if let Some((h, hash)) = cursor {
        parts.write_cursor(h, &hash)?;
    }
    if interrupted {
        producer.abort();
        bail!("walk interrupted at height {:?} — rerun to resume", cursor.map(|c| c.0));
    }
    (&mut producer).await??;
    let (h, hash) = cursor.context("walk produced no blocks")?;
    println!(
        "walk: DONE at height {h} · {scripts} scripts · {:.1} G scratch · {}",
        shards.bytes as f64 / 1e9,
        fmt_dur(t0.elapsed())
    );
    File::create(parts.done())?;
    Ok((h, hash))
}

fn load(parts: &Parts, used: &UsedSet, tip: (u32, [u8; 32])) -> Result<()> {
    let t0 = Instant::now();
    let from = used.loaded()?;
    if from > 0 {
        println!("load: resuming at shard {from}/{SHARD_COUNT}");
    }
    let mut total = 0u64;
    for i in from as usize..SHARD_COUNT {
        let rec = TEMPLATES[i / SHARDS_PER_TEMPLATE].len;
        let data = fs::read(parts.shard(i))?;
        let mut records: Vec<&[u8]> = data.chunks_exact(rec).collect();
        let raw = records.len();
        records.sort_unstable();
        records.dedup();
        used.load_shard(i as u32, &records)?;
        total += records.len() as u64;
        if i % 32 == 31 || i + 1 == SHARD_COUNT {
            println!(
                "load: shard {}/{} ({}) · {} raw → {} unique · {} total · {}",
                i + 1,
                SHARD_COUNT,
                TEMPLATES[i / SHARDS_PER_TEMPLATE].name,
                raw,
                records.len(),
                total,
                fmt_dur(t0.elapsed())
            );
        }
    }
    let tail = hash_tail(parts, tip.0)?;
    used.finish_load(tip.0, &tip.1, &tail)?;
    println!("load: DONE · cursor at height {} · {}", tip.0, fmt_dur(t0.elapsed()));
    Ok(())
}

pub async fn run_cli(store_path: &str) -> Result<()> {
    let rpc = BtcRpc::from_env("http://127.0.0.1:8332")?;
    let parts = Parts::new(store_path);
    let used = UsedSet::open(store_path)?;
    if let Some((h, _)) = used.cursor()? {
        println!("used set at {store_path} is already built (cursor at height {h}); the daemon keeps it current from here");
        return Ok(());
    }
    let tip = if Path::new(&parts.done()).exists() {
        let c = parts.read_cursor()?.context("walk.done without a cursor")?;
        println!("walk already done at height {} — loading", c.0);
        c
    } else {
        walk(&parts, &rpc).await?
    };
    let u = used.clone();
    let p = Parts::new(store_path);
    tokio::task::spawn_blocking(move || load(&p, &u, tip)).await??;
    fs::remove_dir_all(&parts.dir).with_context(|| format!("removing {}", parts.dir.display()))?;
    println!("scratch {} removed", parts.dir.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dannesk_btc_codec::tx::{Header, Transaction, TxOut};

    fn spk(hex: &str) -> Vec<u8> {
        (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect()
    }

    #[test]
    fn templates_are_recognised_and_nothing_else() {
        let h20 = "00".repeat(20);
        let h32 = "11".repeat(32);
        assert!(is_address_script(&spk(&format!("0014{h20}"))));
        assert!(is_address_script(&spk(&format!("5120{h32}"))));
        assert!(is_address_script(&spk(&format!("76a914{h20}88ac"))));
        assert!(is_address_script(&spk(&format!("a914{h20}87"))));
        assert!(!is_address_script(&spk(&format!("0020{h32}"))));
        assert!(!is_address_script(&spk("6a0474657374")));
        assert!(!is_address_script(&spk(&format!("76a914{h20}88ad"))));
        assert!(!is_address_script(&[]));
        assert!(!is_address_script(&spk("0014")));
    }

    #[test]
    fn shards_are_template_major_then_hash_byte() {
        let a = spk(&format!("0014ab{}", "00".repeat(19)));
        let b = spk(&format!("76a914ab{}88ac", "00".repeat(19)));
        assert_eq!(shard_of(&a), Some(0xab >> 2));
        assert_eq!(shard_of(&b), Some(2 * 64 + (0xab >> 2)));
        assert_eq!(shard_of(&spk("6a00")), None);
    }

    fn block_hex(prev: [u8; 32], outs: &[Vec<u8>]) -> String {
        let tx = Transaction {
            version: 2,
            lock_time: 0,
            input: vec![],
            output: outs.iter().map(|s| TxOut { value: 1, script_pubkey: s.clone() }).collect(),
        };
        let mut block = Block {
            header: Header { version: 2, prev_blockhash: prev, merkle_root: [0; 32], time: 0, bits: 0, nonce: 0 },
            txdata: vec![tx],
        };
        block.header.merkle_root = block.merkle_root();
        block.to_hex()
    }

    #[test]
    fn parse_keeps_address_scripts_deduplicated() {
        let a = spk(&format!("0014{}", "aa".repeat(20)));
        let b = spk(&format!("5120{}", "bb".repeat(32)));
        let hex = block_hex([0; 32], &[a.clone(), spk("6a00"), b.clone(), a.clone()]);
        let p = parse_block(7, &hex).unwrap();
        assert_eq!(p.height, 7);
        assert_eq!(p.prev, [0; 32]);
        assert_eq!(p.scripts, vec![a, b]);
    }

    #[test]
    fn apply_then_contains_and_cursor() {
        let dir = std::env::temp_dir().join(format!("used-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let used = UsedSet::open(dir.join("used.redb").to_str().unwrap()).unwrap();
        let a = spk(&format!("0014{}", "aa".repeat(20)));
        let b = spk(&format!("a914{}87", "bb".repeat(20)));
        let c = spk(&format!("0014{}", "cc".repeat(20)));
        let g = parse_block(0, &block_hex([0; 32], &[a.clone()])).unwrap();
        let g_hash = g.hash;
        let one = parse_block(1, &block_hex(g_hash, &[b.clone(), a.clone()])).unwrap();
        assert_eq!(used.apply(&[g, one]).unwrap(), 2);
        assert_eq!(used.contains_batch(&[a.clone(), b.clone(), c.clone()]).unwrap(), vec![true, true, false]);
        assert_eq!(used.cursor().unwrap().map(|c| c.0), Some(1));
        let bad = parse_block(3, &block_hex([9; 32], &[c.clone()])).unwrap();
        assert!(used.apply(&[bad]).is_err());
        let same = parse_block(1, &block_hex(g_hash, &[b.clone(), a.clone()])).unwrap();
        assert_eq!(used.apply(&[same]).unwrap(), 0);
        let other = parse_block(1, &block_hex(g_hash, &[c.clone()])).unwrap();
        assert!(used.apply(&[other]).is_err());
        assert_eq!(used.hash_at(1).unwrap().is_some(), true);
        used.set_cursor(0, &g_hash).unwrap();
        assert_eq!(used.cursor().unwrap().map(|c| c.0), Some(0));
        assert_eq!(used.contains_batch(&[b]).unwrap(), vec![true]);
        drop(used);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn shard_files_trim_partial_tails_and_load_sorted() {
        let dir = std::env::temp_dir().join(format!("used-load-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let store = dir.join("used.redb").display().to_string();
        let parts = Parts::new(&store);
        let mut shards = Shards::open(&parts, None).unwrap();
        let a = spk(&format!("0014{}", "aa".repeat(20)));
        let b = spk(&format!("0014{}", "ab".repeat(20)));
        let g = parse_block(0, &block_hex([0; 32], &[b.clone(), a.clone(), a.clone()])).unwrap();
        shards.push(&g).unwrap();
        let one = parse_block(1, &block_hex(g.hash, &[a.clone()])).unwrap();
        shards.push(&one).unwrap();
        shards.sync().unwrap();
        parts.write_cursor(1, &one.hash).unwrap();
        drop(shards);
        OpenOptions::new().append(true).open(parts.shard(0xaa >> 2)).unwrap().write_all(&[1, 2, 3, 4, 5]).unwrap();
        let shards = Shards::open(&parts, parts.read_cursor().unwrap()).unwrap();
        assert_eq!(shards.files[0xaa >> 2].get_ref().metadata().unwrap().len(), 22 * 3);
        assert_eq!(chain_hash(&parts, 1).unwrap(), one.hash);
        drop(shards);
        let used = UsedSet::open(&store).unwrap();
        load(&parts, &used, (1, one.hash)).unwrap();
        assert_eq!(used.hash_at(1).unwrap(), Some(one.hash));
        assert_eq!(used.hash_at(0).unwrap(), Some(g.hash));
        assert_eq!(used.contains_batch(&[a, b, spk(&format!("0014{}", "ac".repeat(20)))]).unwrap(), vec![true, true, false]);
        assert_eq!(used.cursor().unwrap().map(|c| c.0), Some(1));
        assert_eq!(used.loaded().unwrap(), SHARD_COUNT as u32);
        drop(used);
        let _ = fs::remove_dir_all(&dir);
    }
}

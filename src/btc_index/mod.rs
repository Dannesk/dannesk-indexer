
pub mod audit;
pub mod build;
pub mod history;
pub mod store;
pub mod sync;
pub mod used;

use anyhow::{Context, Result};
use dannesk_btc_codec::address::Address;
use redb::{Database, ReadableDatabase};
use std::sync::Arc;
use tracing::info;

use store::{META, META_TIP, UTXOS, UtxoRecord};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IndexState {
    Opening,
    Empty,
    CatchingUp,
    Live,
}

#[derive(Debug, PartialEq, Eq)]
pub struct AddressBalance {
    pub sats: u64,
    pub utxo_count: usize,
}

#[derive(Clone)]
pub struct BtcIndex {
    db: Arc<Database>,
}

impl BtcIndex {
    pub fn open(path: &str) -> Result<Self> {
        let db = Database::create(path)
            .with_context(|| format!("Failed to open btc_index store at {}", path))?;
        let idx = Self { db: Arc::new(db) };
        info!("btc_index open at {} (tip: {:?})", path, idx.tip()?);
        Ok(idx)
    }

    pub fn tip(&self) -> Result<Option<(u32, [u8; 32])>> {
        let read = self.db.begin_read()?;
        let table = match read.open_table(META) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let Some(guard) = table.get(META_TIP)? else {
            return Ok(None);
        };
        let b = guard.value();
        if b.len() != 36 {
            return Ok(None);
        }
        let height = u32::from_le_bytes(b[..4].try_into().unwrap());
        let hash: [u8; 32] = b[4..36].try_into().unwrap();
        Ok(Some((height, hash)))
    }

    pub fn state(&self) -> IndexState {
        match self.tip() {
            Ok(Some(_)) => IndexState::CatchingUp,
            _ => IndexState::Empty,
        }
    }

    pub fn script_pubkey_for(address: &str) -> Option<Vec<u8>> {
        Address::parse(address).ok().map(|a| a.script_pubkey())
    }

    pub fn sample_spk(&self, seed: u64) -> Result<Option<Vec<u8>>> {
        let read = self.db.begin_read()?;
        let table = match read.open_multimap_table(UTXOS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut start = [0u8; 9];
        start[0] = [0x00u8, 0x51, 0x76, 0xa9][(seed % 4) as usize];
        start[1..9].copy_from_slice(&seed.rotate_left(17).to_be_bytes());
        let mut iter = table.range(&start[..]..)?;
        if let Some(entry) = iter.next() {
            let (key, _) = entry?;
            return Ok(Some(key.value().to_vec()));
        }
        let mut iter = table.range::<&[u8]>(..)?;
        match iter.next() {
            Some(entry) => {
                let (key, _) = entry?;
                Ok(Some(key.value().to_vec()))
            }
            None => Ok(None),
        }
    }

    pub fn utxos_for_spk(&self, spk: &[u8]) -> Result<Vec<UtxoRecord>> {
        let read = self.db.begin_read()?;
        let table = match read.open_multimap_table(UTXOS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut out = Vec::new();
        for entry in table.get(spk)? {
            let guard = entry?;
            if let Some(rec) = UtxoRecord::decode(guard.value()) {
                out.push(rec);
            }
        }
        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn insert_for_test(&self, spk: &[u8], rec: &UtxoRecord) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_multimap_table(UTXOS)?;
            table.insert(spk, rec.encode().as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    pub fn query_spk(&self, spk: &[u8]) -> Result<AddressBalance> {
        let read = self.db.begin_read()?;
        let table = match read.open_multimap_table(UTXOS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(AddressBalance {
                    sats: 0,
                    utxo_count: 0,
                });
            }
            Err(e) => return Err(e.into()),
        };
        let mut sats = 0u64;
        let mut utxo_count = 0usize;
        for entry in table.get(spk)? {
            let guard = entry?;
            if let Some(rec) = UtxoRecord::decode(guard.value()) {
                sats = sats.saturating_add(rec.sats);
                utxo_count += 1;
            }
        }
        Ok(AddressBalance { sats, utxo_count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_index() -> (BtcIndex, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "btc-index-test-{}-{:?}.redb",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        (BtcIndex::open(path.to_str().unwrap()).unwrap(), path)
    }

    fn rec(n: u8, sats: u64) -> UtxoRecord {
        UtxoRecord {
            txid: [n; 32],
            vout: n as u32,
            sats,
            height: 900_000,
        }
    }

    #[test]
    fn record_codec_roundtrip() {
        let r = rec(7, 123_456_789);
        assert_eq!(UtxoRecord::decode(&r.encode()), Some(r));
        assert_eq!(UtxoRecord::decode(&[0u8; 47]), None);
    }

    #[test]
    fn empty_store_is_empty_state_and_zero_balance() {
        let (idx, path) = temp_index();
        assert_eq!(idx.state(), IndexState::Empty);
        let spk = BtcIndex::script_pubkey_for("12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX").unwrap();
        let bal = idx.query_spk(&spk).unwrap();
        assert_eq!(
            bal,
            AddressBalance {
                sats: 0,
                utxo_count: 0
            }
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn sample_spk_finds_keys() {
        let (idx, path) = temp_index();
        assert_eq!(idx.sample_spk(7).unwrap(), None);
        let spk = BtcIndex::script_pubkey_for("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4").unwrap();
        let write = idx.db.begin_write().unwrap();
        {
            let mut table = write.open_multimap_table(store::UTXOS).unwrap();
            table.insert(spk.as_slice(), rec(1, 42).encode().as_slice()).unwrap();
        }
        write.commit().unwrap();
        for seed in 0..12u64 {
            assert_eq!(idx.sample_spk(seed).unwrap(), Some(spk.clone()), "seed {}", seed);
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn insert_query_remove() {
        let (idx, path) = temp_index();
        let spk = BtcIndex::script_pubkey_for("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4").unwrap();
        let other = BtcIndex::script_pubkey_for("12c6DSiU4Rq3P4ZxziKxzrL5LmMBrzjrJX").unwrap();

        let write = idx.db.begin_write().unwrap();
        {
            let mut table = write.open_multimap_table(UTXOS).unwrap();
            table.insert(spk.as_slice(), rec(1, 1000).encode().as_slice()).unwrap();
            table.insert(spk.as_slice(), rec(2, 250).encode().as_slice()).unwrap();
            table.insert(other.as_slice(), rec(3, 77).encode().as_slice()).unwrap();
        }
        write.commit().unwrap();

        assert_eq!(
            idx.query_spk(&spk).unwrap(),
            AddressBalance {
                sats: 1250,
                utxo_count: 2
            }
        );

        let write = idx.db.begin_write().unwrap();
        {
            let mut table = write.open_multimap_table(UTXOS).unwrap();
            table.remove(spk.as_slice(), rec(1, 1000).encode().as_slice()).unwrap();
        }
        write.commit().unwrap();

        assert_eq!(
            idx.query_spk(&spk).unwrap(),
            AddressBalance {
                sats: 250,
                utxo_count: 1
            }
        );
        assert_eq!(idx.query_spk(&other).unwrap().sats, 77);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn invalid_address_is_none() {
        assert_eq!(BtcIndex::script_pubkey_for("nonsense"), None);
        assert_eq!(
            BtcIndex::script_pubkey_for("tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx"),
            None
        );
    }
}

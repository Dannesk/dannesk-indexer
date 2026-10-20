
use redb::{MultimapTableDefinition, TableDefinition};

pub const UTXOS: MultimapTableDefinition<&[u8], &[u8]> = MultimapTableDefinition::new("utxos");

pub const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
pub const META_TIP: &str = "tip";

pub const UNDO: TableDefinition<u32, &[u8]> = TableDefinition::new("undo");
pub const UNDO_DEPTH: u32 = 144;

pub struct UndoRow {
    pub this_hash: [u8; 32],
    pub prev_hash: [u8; 32],
    pub spent: Vec<(Vec<u8>, UtxoRecord)>,
    pub created: Vec<(Vec<u8>, UtxoRecord)>,
}

impl UndoRow {
    pub fn encode(&self) -> Vec<u8> {
        fn put(list: &[(Vec<u8>, UtxoRecord)], out: &mut Vec<u8>) {
            out.extend_from_slice(&(list.len() as u32).to_le_bytes());
            for (spk, rec) in list {
                out.extend_from_slice(&(spk.len() as u16).to_le_bytes());
                out.extend_from_slice(spk);
                out.extend_from_slice(&rec.encode());
            }
        }
        let mut out = Vec::with_capacity(64 + (self.spent.len() + self.created.len()) * 90);
        out.extend_from_slice(&self.this_hash);
        out.extend_from_slice(&self.prev_hash);
        put(&self.spent, &mut out);
        put(&self.created, &mut out);
        out
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        fn take<'a>(b: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
            if b.len() < n {
                return None;
            }
            let (head, tail) = b.split_at(n);
            *b = tail;
            Some(head)
        }
        fn get(b: &mut &[u8]) -> Option<Vec<(Vec<u8>, UtxoRecord)>> {
            let n = u32::from_le_bytes(take(b, 4)?.try_into().ok()?) as usize;
            let mut list = Vec::with_capacity(n.min(1 << 20));
            for _ in 0..n {
                let len = u16::from_le_bytes(take(b, 2)?.try_into().ok()?) as usize;
                let spk = take(b, len)?.to_vec();
                let rec = UtxoRecord::decode(take(b, RECORD_LEN)?)?;
                list.push((spk, rec));
            }
            Some(list)
        }
        let mut b = b;
        let this_hash: [u8; 32] = take(&mut b, 32)?.try_into().ok()?;
        let prev_hash: [u8; 32] = take(&mut b, 32)?.try_into().ok()?;
        let spent = get(&mut b)?;
        let created = get(&mut b)?;
        if !b.is_empty() {
            return None;
        }
        Some(Self {
            this_hash,
            prev_hash,
            spent,
            created,
        })
    }
}

pub const RECORD_LEN: usize = 48;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UtxoRecord {
    pub txid: [u8; 32],
    pub vout: u32,
    pub sats: u64,
    pub height: u32,
}

impl UtxoRecord {
    pub fn encode(&self) -> [u8; RECORD_LEN] {
        let mut b = [0u8; RECORD_LEN];
        b[..32].copy_from_slice(&self.txid);
        b[32..36].copy_from_slice(&self.vout.to_le_bytes());
        b[36..44].copy_from_slice(&self.sats.to_le_bytes());
        b[44..48].copy_from_slice(&self.height.to_le_bytes());
        b
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != RECORD_LEN {
            return None;
        }
        Some(Self {
            txid: b[..32].try_into().ok()?,
            vout: u32::from_le_bytes(b[32..36].try_into().ok()?),
            sats: u64::from_le_bytes(b[36..44].try_into().ok()?),
            height: u32::from_le_bytes(b[44..48].try_into().ok()?),
        })
    }
}

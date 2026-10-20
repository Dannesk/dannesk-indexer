
use anyhow::{Context, Result, bail};
use redb::Database;
use std::io::{BufReader, Read};
use std::time::Instant;

use super::store::{META, META_TIP, UTXOS, UtxoRecord};

const SNAPSHOT_MAGIC: [u8; 5] = *b"utxo\xff";
const MAINNET_MAGIC: [u8; 4] = [0xf9, 0xbe, 0xb4, 0xd9];
const COMMIT_EVERY: u64 = 1_000_000;

pub fn run(dump_path: &str, store_path: &str) -> Result<()> {
    let file = std::fs::File::open(dump_path)
        .with_context(|| format!("Failed to open dump {}", dump_path))?;
    let mut r = BufReader::with_capacity(8 << 20, file);

    let mut magic = [0u8; 5];
    r.read_exact(&mut magic)?;
    if magic != SNAPSHOT_MAGIC {
        bail!("Not a utxo snapshot (bad magic {:02x?})", magic);
    }
    let version = u16::from_le_bytes(read_array(&mut r)?);
    if version != 2 {
        bail!("Unsupported snapshot version {} (expected 2)", version);
    }
    let net: [u8; 4] = read_array(&mut r)?;
    if net != MAINNET_MAGIC {
        bail!("Snapshot is not mainnet (network magic {:02x?})", net);
    }
    let base_hash: [u8; 32] = read_array(&mut r)?;
    let coin_count = u64::from_le_bytes(read_array(&mut r)?);
    println!(
        "snapshot ok: {} coins, base block {}",
        coin_count,
        hex_rev(&base_hash)
    );

    let db = Database::create(store_path)
        .with_context(|| format!("Failed to create store {}", store_path))?;

    let started = Instant::now();
    let mut parsed = 0u64;
    let mut indexed = 0u64;
    let mut skipped_p2pk_uncompressed = 0u64;
    let mut total_sats = 0u128;
    let mut base_height = 0u32;

    let mut write = db.begin_write()?;
    let mut table = write.open_multimap_table(UTXOS)?;

    while parsed < coin_count {
        let mut txid = [0u8; 32];
        r.read_exact(&mut txid)?;
        let n_coins = read_compactsize(&mut r)?;
        for _ in 0..n_coins {
            let vout = read_compactsize(&mut r)?;
            if vout > u32::MAX as u64 {
                bail!("Insane vout {} at coin {} — parser desync", vout, parsed);
            }
            let code = read_varint(&mut r)?;
            let height = (code >> 1) as u32;
            let sats = decompress_amount(read_varint(&mut r)?);
            let spk = read_compressed_script(&mut r)?;

            total_sats += sats as u128;
            base_height = base_height.max(height);

            match spk {
                Some(spk) => {
                    let rec = UtxoRecord {
                        txid,
                        vout: vout as u32,
                        sats,
                        height,
                    };
                    table.insert(spk.as_slice(), rec.encode().as_slice())?;
                    indexed += 1;
                }
                None => skipped_p2pk_uncompressed += 1,
            }

            parsed += 1;
            if parsed % COMMIT_EVERY == 0 {
                drop(table);
                write.commit()?;
                write = db.begin_write()?;
                table = write.open_multimap_table(UTXOS)?;
                let rate = parsed as f64 / started.elapsed().as_secs_f64();
                println!(
                    "{:>4}M / {}M coins ({:.0}k/s)",
                    parsed / 1_000_000,
                    coin_count / 1_000_000,
                    rate / 1000.0
                );
            }
        }
    }

    let mut trailing = [0u8; 1];
    if r.read(&mut trailing)? != 0 {
        bail!("Trailing data after {} coins — parser desync", parsed);
    }

    {
        let mut meta = write.open_table(META)?;
        let mut tip = Vec::with_capacity(36);
        tip.extend_from_slice(&base_height.to_le_bytes());
        tip.extend_from_slice(&base_hash);
        meta.insert(META_TIP, tip.as_slice())?;
    }
    drop(table);
    write.commit()?;

    println!(
        "build complete in {:.0}s: {} parsed, {} indexed, {} uncompressed-P2PK skipped",
        started.elapsed().as_secs_f64(),
        parsed,
        indexed,
        skipped_p2pk_uncompressed
    );
    println!(
        "index tip: height {} block {}",
        base_height,
        hex_rev(&base_hash)
    );
    println!(
        "total: {}.{:08} BTC — MUST match gettxoutsetinfo total_amount",
        total_sats / 100_000_000,
        total_sats % 100_000_000
    );
    Ok(())
}

fn read_array<const N: usize>(r: &mut impl Read) -> Result<[u8; N]> {
    let mut b = [0u8; N];
    r.read_exact(&mut b)?;
    Ok(b)
}

fn read_compactsize(r: &mut impl Read) -> Result<u64> {
    let [tag] = read_array(r)?;
    Ok(match tag {
        0xFD => u16::from_le_bytes(read_array(r)?) as u64,
        0xFE => u32::from_le_bytes(read_array(r)?) as u64,
        0xFF => u64::from_le_bytes(read_array(r)?),
        n => n as u64,
    })
}

fn read_varint(r: &mut impl Read) -> Result<u64> {
    let mut n: u64 = 0;
    loop {
        let [b] = read_array(r)?;
        n = n
            .checked_shl(7)
            .and_then(|n| n.checked_add((b & 0x7F) as u64))
            .context("VARINT overflow — parser desync")?;
        if b & 0x80 == 0 {
            return Ok(n);
        }
        n = n.checked_add(1).context("VARINT overflow")?;
    }
}

fn decompress_amount(x: u64) -> u64 {
    if x == 0 {
        return 0;
    }
    let mut x = x - 1;
    let e = x % 10;
    x /= 10;
    let mut n = if e < 9 {
        let d = x % 9 + 1;
        x /= 9;
        x * 10 + d
    } else {
        x + 1
    };
    for _ in 0..e {
        n *= 10;
    }
    n
}

fn read_compressed_script(r: &mut impl Read) -> Result<Option<Vec<u8>>> {
    let size = read_varint(r)?;
    Ok(match size {
        0 => {
            let h: [u8; 20] = read_array(r)?;
            let mut s = Vec::with_capacity(25);
            s.extend_from_slice(&[0x76, 0xa9, 0x14]);
            s.extend_from_slice(&h);
            s.extend_from_slice(&[0x88, 0xac]);
            Some(s)
        }
        1 => {
            let h: [u8; 20] = read_array(r)?;
            let mut s = Vec::with_capacity(23);
            s.extend_from_slice(&[0xa9, 0x14]);
            s.extend_from_slice(&h);
            s.push(0x87);
            Some(s)
        }
        2 | 3 => {
            let x: [u8; 32] = read_array(r)?;
            let mut s = Vec::with_capacity(35);
            s.push(0x21);
            s.push(size as u8);
            s.extend_from_slice(&x);
            s.push(0xac);
            Some(s)
        }
        4 | 5 => {
            let _x: [u8; 32] = read_array(r)?;
            None
        }
        n => {
            let len = n - 6;
            if len > 10_000 {
                bail!("Insane script length {} — parser desync", len);
            }
            let mut s = vec![0u8; len as usize];
            r.read_exact(&mut s)?;
            Some(s)
        }
    })
}

fn hex_rev(b: &[u8; 32]) -> String {
    b.iter().rev().map(|x| format!("{:02x}", x)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn varint_bytes(mut n: u64) -> Vec<u8> {
        let mut tmp = Vec::new();
        loop {
            tmp.push((n & 0x7F) as u8 | if tmp.is_empty() { 0 } else { 0x80 });
            if n <= 0x7F {
                break;
            }
            n = (n >> 7) - 1;
        }
        tmp.reverse();
        tmp
    }

    fn compress_amount(mut n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        let mut e = 0u64;
        while n % 10 == 0 && e < 9 {
            n /= 10;
            e += 1;
        }
        if e < 9 {
            let d = n % 10;
            n /= 10;
            1 + (n * 9 + d - 1) * 10 + e
        } else {
            1 + (n - 1) * 10 + 9
        }
    }

    #[test]
    fn varint_known_vectors_and_roundtrip() {
        assert_eq!(varint_bytes(0), vec![0x00]);
        assert_eq!(varint_bytes(127), vec![0x7F]);
        assert_eq!(varint_bytes(128), vec![0x80, 0x00]);
        assert_eq!(varint_bytes(16511), vec![0xFF, 0x7F]);
        for n in [0u64, 1, 127, 128, 255, 256, 16511, 65535, u32::MAX as u64] {
            let b = varint_bytes(n);
            assert_eq!(read_varint(&mut b.as_slice()).unwrap(), n, "n={}", n);
        }
    }

    #[test]
    fn amount_roundtrip() {
        for sats in [
            0u64,
            1,
            545,
            600,
            1_000,
            50_000,
            100_000_000,
            2_099_999_999_999_999,
            2_100_000_000_000_000,
        ] {
            assert_eq!(
                decompress_amount(compress_amount(sats)),
                sats,
                "sats={}",
                sats
            );
        }
        assert_eq!(decompress_amount(0), 0);
        assert_eq!(decompress_amount(1), 1);
    }

    #[test]
    fn compactsize_vectors() {
        assert_eq!(read_compactsize(&mut [0x00].as_slice()).unwrap(), 0);
        assert_eq!(read_compactsize(&mut [0xFC].as_slice()).unwrap(), 0xFC);
        assert_eq!(
            read_compactsize(&mut [0xFD, 0xFD, 0x00].as_slice()).unwrap(),
            0xFD
        );
        assert_eq!(
            read_compactsize(&mut [0xFE, 0x00, 0x00, 0x01, 0x00].as_slice()).unwrap(),
            0x10000
        );
    }

    #[test]
    fn script_templates() {
        let mut input = vec![0x00];
        input.extend_from_slice(&[0x11; 20]);
        let s = read_compressed_script(&mut input.as_slice()).unwrap().unwrap();
        assert_eq!(s.len(), 25);
        assert_eq!(&s[..3], &[0x76, 0xa9, 0x14]);
        assert_eq!(&s[23..], &[0x88, 0xac]);

        let mut input = vec![0x06 + 22];
        input.push(0x00);
        input.push(0x14);
        input.extend_from_slice(&[0x22; 20]);
        let s = read_compressed_script(&mut input.as_slice()).unwrap().unwrap();
        assert_eq!(s.len(), 22);
        assert_eq!(s[0], 0x00);

        let mut input = vec![0x04];
        input.extend_from_slice(&[0x33; 32]);
        assert_eq!(read_compressed_script(&mut input.as_slice()).unwrap(), None);
    }
}

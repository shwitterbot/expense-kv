//! Append-before-apply WAL with CRC32, absolute expiration times and atomic rewrite.
use crate::protocol::{MAX_BULK, MAX_FRAME, MAX_KEY, invalid};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"EKVAOF01";
const MAX_RECORD: usize = MAX_FRAME + 64 * 1024;

#[derive(Debug, Clone)]
pub enum Mutation {
    Set(Vec<u8>, Vec<u8>, Option<i64>),
    Delete(Vec<u8>),
    Clear,
}

pub struct Journal {
    file: File,
    dir: PathBuf,
    // Dedicated inode: remains locked even when the WAL inode is replaced.
    _lock: File,
    pub bytes: u64,
    pub failed: bool,
}

const CRC32_TABLE: [u32; 256] = {
    let mut table = [0; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(crc & 1));
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for b in data {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ u32::from(*b)) & 0xff) as usize];
    }
    !crc
}

fn encode(items: &[Mutation]) -> Vec<u8> {
    let mut b = vec![0; 12];
    b.extend((items.len() as u32).to_le_bytes());
    for item in items {
        match item {
            Mutation::Set(k, v, expiry) => {
                b.push(1);
                b.extend((k.len() as u32).to_le_bytes());
                b.extend((v.len() as u32).to_le_bytes());
                b.extend(expiry.unwrap_or(-1).to_le_bytes());
                b.extend(k);
                b.extend(v);
            }
            Mutation::Delete(k) => {
                b.push(2);
                b.extend((k.len() as u32).to_le_bytes());
                b.extend(k);
            }
            Mutation::Clear => b.push(3),
        }
    }
    b
}

fn take<'a>(b: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
    if b.len() < n {
        return Err(invalid("truncated WAL payload"));
    }
    let (head, tail) = b.split_at(n);
    *b = tail;
    Ok(head)
}
fn number(b: &mut &[u8]) -> io::Result<usize> {
    Ok(u32::from_le_bytes(take(b, 4)?.try_into().unwrap()) as usize)
}
fn key(b: &mut &[u8], n: usize) -> io::Result<Vec<u8>> {
    if n > MAX_KEY {
        return Err(invalid("WAL key too long"));
    }
    Ok(take(b, n)?.to_vec())
}
fn decode(mut b: &[u8]) -> io::Result<Vec<Mutation>> {
    let count = number(&mut b)?;
    if count > 1024 {
        return Err(invalid("WAL mutation count exceeds limit"));
    }
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        match take(&mut b, 1)?[0] {
            1 => {
                let kl = number(&mut b)?;
                let vl = number(&mut b)?;
                if vl > MAX_BULK {
                    return Err(invalid("WAL value too large"));
                }
                let expiry = i64::from_le_bytes(take(&mut b, 8)?.try_into().unwrap());
                if expiry < -1 {
                    return Err(invalid("invalid WAL expiry"));
                }
                let k = key(&mut b, kl)?;
                out.push(Mutation::Set(
                    k,
                    take(&mut b, vl)?.to_vec(),
                    (expiry >= 0).then_some(expiry),
                ));
            }
            2 => {
                let kl = number(&mut b)?;
                out.push(Mutation::Delete(key(&mut b, kl)?));
            }
            3 => out.push(Mutation::Clear),
            _ => return Err(invalid("unknown WAL operation")),
        }
    }
    if !b.is_empty() {
        return Err(invalid("trailing WAL payload"));
    }
    Ok(out)
}

fn write_record(file: &mut impl Write, items: &[Mutation]) -> io::Result<u64> {
    let mut record = encode(items);
    if record.len() - 12 > MAX_RECORD {
        return Err(invalid("WAL record too large"));
    }
    let len = ((record.len() - 12) as u32).to_le_bytes();
    let checksum = crc32(&record[12..]).to_le_bytes();
    // The length has its own checksum; a damaged length must not look like a torn tail.
    record[..4].copy_from_slice(&len);
    record[4..8].copy_from_slice(&crc32(&len).to_le_bytes());
    record[8..12].copy_from_slice(&checksum);
    file.write_all(&record)?;
    Ok(record.len() as u64)
}

impl Journal {
    pub fn open(dir: &Path, mut apply: impl FnMut(Vec<Mutation>)) -> io::Result<Self> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .mode(0o600)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("LOCK"))?;
        lock.try_lock()
            .map_err(|e| io::Error::other(format!("data directory already locked: {e}")))?;
        let mut file = OpenOptions::new()
            .create(true)
            .mode(0o600)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("data.aof"))?;
        if file.metadata()?.len() == 0 {
            file.write_all(MAGIC)?;
            file.sync_all()?;
            File::open(dir)?.sync_all()?;
        }
        file.seek(SeekFrom::Start(0))?;
        let mut magic = [0; 8];
        file.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(invalid("invalid WAL header"));
        }
        let size = file.metadata()?.len();
        let mut pos = 8;
        let mut reader = BufReader::new(&mut file);
        while pos < size {
            if size - pos < 12 {
                break;
            }
            let mut header = [0; 12];
            reader.read_exact(&mut header)?;
            if crc32(&header[..4]) != u32::from_le_bytes(header[4..8].try_into().unwrap()) {
                return Err(invalid("WAL length checksum mismatch"));
            }
            let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
            if !(4..=MAX_RECORD).contains(&len) {
                return Err(invalid("invalid WAL record length"));
            }
            if size - pos - 12 < len as u64 {
                break;
            }
            let mut b = vec![0; len];
            reader.read_exact(&mut b)?;
            if crc32(&b) != u32::from_le_bytes(header[8..12].try_into().unwrap()) {
                return Err(invalid("WAL checksum mismatch (refusing to discard data)"));
            }
            apply(decode(&b)?);
            pos += 12 + len as u64;
        }
        drop(reader);
        if pos < size {
            eprintln!(
                "Recovering incomplete final WAL record: {} bytes",
                size - pos
            );
            file.set_len(pos)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            dir: dir.to_owned(),
            _lock: lock,
            bytes: pos,
            failed: false,
        })
    }

    pub fn append(&mut self, items: &[Mutation]) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other(
                "WAL is in failed state; restart after fixing disk",
            ));
        }
        let result = (|| {
            let n = write_record(&mut self.file, items)?;
            self.file.sync_data()?;
            self.bytes += n;
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    pub fn compact(&mut self, items: impl Iterator<Item = Mutation>) -> io::Result<()> {
        if self.failed {
            return Err(io::Error::other("WAL is in failed state"));
        }
        // Supported deployment platforms: Linux/macOS (atomic replace + directory fsync).
        let result = (|| {
            let tmp = self.dir.join("rewrite.tmp");
            let file = OpenOptions::new()
                .create(true)
                .mode(0o600)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&tmp)?;
            let mut writer = BufWriter::new(file);
            writer.write_all(MAGIC)?;
            let mut bytes = 8;
            for item in items {
                bytes += write_record(&mut writer, &[item])?;
            }
            let file = writer.into_inner().map_err(|e| e.into_error())?;
            file.sync_all()?;
            fs::rename(&tmp, self.dir.join("data.aof"))?;
            // Switch handles immediately after rename; never append to the obsolete inode.
            self.file = file;
            self.bytes = bytes;
            File::open(&self.dir)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_ieee_vectors_and_original_bitwise_algorithm() {
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"123456789"), 0xcbf43926);
        let data: Vec<u8> = (0..4096).map(|i| i as u8).collect();
        let mut reference = !0u32;
        for b in &data {
            reference ^= u32::from(*b);
            for _ in 0..8 {
                reference = (reference >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(reference & 1));
            }
        }
        assert_eq!(crc32(&data), !reference);
    }

    #[test]
    fn record_encoding_preserves_existing_disk_format() {
        let mut record = Vec::new();
        let size = write_record(
            &mut record,
            &[Mutation::Set(b"k".to_vec(), b"v".to_vec(), None)],
        )
        .unwrap();
        let mut body = Vec::new();
        body.extend(1u32.to_le_bytes());
        body.push(1);
        body.extend(1u32.to_le_bytes());
        body.extend(1u32.to_le_bytes());
        body.extend((-1i64).to_le_bytes());
        body.extend(b"kv");
        let len = (body.len() as u32).to_le_bytes();
        let mut expected = Vec::from(len);
        expected.extend(crc32(&len).to_le_bytes());
        expected.extend(crc32(&body).to_le_bytes());
        expected.extend(body);
        assert_eq!(record, expected);
        assert_eq!(size, record.len() as u64);
    }
}

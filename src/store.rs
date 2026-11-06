use crate::journal::{Journal, Mutation};
use crate::protocol::{MAX_BULK, MAX_FRAME, MAX_KEY, Reply, write_bulk};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const GET_BATCH_BYTES: usize = 256 * 1024;

#[derive(Clone)]
struct Entry {
    value: Arc<[u8]>,
    expires_at: Option<i64>,
}

pub struct Store {
    entries: HashMap<Vec<u8>, Entry>,
    expirations: BTreeSet<(i64, Vec<u8>)>,
    journal: Journal,
    bytes: usize,
    max_bytes: usize,
    compact_bytes: u64,
    hits: AtomicU64,
    misses: AtomicU64,
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before Unix epoch")
        .as_millis() as i64
}
fn cost(key: &[u8], value: &[u8], expiry: Option<i64>) -> usize {
    key.len() + value.len() + 64 + expiry.map_or(0, |_| key.len() + 48)
}
fn err(message: &str) -> Reply {
    Reply::Error(message.into())
}
fn int(v: &[u8]) -> Result<i64, Reply> {
    std::str::from_utf8(v)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| err("ERR value is not an integer or out of range"))
}
fn key(k: &[u8]) -> Result<(), Reply> {
    if k.len() > MAX_KEY {
        Err(err("ERR key exceeds 1024 bytes"))
    } else {
        Ok(())
    }
}
fn apply(map: &mut HashMap<Vec<u8>, Entry>, items: Vec<Mutation>) {
    for item in items {
        match item {
            Mutation::Set(k, value, expires_at) => {
                map.insert(
                    k,
                    Entry {
                        value: value.into(),
                        expires_at,
                    },
                );
            }
            Mutation::Delete(k) => {
                map.remove(&k);
            }
            Mutation::Clear => map.clear(),
        }
    }
}

impl Store {
    pub fn open(dir: &Path, max_bytes: usize, compact_bytes: u64) -> io::Result<Self> {
        let mut entries = HashMap::new();
        let journal = Journal::open(dir, |items| apply(&mut entries, items))?;
        let expirations = entries
            .iter()
            .filter_map(|(k, e)| e.expires_at.map(|t| (t, k.clone())))
            .collect();
        let bytes = entries
            .iter()
            .map(|(k, e)| cost(k, &e.value, e.expires_at))
            .sum();
        let mut store = Self {
            entries,
            expirations,
            journal,
            bytes,
            max_bytes,
            compact_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        };
        store.prune(now_ms());
        if store.bytes > max_bytes {
            return Err(io::Error::other("recovered data exceeds KV_MAX_BYTES"));
        }
        Ok(store)
    }

    fn prune(&mut self, now: i64) {
        while let Some((t, _)) = self.expirations.first() {
            if *t > now {
                break;
            }
            let (_, k) = self.expirations.pop_first().unwrap();
            if let Some(old) = self.entries.remove(&k) {
                self.bytes -= cost(&k, &old.value, old.expires_at);
            }
        }
    }
    fn get(&mut self, k: &[u8], now: i64) -> Option<&Entry> {
        if self
            .entries
            .get(k)
            .is_some_and(|e| e.expires_at.is_some_and(|t| t <= now))
            && let Some(old) = self.entries.remove(k)
        {
            self.bytes -= cost(k, &old.value, old.expires_at);
            self.expirations
                .remove(&(old.expires_at.unwrap(), k.to_vec()));
        }
        self.entries.get(k)
    }

    pub fn get_batch(&self, keys: &[&[u8]], now: i64) -> Vec<Reply> {
        let mut replies = Vec::new();
        self.read_batch(keys, now, MAX_FRAME, |value| {
            replies.push(match value {
                Ok(value) => Reply::Bulk(value.cloned()),
                Err(reply) => reply,
            });
        });
        replies
    }

    pub fn encode_get_batch(&self, keys: &[&[u8]], now: i64, output: &mut Vec<u8>) -> usize {
        let size = keys
            .first()
            .and_then(|k| self.entries.get(*k))
            .map_or(0, |e| e.value.len());
        let capacity = ((size + 32) * keys.len())
            .min(GET_BATCH_BYTES)
            .max(size + 32);
        output.clear();
        if output.capacity() > GET_BATCH_BYTES && capacity <= GET_BATCH_BYTES {
            *output = Vec::new();
        }
        if output.capacity() < capacity {
            output.reserve_exact(capacity);
        }
        // Reserve framing space; a single legal value may exceed the batch target.
        let value_budget = GET_BATCH_BYTES.saturating_sub(32 * keys.len());
        self.read_batch(keys, now, value_budget, |value| {
            let additional = match &value {
                Ok(Some(value)) => value.len() + 32,
                Err(Reply::Error(message)) => message.len() + 3,
                _ => 32,
            };
            if output.capacity() - output.len() < additional {
                output.reserve_exact(additional);
            }
            match value {
                Ok(value) => write_bulk(output, value.map(|v| v.as_ref())),
                Err(reply) => reply.write_to(output),
            }
            .expect("writing to Vec cannot fail");
        })
    }

    fn read_batch(
        &self,
        keys: &[&[u8]],
        now: i64,
        value_budget: usize,
        mut emit: impl FnMut(Result<Option<&Arc<[u8]>>, Reply>),
    ) -> usize {
        let mut bytes = 0;
        let mut hits = 0;
        let mut misses = 0;
        let mut count = 0;
        for k in keys {
            if let Err(reply) = key(k) {
                emit(Err(reply));
                count += 1;
                continue;
            }
            let entry = self
                .entries
                .get(*k)
                .filter(|e| e.expires_at.is_none_or(|t| t > now));
            let size = entry.map_or(0, |e| e.value.len());
            if count > 0 && bytes + size > value_budget {
                break;
            }
            bytes += size;
            let value = entry.map(|e| &e.value);
            if value.is_some() {
                hits += 1;
            } else {
                misses += 1;
            }
            emit(Ok(value));
            count += 1;
        }
        if hits > 0 {
            self.hits.fetch_add(hits, Ordering::Relaxed);
        }
        if misses > 0 {
            self.misses.fetch_add(misses, Ordering::Relaxed);
        }
        count
    }
    fn commit(&mut self, items: Vec<Mutation>, now: i64) -> Result<(), Reply> {
        // Commands construct a batch with unique keys. A batch is one WAL record.
        let projected = |s: &Self| {
            let mut bytes = s.bytes as i128;
            for item in &items {
                match item {
                    Mutation::Set(k, v, expiry) => {
                        bytes -= s
                            .entries
                            .get(k)
                            .map_or(0, |e| cost(k, &e.value, e.expires_at))
                            as i128;
                        bytes += cost(k, v, *expiry) as i128;
                    }
                    Mutation::Delete(k) => {
                        bytes -= s
                            .entries
                            .get(k)
                            .map_or(0, |e| cost(k, &e.value, e.expires_at))
                            as i128
                    }
                    Mutation::Clear => bytes = 0,
                }
            }
            bytes
        };
        let mut bytes = projected(self);
        if bytes > self.max_bytes as i128 {
            self.prune(now);
            bytes = projected(self);
        }
        if bytes > self.max_bytes as i128 {
            return Err(err("OOM configured memory limit reached"));
        }
        self.journal
            .append(&items)
            .map_err(|e| err(&format!("MISCONF persistence failed: {e}")))?;
        for item in &items {
            match item {
                Mutation::Set(k, _, expiry) => {
                    if let Some(t) = self.entries.get(k).and_then(|e| e.expires_at) {
                        self.expirations.remove(&(t, k.clone()));
                    }
                    if let Some(t) = expiry {
                        self.expirations.insert((*t, k.clone()));
                    }
                }
                Mutation::Delete(k) => {
                    if let Some(t) = self.entries.get(k).and_then(|e| e.expires_at) {
                        self.expirations.remove(&(t, k.clone()));
                    }
                }
                Mutation::Clear => self.expirations.clear(),
            }
        }
        apply(&mut self.entries, items);
        self.bytes = bytes as usize;
        Ok(())
    }
    pub fn compact(&mut self, now: i64) -> io::Result<()> {
        self.prune(now);
        self.journal.compact(
            self.entries
                .iter()
                .map(|(k, e)| Mutation::Set(k.clone(), e.value.to_vec(), e.expires_at)),
        )
    }
    pub fn maintain(&mut self) {
        self.prune(now_ms());
        // Avoid rewriting every second when the live dataset itself exceeds the threshold.
        let threshold = self
            .compact_bytes
            .max((self.bytes as u64).saturating_mul(2));
        if !self.journal.failed
            && self.journal.bytes > threshold
            && let Err(e) = self.compact(now_ms())
        {
            eprintln!("WAL compaction failed: {e}");
        }
    }
    pub fn execute(&mut self, args: &[Vec<u8>], now: i64) -> (Reply, bool) {
        let previous = self.journal.bytes;
        let reply = match self.run(args, now) {
            Ok(r) | Err(r) => r,
        };
        // Durable write acknowledgements must not wait behind later pipelined commands.
        (reply, self.journal.bytes != previous)
    }
    fn run(&mut self, a: &[Vec<u8>], now: i64) -> Result<Reply, Reply> {
        let Some(name) = a.first() else {
            return Err(err("ERR empty command"));
        };
        let command = name.to_ascii_uppercase();
        let n = a.len();
        let ok = Reply::Simple("OK");
        match command.as_slice() {
            b"PING" if n == 1 => Ok(Reply::Simple("PONG")),
            b"PING" | b"ECHO" if n == 2 => Ok(Reply::Bulk(Some(Arc::from(a[1].as_slice())))),
            b"GET" if n == 2 => Ok(self.get_batch(&[&a[1]], now).pop().unwrap()),
            b"MGET" if n > 1 => {
                let mut total = 0;
                let mut values = Vec::new();
                for k in &a[1..] {
                    key(k)?;
                    let value = self.get(k, now).map(|e| e.value.clone());
                    total += value.as_ref().map_or(0, |v| v.len());
                    if total > MAX_FRAME {
                        return Err(err("ERR MGET reply exceeds 2 MiB"));
                    }
                    if value.is_some() {
                        self.hits.fetch_add(1, Ordering::Relaxed);
                    } else {
                        self.misses.fetch_add(1, Ordering::Relaxed);
                    }
                    values.push(Reply::Bulk(value));
                }
                Ok(Reply::Array(values))
            }
            b"SET" if n >= 3 => {
                key(&a[1])?;
                if a[2].len() > MAX_BULK {
                    return Err(err("ERR value too large"));
                }
                let mut condition = None;
                let mut expiry = None;
                let mut i = 3;
                while i < n {
                    match a[i].to_ascii_uppercase().as_slice() {
                        b"NX" | b"XX" if condition.is_none() => {
                            condition = Some(a[i].eq_ignore_ascii_case(b"NX"))
                        }
                        b"EX" | b"PX" if expiry.is_none() && i + 1 < n => {
                            let factor = if a[i].eq_ignore_ascii_case(b"EX") {
                                1000
                            } else {
                                1
                            };
                            i += 1;
                            let ttl = int(&a[i])?;
                            if ttl <= 0 {
                                return Err(err("ERR invalid expire time in SET"));
                            }
                            expiry = Some(
                                ttl.checked_mul(factor)
                                    .and_then(|t| now.checked_add(t))
                                    .ok_or_else(|| err("ERR expiration overflow"))?,
                            );
                        }
                        _ => return Err(err("ERR syntax error")),
                    }
                    i += 1;
                }
                let exists = self.get(&a[1], now).is_some();
                if condition == Some(exists) {
                    return Ok(Reply::Bulk(None));
                }
                self.commit(vec![Mutation::Set(a[1].clone(), a[2].clone(), expiry)], now)?;
                Ok(ok)
            }
            b"MSET" if n >= 3 && n % 2 == 1 => {
                let mut final_values = HashMap::new();
                for pair in a[1..].as_chunks::<2>().0 {
                    key(&pair[0])?;
                    final_values.insert(pair[0].clone(), pair[1].clone());
                }
                self.commit(
                    final_values
                        .into_iter()
                        .map(|(k, v)| Mutation::Set(k, v, None))
                        .collect(),
                    now,
                )?;
                Ok(ok)
            }
            b"DEL" if n > 1 => {
                let mut keys = HashSet::new();
                for k in &a[1..] {
                    key(k)?;
                    if self.get(k, now).is_some() {
                        keys.insert(k.clone());
                    }
                }
                let count = keys.len();
                if count > 0 {
                    self.commit(keys.into_iter().map(Mutation::Delete).collect(), now)?;
                }
                Ok(Reply::Integer(count as i64))
            }
            b"EXISTS" if n > 1 => {
                let mut count = 0;
                for k in &a[1..] {
                    key(k)?;
                    count += i64::from(self.get(k, now).is_some());
                }
                Ok(Reply::Integer(count))
            }
            b"INCR" | b"DECR" | b"INCRBY"
                if (n == 2 && command != b"INCRBY") || (n == 3 && command == b"INCRBY") =>
            {
                key(&a[1])?;
                let delta = if n == 3 {
                    int(&a[2])?
                } else if command == b"DECR" {
                    -1
                } else {
                    1
                };
                let (old, expiry) = match self.get(&a[1], now) {
                    Some(e) => (int(&e.value)?, e.expires_at),
                    None => (0, None),
                };
                let value = old
                    .checked_add(delta)
                    .ok_or_else(|| err("ERR increment would overflow"))?;
                self.commit(
                    vec![Mutation::Set(
                        a[1].clone(),
                        value.to_string().into_bytes(),
                        expiry,
                    )],
                    now,
                )?;
                Ok(Reply::Integer(value))
            }
            b"EXPIRE" | b"PEXPIRE" if n == 3 => {
                key(&a[1])?;
                let factor = if command == b"EXPIRE" { 1000 } else { 1 };
                let ttl = int(&a[2])?;
                let expiry = ttl
                    .checked_mul(factor)
                    .and_then(|t| now.checked_add(t))
                    .ok_or_else(|| err("ERR expiration overflow"))?;
                let Some(entry) = self.get(&a[1], now).cloned() else {
                    return Ok(Reply::Integer(0));
                };
                let op = if ttl <= 0 {
                    Mutation::Delete(a[1].clone())
                } else {
                    Mutation::Set(a[1].clone(), entry.value.to_vec(), Some(expiry))
                };
                self.commit(vec![op], now)?;
                Ok(Reply::Integer(1))
            }
            b"TTL" | b"PTTL" if n == 2 => {
                key(&a[1])?;
                let ttl = match self.get(&a[1], now) {
                    None => -2,
                    Some(Entry {
                        expires_at: None, ..
                    }) => -1,
                    Some(Entry {
                        expires_at: Some(t),
                        ..
                    }) => {
                        if command == b"TTL" {
                            (t - now) / 1000
                        } else {
                            t - now
                        }
                    }
                };
                Ok(Reply::Integer(ttl))
            }
            b"PERSIST" if n == 2 => {
                key(&a[1])?;
                match self.get(&a[1], now).cloned() {
                    Some(e) if e.expires_at.is_some() => {
                        self.commit(
                            vec![Mutation::Set(a[1].clone(), e.value.to_vec(), None)],
                            now,
                        )?;
                        Ok(Reply::Integer(1))
                    }
                    _ => Ok(Reply::Integer(0)),
                }
            }
            b"TYPE" if n == 2 => {
                key(&a[1])?;
                Ok(Reply::Simple(if self.get(&a[1], now).is_some() {
                    "string"
                } else {
                    "none"
                }))
            }
            b"DBSIZE" if n == 1 => {
                self.prune(now);
                Ok(Reply::Integer(self.entries.len() as i64))
            }
            b"FLUSHDB" if n == 1 => {
                self.commit(vec![Mutation::Clear], now)?;
                Ok(ok)
            }
            b"SAVE" if n == 1 => {
                self.compact(now)
                    .map_err(|e| err(&format!("MISCONF compaction failed: {e}")))?;
                Ok(ok)
            }
            b"INFO" if n == 1 => {
                self.prune(now);
                Ok(Reply::Bulk(Some(format!(
                    "# Server\r\nexpense_kv_version:0.1.0\r\n# Memory\r\nused_memory_estimate:{}\r\nmaxmemory:{}\r\n# Persistence\r\naof_bytes:{}\r\naof_failed:{}\r\n# Stats\r\nkeyspace_hits:{}\r\nkeyspace_misses:{}\r\n# Keyspace\r\ndb0:keys={}\r\n",
                    self.bytes, self.max_bytes, self.journal.bytes, u8::from(self.journal.failed), self.hits.load(Ordering::Relaxed), self.misses.load(Ordering::Relaxed), self.entries.len()
                ).into_bytes().into())))
            }
            b"SELECT" if n == 2 && a[1] == b"0" => Ok(ok),
            b"COMMAND" if n == 1 => Ok(Reply::Array(Vec::new())),
            b"CLIENT" if n == 4 && a[1].eq_ignore_ascii_case(b"SETINFO") => Ok(ok),
            _ => Err(err(
                "ERR unknown command, unsupported option, or wrong number of arguments",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static ID: AtomicU64 = AtomicU64::new(0);
    pub struct Temp(std::path::PathBuf);
    impl Temp {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "expense-kv-{}-{}",
                std::process::id(),
                ID.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn run(s: &mut Store, a: &[&str], t: i64) -> Reply {
        s.execute(
            &a.iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>(),
            t,
        )
        .0
    }

    #[test]
    fn durable_replies_are_identified_by_actual_wal_changes() {
        let temp = Temp::new();
        let mut s = Store::open(&temp.0, 4096, 1_000_000).unwrap();
        for (args, durable) in [
            (vec!["SET", "k", "value"], true),
            (vec!["GET", "k"], false),
            (vec!["SET", "k", "skip", "NX"], false),
            (vec!["DEL", "k"], true),
            (vec!["DEL", "k"], false),
        ] {
            let args: Vec<_> = args.into_iter().map(|s| s.as_bytes().to_vec()).collect();
            assert_eq!(s.execute(&args, now_ms()).1, durable);
        }
    }

    #[test]
    fn read_batches_bound_retained_values_and_preserve_snapshots_and_stats() {
        let temp = Temp::new();
        let mut s = Store::open(&temp.0, 4 * MAX_BULK, 1_000_000).unwrap();
        let t = now_ms();
        let value = "v".repeat(MAX_BULK);
        run(&mut s, &["SET", "k", &value], t);
        let replies = s.get_batch(&[b"k", b"k", b"k"], t);
        assert_eq!(replies.len(), 2);
        assert_eq!(s.hits.load(Ordering::Relaxed), 2);
        run(&mut s, &["SET", "k", "new", "PX", "1"], t);
        assert_eq!(replies[0], Reply::Bulk(Some(Arc::from(value.into_bytes()))));
        assert_eq!(
            s.get_batch(&[b"k", b"missing"], t + 1),
            vec![Reply::Bulk(None), Reply::Bulk(None)]
        );
        assert_eq!(s.misses.load(Ordering::Relaxed), 2);
        s.prune(t + 1);
        assert!(s.expirations.is_empty());
        assert_eq!(s.bytes, 0);
    }

    #[test]
    fn concurrent_readers_and_writers_preserve_values_expiry_and_counters() {
        use std::sync::{Barrier, RwLock};
        let temp = Temp::new();
        let t = now_ms();
        let mut s = Store::open(&temp.0, 4096, 1_000_000).unwrap();
        run(&mut s, &["SET", "counter", "0"], t);
        run(&mut s, &["SET", "expired", "hidden", "PX", "1"], t);
        let shared = Arc::new(RwLock::new(s));
        let barrier = Arc::new(Barrier::new(9));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let shared = Arc::clone(&shared);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..1000 {
                        let replies = shared
                            .read()
                            .unwrap()
                            .get_batch(&[b"counter", b"expired"], t + 2);
                        let Reply::Bulk(Some(value)) = &replies[0] else {
                            panic!("missing counter")
                        };
                        assert!((0..=50).contains(&int(value).unwrap()));
                        assert_eq!(replies[1], Reply::Bulk(None));
                        std::thread::yield_now();
                    }
                });
            }
            barrier.wait();
            for i in 1..=50 {
                assert_eq!(
                    run(&mut shared.write().unwrap(), &["INCR", "counter"], t + 2),
                    Reply::Integer(i)
                );
            }
        });
        let s = shared.read().unwrap();
        assert_eq!(s.hits.load(Ordering::Relaxed), 8000);
        assert_eq!(s.misses.load(Ordering::Relaxed), 8000);
    }

    #[test]
    fn encoded_batches_reuse_memory_and_release_oversized_buffers() {
        let temp = Temp::new();
        let mut s = Store::open(&temp.0, 4 * MAX_BULK, 10_000_000).unwrap();
        let t = now_ms();
        let value = "v".repeat(65536);
        run(&mut s, &["SET", "big", &value], t);
        let mut output = Vec::new();
        assert_eq!(
            s.encode_get_batch(&[b"big".as_slice(); 64], t, &mut output),
            3
        );
        let mut expected = b"$65536\r\n".to_vec();
        expected.extend(value.as_bytes());
        expected.extend(b"\r\n");
        assert_eq!(output, expected.repeat(3));
        assert!(output.capacity() <= GET_BATCH_BYTES);
        let pointer = output.as_ptr();
        let capacity = output.capacity();
        assert_eq!(
            s.encode_get_batch(&[b"big".as_slice(); 64], t, &mut output),
            3
        );
        assert_eq!(output.as_ptr(), pointer);
        assert_eq!(output.capacity(), capacity);
        s.encode_get_batch(&[b"missing", b"big", b"big", b"big"], t, &mut output);
        assert!(output.capacity() <= GET_BATCH_BYTES);

        let huge = "h".repeat(MAX_BULK);
        run(&mut s, &["SET", "huge", &huge], t);
        assert_eq!(s.encode_get_batch(&[b"huge", b"big"], t, &mut output), 1);
        assert_eq!(output.len(), MAX_BULK + 12);
        assert_eq!(s.encode_get_batch(&[b"big"], t, &mut output), 1);
        assert!(output.capacity() <= GET_BATCH_BYTES);
        assert_eq!(output, expected);
    }

    #[test]
    fn expiry_index_tracks_updates_deletes_and_memory_reclamation() {
        let temp = Temp::new();
        let mut s = Store::open(&temp.0, 4096, 1_000_000).unwrap();
        let t = now_ms();
        for ttl in 1..100 {
            assert_eq!(
                run(&mut s, &["SET", "k", "value", "PX", &ttl.to_string()], t),
                Reply::Simple("OK")
            );
            assert_eq!(s.expirations.len(), 1);
        }
        s.prune(t + 98);
        assert_eq!(s.entries.len(), 1);
        assert_eq!(run(&mut s, &["PERSIST", "k"], t), Reply::Integer(1));
        assert!(s.expirations.is_empty());
        s.prune(t + 1000);
        assert_eq!(
            run(&mut s, &["GET", "k"], t),
            Reply::Bulk(Some(Arc::from(b"value".as_slice())))
        );

        run(&mut s, &["PEXPIRE", "k", "1"], t);
        s.prune(t + 1);
        assert_eq!(s.bytes, 0);
        assert!(s.entries.is_empty());
        assert!(s.expirations.is_empty());

        run(&mut s, &["SET", "k", "value", "PX", "10"], t);
        run(&mut s, &["MSET", "k", "new"], t);
        assert!(s.expirations.is_empty());
        assert_eq!(s.bytes, cost(b"k", b"new", None));
        run(&mut s, &["PEXPIRE", "k", "10"], t);
        run(&mut s, &["DEL", "k"], t);
        assert!(s.expirations.is_empty());
        run(&mut s, &["SET", "k", "value", "PX", "10"], t);
        run(&mut s, &["FLUSHDB"], t);
        assert!(s.expirations.is_empty());
        assert_eq!(s.bytes, 0);

        s.max_bytes = 120;
        run(&mut s, &["SET", "k", "value", "PX", "1"], t);
        assert_eq!(
            run(&mut s, &["SET", "next", "value"], t + 1),
            Reply::Simple("OK")
        );
        assert!(s.expirations.is_empty());
        assert_eq!(s.bytes, cost(b"next", b"value", None));
    }

    #[test]
    fn failed_writes_leave_the_expiry_index_unchanged() {
        let temp = Temp::new();
        let mut s = Store::open(&temp.0, 4096, 1_000_000).unwrap();
        let t = now_ms();
        run(&mut s, &["SET", "k", "value", "PX", "100"], t);
        let before = s.bytes;
        s.journal.failed = true;
        for command in [vec!["SET", "k", "new"], vec!["DEL", "k"], vec!["FLUSHDB"]] {
            assert!(matches!(run(&mut s, &command, t), Reply::Error(_)));
            assert_eq!(s.bytes, before);
            assert_eq!(s.expirations, BTreeSet::from([(t + 100, b"k".to_vec())]));
        }
        s.prune(t + 100);
        assert_eq!(s.bytes, 0);
    }

    #[test]
    fn expiry_conditions_and_overflow_are_atomic() {
        let temp = Temp::new();
        let mut s = Store::open(&temp.0, 4096, 1_000_000).unwrap();
        let t = now_ms();
        assert_eq!(
            run(&mut s, &["SET", "k", "9", "PX", "50", "NX"], t),
            Reply::Simple("OK")
        );
        assert_eq!(
            run(&mut s, &["SET", "k", "bad", "NX"], t),
            Reply::Bulk(None)
        );
        assert_eq!(run(&mut s, &["INCR", "k"], t), Reply::Integer(10));
        assert_eq!(run(&mut s, &["PTTL", "k"], t + 49), Reply::Integer(1));
        assert_eq!(run(&mut s, &["GET", "k"], t + 50), Reply::Bulk(None));
        run(&mut s, &["SET", "n", "9223372036854775807"], t);
        assert!(matches!(run(&mut s, &["INCR", "n"], t), Reply::Error(_)));
        assert_eq!(
            run(&mut s, &["GET", "n"], t),
            Reply::Bulk(Some(Arc::from(b"9223372036854775807".as_slice())))
        );
        assert_eq!(
            run(&mut s, &["SET", "missing", "1", "XX"], t),
            Reply::Bulk(None)
        );
        assert_eq!(
            run(&mut s, &["SET", "n", "5", "XX"], t),
            Reply::Simple("OK")
        );
        assert_eq!(
            run(&mut s, &["GET", "n"], t),
            Reply::Bulk(Some(Arc::from(b"5".as_slice())))
        );
    }

    #[test]
    fn restart_compaction_lock_and_torn_tail() {
        use std::io::Write;
        let temp = Temp::new();
        let t = now_ms();
        {
            let mut s = Store::open(&temp.0, 4096, 1_000_000).unwrap();
            assert!(Store::open(&temp.0, 4096, 1_000_000).is_err());
            run(&mut s, &["MSET", "a", "1", "b", "2"], t);
            run(&mut s, &["SET", "expiring", "yes", "PX", "100000"], t);
            run(&mut s, &["DEL", "b"], t);
            s.compact(t).unwrap();
            run(&mut s, &["SET", "after-rewrite", "saved"], t);
        }
        let file = temp.0.join("data.aof");
        let len = std::fs::metadata(&file).unwrap().len();
        std::fs::OpenOptions::new()
            .append(true)
            .open(&file)
            .unwrap()
            .write_all(b"broken")
            .unwrap();
        let mut s = Store::open(&temp.0, 4096, 1_000_000).unwrap();
        assert_eq!(std::fs::metadata(&file).unwrap().len(), len);
        assert_eq!(
            run(&mut s, &["GET", "after-rewrite"], t),
            Reply::Bulk(Some(Arc::from(b"saved".as_slice())))
        );
        assert_eq!(run(&mut s, &["GET", "b"], t), Reply::Bulk(None));
        assert_eq!(
            run(&mut s, &["PTTL", "expiring"], t + 99999),
            Reply::Integer(1)
        );
        assert_eq!(
            run(&mut s, &["GET", "expiring"], t + 100000),
            Reply::Bulk(None)
        );
    }

    #[test]
    fn rejects_corruption_and_memory_overflow() {
        let temp = Temp::new();
        let t = now_ms();
        {
            let mut s = Store::open(&temp.0, 70, 1_000_000).unwrap();
            run(&mut s, &["SET", "a", "1"], t);
            assert!(matches!(
                run(&mut s, &["MSET", "a", "2", "b", "3"], t),
                Reply::Error(_)
            ));
            assert_eq!(
                run(&mut s, &["GET", "a"], t),
                Reply::Bulk(Some(Arc::from(b"1".as_slice())))
            );
        }
        let file = temp.0.join("data.aof");
        let mut bytes = std::fs::read(&file).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(file, bytes).unwrap();
        assert!(Store::open(&temp.0, 4096, 1_000_000).is_err());
    }

    #[test]
    fn every_partial_final_batch_recovers_only_complete_records() {
        let source = Temp::new();
        let t = now_ms();
        let prefix;
        {
            let mut s = Store::open(&source.0, 4096, 1_000_000).unwrap();
            run(&mut s, &["SET", "x", "original"], t);
            prefix = std::fs::metadata(source.0.join("data.aof")).unwrap().len() as usize;
            run(&mut s, &["MSET", "x", "changed", "y", "new"], t);
        }
        let data = std::fs::read(source.0.join("data.aof")).unwrap();
        for cut in prefix..data.len() {
            let target = Temp::new();
            std::fs::create_dir_all(&target.0).unwrap();
            std::fs::write(target.0.join("data.aof"), &data[..cut]).unwrap();
            {
                let mut s = Store::open(&target.0, 4096, 1_000_000).unwrap();
                assert_eq!(
                    run(&mut s, &["GET", "x"], t),
                    Reply::Bulk(Some(Arc::from(b"original".as_slice())))
                );
                assert_eq!(run(&mut s, &["GET", "y"], t), Reply::Bulk(None));
                assert_eq!(
                    run(&mut s, &["SET", "after-repair", "works"], t),
                    Reply::Simple("OK")
                );
            }
            let mut s = Store::open(&target.0, 4096, 1_000_000).unwrap();
            assert_eq!(
                run(&mut s, &["GET", "after-repair"], t),
                Reply::Bulk(Some(Arc::from(b"works".as_slice())))
            );
        }
    }

    #[test]
    fn maintenance_rewrites_obsolete_values_and_ignores_abandoned_temp_file() {
        let temp = Temp::new();
        let t = now_ms();
        {
            let mut s = Store::open(&temp.0, 4096, 100).unwrap();
            for _ in 0..50 {
                run(&mut s, &["SET", "same", "value"], t);
            }
            let before = s.journal.bytes;
            s.maintain();
            assert!(s.journal.bytes < before / 2);
        }
        std::fs::write(temp.0.join("rewrite.tmp"), b"interrupted rewrite").unwrap();
        let mut s = Store::open(&temp.0, 4096, 100).unwrap();
        assert_eq!(
            run(&mut s, &["GET", "same"], t),
            Reply::Bulk(Some(Arc::from(b"value".as_slice())))
        );
    }
}

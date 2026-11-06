mod journal;
mod protocol;
mod store;

use protocol::{Reply, buffered_command, read_command};
use std::env;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use store::{Store, now_ms};

struct Slot(Arc<AtomicUsize>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct Connection {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    flushed_at: Instant,
    output: Vec<u8>,
}

impl Connection {
    fn get_pipeline(&mut self, store: &RwLock<Store>) -> io::Result<bool> {
        let input = self.reader.buffer();
        let mut keys = Vec::new();
        let mut ends = Vec::new();
        let mut args = Vec::new();
        let mut consumed = 0;
        while keys.len() < 64 {
            let Ok(Some(size)) = buffered_command(&input[consumed..], &mut args) else {
                break;
            };
            if args.len() != 2 || !args[0].eq_ignore_ascii_case(b"GET") {
                break;
            }
            keys.push(args[1]);
            consumed += size;
            ends.push(consumed);
        }
        if keys.is_empty() {
            return Ok(false);
        }
        // Readers share a lock; socket reads and writes stay outside it.
        let count = store
            .read()
            .map_err(|_| io::Error::other("store poisoned"))?
            .encode_get_batch(&keys, now_ms(), &mut self.output);
        self.reader.consume(ends[count - 1]);
        self.writer.write_all(&self.output)?;
        self.flush()?;
        Ok(true)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.flushed_at = Instant::now();
        Ok(())
    }

    fn reply(&mut self, reply: &Reply, durable: bool) -> io::Result<()> {
        reply.write_to(&mut self.writer)?;
        // A pipeline of slow fsyncs must not hold earlier replies until the next socket read.
        if durable || self.flushed_at.elapsed() >= Duration::from_millis(1) {
            self.flush()?;
        }
        Ok(())
    }
}

impl Read for Connection {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let mut available = self.fill_buf()?;
        let n = available.read(out)?;
        self.consume(n);
        Ok(n)
    }
}

impl BufRead for Connection {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.reader.buffer().is_empty() {
            // Flush before a blocking read, including when a pipelined frame is partial.
            self.flush()?;
        }
        self.reader.fill_buf()
    }

    fn consume(&mut self, amount: usize) {
        self.reader.consume(amount);
    }
}

fn password_matches(a: &[u8], b: &[u8]) -> bool {
    let mut different = a.len() ^ b.len();
    for (i, byte) in b.iter().enumerate() {
        different |= usize::from(a.get(i).copied().unwrap_or(0) ^ byte);
    }
    different == 0
}

fn serve(
    stream: TcpStream,
    store: Arc<RwLock<Store>>,
    password: Option<Arc<Vec<u8>>>,
    timeout: Duration,
) -> io::Result<()> {
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.set_nodelay(true)?;
    let mut connection = Connection {
        reader: BufReader::new(stream.try_clone()?),
        writer: BufWriter::with_capacity(64 * 1024, stream),
        flushed_at: Instant::now(),
        output: Vec::new(),
    };
    let mut authenticated = password.is_none();
    loop {
        if authenticated {
            connection.fill_buf()?;
            if connection.get_pipeline(&store)? {
                continue;
            }
        }
        let args = match read_command(&mut connection) {
            Ok(Some(args)) => args,
            Ok(None) => return Ok(()),
            Err(e) => {
                if e.kind() == io::ErrorKind::InvalidData {
                    let _ =
                        Reply::Error(format!("ERR protocol: {e}")).write_to(&mut connection.writer);
                    let _ = connection.writer.flush();
                }
                return Err(e);
            }
        };
        let name = args[0].to_ascii_uppercase();
        let mut quit = false;
        let mut durable = false;
        let response = if name == b"AUTH" {
            authenticated = false;
            match (&password, args.len()) {
                (Some(expected), 2) if password_matches(&args[1], expected) => {
                    authenticated = true;
                    Reply::Simple("OK")
                }
                (None, _) => {
                    authenticated = true;
                    Reply::Error("ERR no password configured".into())
                }
                _ => Reply::Error("WRONGPASS invalid password or AUTH arguments".into()),
            }
        } else if !authenticated {
            Reply::Error("NOAUTH authentication required".into())
        } else if name == b"QUIT" && args.len() == 1 {
            quit = true;
            Reply::Simple("OK")
        } else {
            // Mutations are serialized; no network IO under the write lock.
            let (reply, persisted) = store
                .write()
                .map_err(|_| io::Error::other("store poisoned"))?
                .execute(&args, now_ms());
            durable = persisted;
            reply
        };
        connection.reply(&response, durable)?;
        if quit {
            return connection.flush();
        }
    }
}

fn setting<T: std::str::FromStr>(name: &str, default: &str) -> io::Result<T> {
    env::var(name)
        .unwrap_or_else(|_| default.into())
        .parse()
        .map_err(|_| io::Error::other(format!("invalid {name}")))
}

fn main() -> io::Result<()> {
    if env::args().skip(1).any(|a| a == "--help" || a == "-h") {
        println!(
            "expense-kv 0.1.0\nConfiguration via environment:\n  KV_BIND=127.0.0.1:6380\n  KV_DATA_DIR=./data\n  KV_PASSWORD=<optional password>\n  KV_MAX_BYTES=67108864\n  KV_MAX_CLIENTS=128\n  KV_TIMEOUT_SECONDS=10\n  KV_COMPACT_BYTES=16777216\nLinux/macOS; RESP2; DB 0 only. See README.md."
        );
        return Ok(());
    }
    if env::args().len() > 1 {
        return Err(io::Error::other("unknown argument; use --help"));
    }
    let bind: SocketAddr = setting("KV_BIND", "127.0.0.1:6380")?;
    let max_bytes = setting("KV_MAX_BYTES", "67108864")?;
    let max_clients: usize = setting("KV_MAX_CLIENTS", "128")?;
    let timeout: u64 = setting("KV_TIMEOUT_SECONDS", "10")?;
    let compact_bytes = setting("KV_COMPACT_BYTES", "16777216")?;
    if max_clients == 0
        || max_clients > 4096
        || max_bytes == 0
        || timeout == 0
        || compact_bytes == 0
    {
        return Err(io::Error::other(
            "limits must be positive; KV_MAX_CLIENTS <= 4096",
        ));
    }
    let password = env::var("KV_PASSWORD")
        .ok()
        .filter(|v| !v.is_empty())
        .map(|p| Arc::new(p.into_bytes()));
    if !bind.ip().is_loopback() && password.is_none() {
        return Err(io::Error::other(
            "non-loopback binding requires KV_PASSWORD",
        ));
    }
    let dir = env::var_os("KV_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("data"));
    let store = Arc::new(RwLock::new(Store::open(&dir, max_bytes, compact_bytes)?));
    let listener = TcpListener::bind(bind)?;
    let maintenance = Arc::clone(&store);
    thread::Builder::new()
        .name("expiry-and-compaction".into())
        .spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));
                match maintenance.write() {
                    Ok(mut s) => s.maintain(),
                    Err(_) => return,
                }
            }
        })?;
    let clients = Arc::new(AtomicUsize::new(0));
    println!("expense-kv listening on {}", listener.local_addr()?);
    io::stdout().flush()?;
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Accept failed: {e}");
                continue;
            }
        };
        // Only this accept loop increments; worker threads only decrement.
        if clients.load(Ordering::Relaxed) >= max_clients {
            let _ = stream.set_write_timeout(Some(Duration::from_millis(100)));
            let _ = Reply::Error("ERR max clients reached".into()).write_to(&mut stream);
            continue;
        }
        clients.fetch_add(1, Ordering::Relaxed);
        let slot = Slot(Arc::clone(&clients));
        let shared = Arc::clone(&store);
        let secret = password.clone();
        if let Err(e) = thread::Builder::new()
            .name("kv-client".into())
            .spawn(move || {
                let _slot = slot;
                // Routine disconnects/timeouts are expected; never log request data or secrets.
                let _ = serve(stream, shared, secret, Duration::from_secs(timeout));
            })
        {
            eprintln!("Cannot start client thread: {e}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Shutdown;

    fn with_client(password: Option<&[u8]>, test: impl FnOnce(&mut TcpStream)) -> io::Result<()> {
        static ID: AtomicUsize = AtomicUsize::new(0);
        let dir = env::temp_dir().join(format!(
            "expense-kv-network-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::Relaxed)
        ));
        let store = Arc::new(RwLock::new(Store::open(&dir, 1 << 20, 1 << 20)?));
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let mut client = TcpStream::connect(listener.local_addr()?)?;
        client.set_read_timeout(Some(Duration::from_secs(3)))?;
        client.set_write_timeout(Some(Duration::from_secs(3)))?;
        let password = password.map(|p| Arc::new(p.to_vec()));
        let worker = thread::spawn(move || {
            let (stream, _) = listener.accept()?;
            serve(stream, store, password, Duration::from_secs(3))
        });
        test(&mut client);
        let _ = client.shutdown(Shutdown::Both);
        let result = worker.join().unwrap();
        std::fs::remove_dir_all(dir)?;
        result
    }

    fn expect(client: &mut TcpStream, expected: &[u8]) {
        let mut actual = vec![0; expected.len()];
        client.read_exact(&mut actual).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn slow_pipeline_flushes_without_waiting_for_another_socket_read() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let (stream, _) = listener.accept().unwrap();
        let mut connection = Connection {
            reader: BufReader::new(stream.try_clone().unwrap()),
            writer: BufWriter::new(stream),
            flushed_at: Instant::now() - Duration::from_millis(2),
            output: Vec::new(),
        };
        connection.reply(&Reply::Simple("OK"), false).unwrap();
        expect(&mut client, b"+OK\r\n");
        connection.flushed_at = Instant::now();
        connection.reply(&Reply::Simple("OK"), true).unwrap();
        expect(&mut client, b"+OK\r\n");
    }

    #[test]
    fn flushes_before_waiting_for_partial_frame_and_handles_large_pipelines() {
        with_client(None, |client| {
            client
                .write_all(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$4\r\na")
                .unwrap();
            expect(client, b"+PONG\r\n");
            client.write_all(b"\0bc\r\n").unwrap();
            expect(client, b"$4\r\na\0bc\r\n");

            client
                .write_all(&b"*1\r\n$4\r\nPING\r\n".repeat(3000))
                .unwrap();
            expect(client, &b"+PONG\r\n".repeat(3000));

            let value = vec![0xff; 20000];
            client.write_all(b"*2\r\n$4\r\nECHO\r\n$20000\r\n").unwrap();
            client.write_all(&value).unwrap();
            client.write_all(b"\r\n").unwrap();
            expect(client, b"$20000\r\n");
            expect(client, &value);
            expect(client, b"\r\n");

            client.write_all(b"*1\r\n$4\r\nQUIT\r\n").unwrap();
            expect(client, b"+OK\r\n");
            assert_eq!(client.read(&mut [0]).unwrap(), 0);
        })
        .unwrap();
    }

    #[test]
    fn buffered_auth_and_protocol_errors_preserve_response_order() {
        let result = with_client(Some(b"secret"), |client| {
            client.write_all(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n*1\r\n$4\r\nPING\r\n!bad\r\n").unwrap();
            expect(client, b"-NOAUTH authentication required\r\n+OK\r\n+PONG\r\n-ERR protocol: expected RESP2 array\r\n");
        });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn get_batches_preserve_order_across_writes_and_partial_frames() {
        with_client(None, |client| {
            client.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n").unwrap();
            expect(client, b"+OK\r\n");
            client.write_all(&b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n".repeat(3000)).unwrap();
            expect(client, &b"$1\r\n1\r\n".repeat(3000));
            client.write_all(b"*2\r\n$3\r\nget\r\n$1\r\nk\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n2\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*2\r\n$3\r\nGET\r\n$1\r\n").unwrap();
            expect(client, b"$1\r\n1\r\n+OK\r\n$1\r\n2\r\n:1\r\n$-1\r\n");
            client.write_all(b"k\r\n").unwrap();
            expect(client, b"$-1\r\n");
            let value = vec![b'v'; 40000];
            client.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$40000\r\n").unwrap();
            client.write_all(&value).unwrap();
            client.write_all(b"\r\n").unwrap();
            expect(client, b"+OK\r\n");
            client.write_all(&b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n".repeat(100)).unwrap();
            for _ in 0..100 {
                expect(client, b"$40000\r\n");
                expect(client, &value);
                expect(client, b"\r\n");
            }
        }).unwrap();
    }

    #[test]
    fn failed_auth_never_enters_the_get_batch_path() {
        with_client(Some(b"secret"), |client| {
            client.write_all(b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$7\r\nprivate\r\n*2\r\n$4\r\nAUTH\r\n$3\r\nbad\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").unwrap();
            expect(client, b"+OK\r\n+OK\r\n-WRONGPASS invalid password or AUTH arguments\r\n-NOAUTH authentication required\r\n-NOAUTH authentication required\r\n");
        }).unwrap();
    }
}

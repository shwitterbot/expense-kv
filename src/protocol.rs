//! A bounded, binary-safe subset of RESP2. Requests are arrays of bulk strings.
use std::io::{self, BufRead, Read, Write};
use std::sync::Arc;

pub const MAX_ARGS: usize = 1024;
pub const MAX_BULK: usize = 1024 * 1024;
pub const MAX_FRAME: usize = 2 * 1024 * 1024;
pub const MAX_KEY: usize = 1024;

#[derive(Debug, PartialEq)]
pub enum Reply {
    Simple(&'static str),
    Error(String),
    Integer(i64),
    Bulk(Option<Arc<[u8]>>),
    Array(Vec<Reply>),
}

pub fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn line(reader: &mut impl BufRead) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let n = reader.take(66).read_until(b'\n', &mut buf)?;
    if n == 0 || !buf.ends_with(b"\r\n") || n > 64 {
        return Err(invalid("invalid or overlong RESP header"));
    }
    buf.truncate(n - 2);
    Ok(buf)
}

fn length(bytes: &[u8]) -> io::Result<usize> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(invalid("invalid RESP length"));
    }
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| invalid("RESP length overflow"))
}

// Parse only complete buffered frames; slices borrow the input without copying arguments.
pub fn buffered_command<'a>(
    input: &'a [u8],
    args: &mut Vec<&'a [u8]>,
) -> io::Result<Option<usize>> {
    fn header<'a>(input: &'a [u8], pos: &mut usize) -> io::Result<Option<&'a [u8]>> {
        let remaining = &input[*pos..];
        let Some(end) = remaining.iter().take(66).position(|b| *b == b'\n') else {
            if remaining.len() >= 66 {
                return Err(invalid("invalid or overlong RESP header"));
            }
            return Ok(None);
        };
        if end == 0 || end + 1 > 64 || remaining[end - 1] != b'\r' {
            return Err(invalid("invalid or overlong RESP header"));
        }
        *pos += end + 1;
        Ok(Some(&remaining[..end - 1]))
    }

    args.clear();
    let mut pos = 0;
    let Some(array) = header(input, &mut pos)? else {
        return Ok(None);
    };
    if array.first() != Some(&b'*') {
        return Err(invalid("expected RESP2 array"));
    }
    let count = length(&array[1..])?;
    if count == 0 || count > MAX_ARGS {
        return Err(invalid("invalid argument count"));
    }
    let mut total = 0usize;
    for _ in 0..count {
        let Some(bulk) = header(input, &mut pos)? else {
            return Ok(None);
        };
        if bulk.first() != Some(&b'$') {
            return Err(invalid("expected bulk string"));
        }
        let size = length(&bulk[1..])?;
        total = total
            .checked_add(size)
            .ok_or_else(|| invalid("frame overflow"))?;
        if size > MAX_BULK || total > MAX_FRAME {
            return Err(invalid("request exceeds size limit"));
        }
        if input.len() - pos < size + 2 {
            return Ok(None);
        }
        if input[pos + size..pos + size + 2] != *b"\r\n" {
            return Err(invalid("missing bulk terminator"));
        }
        args.push(&input[pos..pos + size]);
        pos += size + 2;
    }
    Ok(Some(pos))
}

pub fn read_command(reader: &mut impl BufRead) -> io::Result<Option<Vec<Vec<u8>>>> {
    // Only an EOF between frames is a clean disconnect.
    if reader.fill_buf()?.is_empty() {
        return Ok(None);
    }
    let header = line(reader)?;
    if header.first() != Some(&b'*') {
        return Err(invalid("expected RESP2 array"));
    }
    let count = length(&header[1..])?;
    if count == 0 || count > MAX_ARGS {
        return Err(invalid("invalid argument count"));
    }
    let mut args = Vec::with_capacity(count);
    let mut total = 0usize;
    for _ in 0..count {
        let header = line(reader)?;
        if header.first() != Some(&b'$') {
            return Err(invalid("expected bulk string"));
        }
        let size = length(&header[1..])?;
        total = total
            .checked_add(size)
            .ok_or_else(|| invalid("frame overflow"))?;
        if size > MAX_BULK || total > MAX_FRAME {
            return Err(invalid("request exceeds size limit"));
        }
        let mut value = vec![0; size];
        reader.read_exact(&mut value)?;
        let mut end = [0; 2];
        reader.read_exact(&mut end)?;
        if end != *b"\r\n" {
            return Err(invalid("missing bulk terminator"));
        }
        args.push(value);
    }
    Ok(Some(args))
}

impl Reply {
    pub fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        match self {
            Self::Simple(s) => write!(w, "+{s}\r\n"),
            Self::Error(s) => write!(w, "-{}\r\n", s.replace(['\r', '\n'], " ")),
            Self::Integer(n) => write!(w, ":{n}\r\n"),
            Self::Bulk(b) => write_bulk(w, b.as_deref()),
            Self::Array(items) => {
                write!(w, "*{}\r\n", items.len())?;
                for item in items {
                    item.write_to(w)?;
                }
                Ok(())
            }
        }
    }
}

pub fn write_bulk(w: &mut impl Write, value: Option<&[u8]>) -> io::Result<()> {
    match value {
        None => w.write_all(b"$-1\r\n"),
        Some(b) => {
            write!(w, "${}\r\n", b.len())?;
            w.write_all(b)?;
            w.write_all(b"\r\n")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn binary_and_pipeline() {
        let mut r = Cursor::new(b"*2\r\n$4\r\nECHO\r\n$4\r\na\0\r\n\r\n*1\r\n$4\r\nPING\r\n");
        assert_eq!(read_command(&mut r).unwrap().unwrap()[1], b"a\0\r\n");
        assert_eq!(read_command(&mut r).unwrap().unwrap()[0], b"PING");
        assert!(read_command(&mut r).unwrap().is_none());
    }

    #[test]
    fn buffered_parser_matches_streaming_parser_at_every_fragment_boundary() {
        let first = b"*2\r\n$3\r\nget\r\n$5\r\nx\0\r\nz\r\n";
        let mut args = Vec::new();
        for cut in 0..first.len() {
            assert_eq!(buffered_command(&first[..cut], &mut args).unwrap(), None);
        }
        let mut pipeline = first.to_vec();
        pipeline.extend(b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(
            buffered_command(&pipeline, &mut args).unwrap(),
            Some(first.len())
        );
        let streamed = read_command(&mut Cursor::new(&pipeline)).unwrap().unwrap();
        assert_eq!(args, streamed.iter().map(Vec::as_slice).collect::<Vec<_>>());
        assert_eq!(
            buffered_command(&pipeline[first.len()..], &mut args).unwrap(),
            Some(14)
        );
        assert_eq!(args, [b"PING"]);
    }

    #[test]
    fn buffered_parser_preserves_protocol_limits() {
        let mut args = Vec::new();
        for request in [
            b"*1025\r\n".as_slice(),
            b"*0\r\n",
            b"*1\r\n$1048577\r\n",
            b"*1\r\n$-1\r\n",
            b"*1\r\n$1\r\naXX",
            b"*1\n",
            b"*184467440737095516160\r\n",
            b"*1\r\n!1\r\na\r\n",
        ] {
            assert!(buffered_command(request, &mut args).is_err(), "{request:?}");
            assert!(read_command(&mut Cursor::new(request)).is_err());
        }
        assert!(buffered_command(&[b'*'; 66], &mut args).is_err());
        let mut request = b"*3\r\n".to_vec();
        for _ in 0..2 {
            request.extend(b"$1048576\r\n");
            request.resize(request.len() + MAX_BULK, b'x');
            request.extend(b"\r\n");
        }
        request.extend(b"$1\r\n");
        assert!(buffered_command(&request, &mut args).is_err());
    }

    #[test]
    fn rejects_unbounded_and_truncated_requests() {
        for request in [
            b"*99999999999\r\n".as_slice(),
            b"*1\r\n$999999999\r\n",
            b"*1\r\n$-1\r\n",
            b"*1\r\n$3\r\na",
            b"*1\r\n$1\r\naXX",
            b"*0\r\n",
            b"*1\r\n$1\r\n",
        ] {
            assert!(read_command(&mut Cursor::new(request)).is_err());
        }
    }
}

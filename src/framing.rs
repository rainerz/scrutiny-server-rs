/// SCRUTINY stream-datagram framing codec.
///
/// Wire format (server → client, client → server):
///   `<SCRUTINY size=HHHHHHHH flags=ch>` + <zlib-compressed payload> + <md5 of compressed>
///
/// Flags:
///   `c` = payload is zlib-compressed (Compression::fast / level 1)
///   `h` = 16-byte MD5 hash of the (compressed) payload appended after the data
///
/// Both directions use the same framing.
use std::io::{self, Read, Write};

use bytes::{Buf, BufMut, BytesMut};
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use md5::{Digest, Md5};
use tokio_util::codec::{Decoder, Encoder};

const HASH_SIZE: usize = 16;
const PREFIX: &[u8] = b"<SCRUTINY size=";

/// State saved between `decode()` calls once a valid header has been found but
/// the full payload has not arrived yet.
struct PendingFrame {
    /// Byte offset in the buffer where the payload starts (i.e. just after `>`).
    header_end: usize,
    data_size: usize,
    compressed: bool,
    use_hash: bool,
}

/// tokio codec that implements SCRUTINY datagram framing.
pub struct ScrutinyCodec {
    pending: Option<PendingFrame>,
}

impl ScrutinyCodec {
    pub fn new() -> Self {
        Self { pending: None }
    }
}

impl Decoder for ScrutinyCodec {
    type Item = serde_json::Value;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // ── Phase 1: find and parse the header ──────────────────────────────
        if self.pending.is_none() {
            // Find the header prefix in the buffer.
            let start = match find_subsequence(buf, PREFIX) {
                Some(pos) => pos,
                None => {
                    // Discard everything except the last (PREFIX.len()-1) bytes, which
                    // might be the beginning of a header that spans two reads.
                    let keep = PREFIX.len() - 1;
                    if buf.len() > keep {
                        buf.advance(buf.len() - keep);
                    }
                    return Ok(None);
                }
            };

            if start > 0 {
                buf.advance(start); // skip junk before header
            }

            // We need to find the closing '>' to know where the header ends.
            let gt = match buf.iter().position(|&b| b == b'>') {
                Some(p) => p,
                None => return Ok(None), // header not complete yet
            };

            let header_end = gt + 1;
            let header_str = match std::str::from_utf8(&buf[..header_end]) {
                Ok(s) => s,
                Err(_) => {
                    buf.advance(1);
                    return Ok(None);
                }
            };

            match parse_header(header_str, header_end) {
                Some(pf) => self.pending = Some(pf),
                None => {
                    // Not a valid SCRUTINY header – skip the '<' and retry.
                    buf.advance(1);
                    return Ok(None);
                }
            }
        }

        // ── Phase 2: wait until full payload (+ hash) has arrived ───────────
        let pf = self.pending.as_ref().unwrap();
        let hash_size = if pf.use_hash { HASH_SIZE } else { 0 };
        let total_needed = pf.header_end + pf.data_size + hash_size;

        if buf.len() < total_needed {
            return Ok(None);
        }

        // ── Phase 3: extract and verify ─────────────────────────────────────
        let header_end = pf.header_end;
        let data_size = pf.data_size;
        let compressed = pf.compressed;
        let use_hash = pf.use_hash;
        self.pending = None;

        let data_bytes = buf[header_end..header_end + data_size].to_vec();

        if use_hash {
            let expected = &buf[header_end + data_size..header_end + data_size + HASH_SIZE];
            let computed = Md5::digest(&data_bytes);
            if computed.as_slice() != expected {
                buf.advance(total_needed);
                return Err(io::Error::new(io::ErrorKind::InvalidData, "MD5 mismatch"));
            }
        }

        buf.advance(total_needed);

        // ── Phase 4: decompress + JSON parse ────────────────────────────────
        let payload = if compressed {
            let mut dec = ZlibDecoder::new(&data_bytes[..]);
            let mut out = Vec::new();
            dec.read_to_end(&mut out)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            out
        } else {
            data_bytes
        };

        let json: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(json))
    }
}

impl Encoder<serde_json::Value> for ScrutinyCodec {
    type Error = io::Error;

    fn encode(&mut self, item: serde_json::Value, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let json_bytes =
            serde_json::to_vec(&item).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // zlib compress (level 1 = fast, same as Python default)
        let mut enc = ZlibEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(&json_bytes)?;
        let compressed = enc.finish()?;

        // MD5 of compressed payload
        let hash = Md5::digest(&compressed);

        let header = format!("<SCRUTINY size={:x} flags=ch>", compressed.len());

        dst.reserve(header.len() + compressed.len() + HASH_SIZE);
        dst.put(header.as_bytes());
        dst.put(compressed.as_slice());
        dst.put(hash.as_slice());

        Ok(())
    }
}

// ── helpers ─────────────────────────────────────────────────────────────────

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn parse_header(s: &str, header_end: usize) -> Option<PendingFrame> {
    // Expected format: `<SCRUTINY size=HEX flags=FLAGS>`
    let s = s.strip_prefix("<SCRUTINY size=")?;
    let (size_hex, rest) = s.split_once(" flags=")?;
    let flags = rest.strip_suffix('>')?;

    let data_size = usize::from_str_radix(size_hex.trim(), 16).ok()?;
    let compressed = flags.contains('c');
    let use_hash = flags.contains('h');

    Some(PendingFrame { header_end, data_size, compressed, use_hash })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    fn round_trip(value: serde_json::Value) -> serde_json::Value {
        let mut codec = ScrutinyCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(value, &mut buf).unwrap();
        codec.decode(&mut buf).unwrap().unwrap()
    }

    #[test]
    fn simple_object() {
        let v = serde_json::json!({"cmd": "echo", "reqid": 1, "payload": "hello"});
        assert_eq!(round_trip(v.clone()), v);
    }

    #[test]
    fn incremental_delivery() {
        let mut codec = ScrutinyCodec::new();
        let mut encoded = BytesMut::new();
        codec.encode(serde_json::json!({"x": 42}), &mut encoded).unwrap();

        // Feed one byte at a time
        let mut partial = BytesMut::new();
        let mut result = None;
        for byte in encoded.iter() {
            partial.put_u8(*byte);
            if let Some(v) = codec.decode(&mut partial).unwrap() {
                result = Some(v);
                break;
            }
        }
        assert_eq!(result.unwrap()["x"], 42);
    }
}

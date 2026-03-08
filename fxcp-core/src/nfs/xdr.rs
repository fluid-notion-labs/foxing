// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/nfs/xdr.rs — Minimal XDR encoder/decoder for NFSv4.2

//! Zero-dependency XDR (External Data Representation) encoder and decoder.
//!
//! Implements the subset of XDR needed for NFSv4.2 compound RPCs:
//! big-endian integers, length-prefixed opaque data with 4-byte padding,
//! and UTF-8 strings.

/// XDR encoder that writes into a pre-allocated `Vec<u8>`.
pub struct XdrEncoder {
    buf: Vec<u8>,
}

impl XdrEncoder {
    /// Create a new encoder with the given initial capacity.
    pub fn new(capacity: usize) -> Self {
        Self { buf: Vec::with_capacity(capacity) }
    }

    /// Encode a 32-bit unsigned integer (big-endian).
    pub fn encode_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Encode a 32-bit signed integer (big-endian).
    pub fn encode_i32(&mut self, v: i32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Encode a 64-bit unsigned integer (big-endian, as two u32s).
    pub fn encode_u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Encode a 64-bit signed integer (big-endian).
    pub fn encode_i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Encode a boolean as a u32 (0 or 1).
    pub fn encode_bool(&mut self, v: bool) {
        self.encode_u32(if v { 1 } else { 0 });
    }

    /// Encode variable-length opaque data: u32 length + data + padding to 4-byte boundary.
    pub fn encode_opaque(&mut self, data: &[u8]) {
        self.encode_u32(data.len() as u32);
        self.buf.extend_from_slice(data);
        // Pad to 4-byte boundary
        let pad = (4 - (data.len() % 4)) % 4;
        for _ in 0..pad {
            self.buf.push(0);
        }
    }

    /// Encode fixed-length opaque data (no length prefix, just data + padding).
    pub fn encode_opaque_fixed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
        let pad = (4 - (data.len() % 4)) % 4;
        for _ in 0..pad {
            self.buf.push(0);
        }
    }

    /// Encode a UTF-8 string (same wire format as opaque).
    pub fn encode_string(&mut self, s: &str) {
        self.encode_opaque(s.as_bytes());
    }

    /// Consume the encoder and return the encoded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// Current encoded length in bytes.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Get a reference to the internal buffer.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
}

/// XDR decoder for parsing NFS4 compound replies.
pub struct XdrDecoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> XdrDecoder<'a> {
    /// Create a new decoder over the given data.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// Decode a 32-bit unsigned integer.
    pub fn decode_u32(&mut self) -> Result<u32, XdrError> {
        if self.pos + 4 > self.data.len() {
            return Err(XdrError::Truncated);
        }
        let v = u32::from_be_bytes([
            self.data[self.pos], self.data[self.pos + 1],
            self.data[self.pos + 2], self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    /// Decode a 32-bit signed integer.
    pub fn decode_i32(&mut self) -> Result<i32, XdrError> {
        Ok(self.decode_u32()? as i32)
    }

    /// Decode a 64-bit unsigned integer.
    pub fn decode_u64(&mut self) -> Result<u64, XdrError> {
        if self.pos + 8 > self.data.len() {
            return Err(XdrError::Truncated);
        }
        let v = u64::from_be_bytes([
            self.data[self.pos], self.data[self.pos + 1],
            self.data[self.pos + 2], self.data[self.pos + 3],
            self.data[self.pos + 4], self.data[self.pos + 5],
            self.data[self.pos + 6], self.data[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(v)
    }

    /// Decode variable-length opaque data (reads length, then data + padding).
    pub fn decode_opaque(&mut self) -> Result<&'a [u8], XdrError> {
        let len = self.decode_u32()? as usize;
        if self.pos + len > self.data.len() {
            return Err(XdrError::Truncated);
        }
        let data = &self.data[self.pos..self.pos + len];
        let padded = len + ((4 - (len % 4)) % 4);
        self.pos += padded;
        Ok(data)
    }

    /// Decode a fixed-length opaque block (no length prefix).
    pub fn decode_opaque_fixed(&mut self, len: usize) -> Result<&'a [u8], XdrError> {
        if self.pos + len > self.data.len() {
            return Err(XdrError::Truncated);
        }
        let data = &self.data[self.pos..self.pos + len];
        let padded = len + ((4 - (len % 4)) % 4);
        self.pos += padded;
        Ok(data)
    }

    /// Skip `n` bytes (with 4-byte alignment).
    pub fn skip(&mut self, n: usize) -> Result<(), XdrError> {
        let padded = n + ((4 - (n % 4)) % 4);
        if self.pos + padded > self.data.len() {
            return Err(XdrError::Truncated);
        }
        self.pos += padded;
        Ok(())
    }

    /// Skip raw bytes without alignment.
    pub fn skip_raw(&mut self, n: usize) -> Result<(), XdrError> {
        if self.pos + n > self.data.len() {
            return Err(XdrError::Truncated);
        }
        self.pos += n;
        Ok(())
    }

    /// Remaining bytes in the buffer.
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    /// Current position in the buffer.
    pub fn position(&self) -> usize {
        self.pos
    }
}

/// XDR decoding errors.
#[derive(Debug, Clone)]
pub enum XdrError {
    /// Not enough data remaining to decode the value.
    Truncated,
    /// Opaque length exceeds reasonable bounds.
    InvalidLength,
}

impl std::fmt::Display for XdrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "XDR: unexpected end of data"),
            Self::InvalidLength => write!(f, "XDR: invalid opaque length"),
        }
    }
}

impl std::error::Error for XdrError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u32_roundtrip() {
        let mut enc = XdrEncoder::new(64);
        enc.encode_u32(0);
        enc.encode_u32(1);
        enc.encode_u32(0xDEADBEEF);
        enc.encode_u32(u32::MAX);

        let bytes = enc.into_bytes();
        let mut dec = XdrDecoder::new(&bytes);
        assert_eq!(dec.decode_u32().unwrap(), 0);
        assert_eq!(dec.decode_u32().unwrap(), 1);
        assert_eq!(dec.decode_u32().unwrap(), 0xDEADBEEF);
        assert_eq!(dec.decode_u32().unwrap(), u32::MAX);
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn test_u64_roundtrip() {
        let mut enc = XdrEncoder::new(64);
        enc.encode_u64(0x0102030405060708);
        let bytes = enc.into_bytes();
        let mut dec = XdrDecoder::new(&bytes);
        assert_eq!(dec.decode_u64().unwrap(), 0x0102030405060708);
    }

    #[test]
    fn test_opaque_roundtrip() {
        let mut enc = XdrEncoder::new(64);
        enc.encode_opaque(b"hello");     // 5 bytes + 3 padding
        enc.encode_opaque(b"test");      // 4 bytes + 0 padding
        enc.encode_opaque(b"x");         // 1 byte + 3 padding
        enc.encode_opaque(b"");          // 0 bytes

        let bytes = enc.into_bytes();
        assert_eq!(bytes.len(), 4+8 + 4+4 + 4+4 + 4); // length+padded for each

        let mut dec = XdrDecoder::new(&bytes);
        assert_eq!(dec.decode_opaque().unwrap(), b"hello");
        assert_eq!(dec.decode_opaque().unwrap(), b"test");
        assert_eq!(dec.decode_opaque().unwrap(), b"x");
        assert_eq!(dec.decode_opaque().unwrap(), b"");
        assert_eq!(dec.remaining(), 0);
    }

    #[test]
    fn test_string_roundtrip() {
        let mut enc = XdrEncoder::new(64);
        enc.encode_string("foxing");
        let bytes = enc.into_bytes();
        let mut dec = XdrDecoder::new(&bytes);
        let s = dec.decode_opaque().unwrap();
        assert_eq!(std::str::from_utf8(s).unwrap(), "foxing");
    }

    #[test]
    fn test_bool_encode() {
        let mut enc = XdrEncoder::new(16);
        enc.encode_bool(true);
        enc.encode_bool(false);
        let bytes = enc.into_bytes();
        assert_eq!(&bytes, &[0, 0, 0, 1, 0, 0, 0, 0]);
    }

    #[test]
    fn test_decoder_truncated() {
        let data = [0u8; 3]; // Not enough for a u32
        let mut dec = XdrDecoder::new(&data);
        assert!(matches!(dec.decode_u32(), Err(XdrError::Truncated)));
    }

    #[test]
    fn test_padding_alignment() {
        // Opaque of length 1 should produce 4+1+3 = 8 bytes on wire
        let mut enc = XdrEncoder::new(16);
        enc.encode_opaque(&[0xFF]);
        let bytes = enc.into_bytes();
        assert_eq!(bytes.len(), 8);
        assert_eq!(&bytes, &[0, 0, 0, 1, 0xFF, 0, 0, 0]);
    }
}

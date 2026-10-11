//! A tiny binary codec: fixed-width little-endian integers and
//! length-prefixed byte strings. Hand-written so the on-disk and on-wire
//! formats are fully explained, with no dependency to trust.

use std::fmt;

/// A decoding failure. Decoding never panics on bad input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The input ended before a value was complete.
    Short,
    /// An enum tag had no meaning.
    BadTag(u8),
    /// A string was not valid UTF-8.
    BadUtf8,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Short => write!(f, "input ended early"),
            Error::BadTag(t) => write!(f, "unknown tag {t}"),
            Error::BadUtf8 => write!(f, "invalid UTF-8"),
        }
    }
}

impl std::error::Error for Error {}

/// Builds a byte buffer.
#[derive(Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    pub fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    pub fn opt_u64(&mut self, v: Option<u64>) {
        match v {
            None => self.u8(0),
            Some(x) => {
                self.u8(1);
                self.u64(x);
            }
        }
    }
    pub fn bytes(&mut self, b: &[u8]) {
        self.u32(u32::try_from(b.len()).expect("value over 4 GiB"));
        self.buf.extend_from_slice(b);
    }
    pub fn str(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }
    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

/// Reads values back out of a byte slice.
pub struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf }
    }
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if self.buf.len() < n {
            return Err(Error::Short);
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }
    pub fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn opt_u64(&mut self) -> Result<Option<u64>, Error> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            t => Err(Error::BadTag(t)),
        }
    }
    pub fn bytes(&mut self) -> Result<&'a [u8], Error> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    pub fn str(&mut self) -> Result<&'a str, Error> {
        std::str::from_utf8(self.bytes()?).map_err(|_| Error::BadUtf8)
    }
}

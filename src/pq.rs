//! Minimal Parquet writer: one row group, one PLAIN data page (v1) per column, uncompressed.
//! Supports exactly what the two output tables need: required/optional UTF8 strings,
//! required DATE (INT32) and required INT64. Metadata is Thrift compact protocol, written by hand.
use std::io::Write;

/// One column, already PLAIN-encoded.
pub enum Col {
    /// required UTF8: plain bytes (u32 LE length + bytes per value)
    Str(Vec<u8>),
    /// optional UTF8: plain bytes of the present values + one bool per row
    OptStr(Vec<u8>, Vec<bool>),
    /// required DATE (days since epoch)
    Date(Vec<i32>),
    /// required INT64
    I64(Vec<i64>),
}

#[inline]
pub fn push_str(buf: &mut Vec<u8>, s: &[u8]) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s);
}

// ------------------------------------------------ thrift compact protocol
struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}
const I32: u8 = 5;
const I64: u8 = 6;
const BIN: u8 = 8;
const LIST: u8 = 9;
const STRUCT: u8 = 12;
impl T {
    fn new() -> Self {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn fh(&mut self, id: i16, ty: u8) {
        let d = id - *self.last.last().unwrap();
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.fh(id, I32);
        self.zz(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.fh(id, I64);
        self.zz(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.fh(id, BIN);
        self.varint(v.len() as u64);
        self.b.extend_from_slice(v);
    }
    fn begin(&mut self, id: i16) {
        self.fh(id, STRUCT);
        self.last.push(0);
    }
    /// starts a struct that is a list element (no field header)
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, ety: u8, n: usize) {
        self.fh(id, LIST);
        if n < 15 {
            self.b.push(((n as u8) << 4) | ety);
        } else {
            self.b.push(0xF0 | ety);
            self.varint(n as u64);
        }
    }
}

/// RLE/bit-packed hybrid, bit width 1, as one bit-packed run (v1 page: 4-byte length prefix).
fn def_levels(present: &[bool]) -> Vec<u8> {
    let groups = present.len().div_ceil(8);
    let mut body = Vec::with_capacity(groups + 10);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        body.push((h as u8) | 0x80);
        h >>= 7;
    }
    body.push(h as u8);
    for g in 0..groups {
        let mut byte = 0u8;
        for k in 0..8 {
            if present.get(g * 8 + k).copied().unwrap_or(false) {
                byte |= 1 << k;
            }
        }
        body.push(byte);
    }
    let mut out = Vec::with_capacity(body.len() + 4);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Writes a Parquet file with the given (name, column) pairs, all of `rows` rows.
pub fn write(path: &std::path::Path, rows: usize, cols: Vec<(&str, Col)>) -> std::io::Result<()> {
    let f = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::with_capacity(1 << 20, f);
    w.write_all(b"PAR1")?;
    let mut off: u64 = 4;
    // (physical type, data page offset, chunk size)
    let mut metas: Vec<(i32, u64, u64)> = Vec::new();
    if rows > 0 {
        for (_, c) in &cols {
            let (ty, pre, data): (i32, Vec<u8>, &[u8]) = match c {
                Col::Str(b) => (6, Vec::new(), b),
                Col::OptStr(b, p) => (6, def_levels(p), b),
                Col::Date(v) => (1, Vec::new(), unsafe {
                    std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * 4)
                }),
                Col::I64(v) => (2, Vec::new(), unsafe {
                    std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * 8)
                }),
            };
            let size = (pre.len() + data.len()) as i32;
            let mut h = T::new();
            h.i32(1, 0); // DATA_PAGE
            h.i32(2, size);
            h.i32(3, size);
            h.begin(5);
            h.i32(1, rows as i32);
            h.i32(2, 0); // PLAIN
            h.i32(3, 3); // RLE
            h.i32(4, 3); // RLE
            h.end();
            h.b.push(0);
            w.write_all(&h.b)?;
            w.write_all(&pre)?;
            w.write_all(data)?;
            let total = h.b.len() as u64 + size as u64;
            metas.push((ty, off, total));
            off += total;
        }
    }
    // footer
    let mut t = T::new();
    t.i32(1, 1);
    t.list(2, STRUCT, cols.len() + 1);
    t.begin_elem();
    t.bin(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end();
    for (name, c) in &cols {
        t.begin_elem();
        let (ty, rep, conv) = match c {
            Col::Str(_) => (6, 0, Some(0)),
            Col::OptStr(..) => (6, 1, Some(0)),
            Col::Date(_) => (1, 0, Some(6)),
            Col::I64(_) => (2, 0, None),
        };
        t.i32(1, ty);
        t.i32(3, rep);
        t.bin(4, name.as_bytes());
        if let Some(cv) = conv {
            t.i32(6, cv);
            t.begin(10); // logicalType union
            t.begin(if cv == 0 { 1 } else { 6 }); // STRING / DATE
            t.end();
            t.end();
        }
        t.end();
    }
    t.i64(3, rows as i64);
    let nrg = if rows > 0 { 1 } else { 0 };
    t.list(4, STRUCT, nrg);
    if rows > 0 {
        t.begin_elem();
        t.list(1, STRUCT, cols.len());
        let mut total = 0i64;
        for ((name, _), &(ty, dpo, sz)) in cols.iter().zip(metas.iter()) {
            t.begin_elem();
            t.i64(2, dpo as i64);
            t.begin(3);
            t.i32(1, ty);
            t.list(2, I32, 2);
            t.zz(0); // PLAIN
            t.zz(3); // RLE
            t.list(3, BIN, 1);
            t.varint(name.len() as u64);
            t.b.extend_from_slice(name.as_bytes());
            t.i32(4, 0); // UNCOMPRESSED
            t.i64(5, rows as i64);
            t.i64(6, sz as i64);
            t.i64(7, sz as i64);
            t.i64(9, dpo as i64);
            t.end();
            t.end();
            total += sz as i64;
        }
        t.i64(2, total);
        t.i64(3, rows as i64);
        t.end();
    }
    t.bin(6, b"shopping-sessions");
    t.b.push(0);
    w.write_all(&t.b)?;
    w.write_all(&(t.b.len() as u32).to_le_bytes())?;
    w.write_all(b"PAR1")?;
    w.flush()
}

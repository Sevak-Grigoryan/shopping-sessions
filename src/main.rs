//! Shopping Sessions: native parallel pipeline (Rust).
//! Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.
//! Every rule below follows rules.md; comments name the rule they implement.

mod pq;
mod scan;
use memchr::memchr;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

type S = &'static [u8];

/// Big buffers ask the kernel for transparent huge pages: far fewer page faults and TLB misses.
struct HugeAlloc;
unsafe impl std::alloc::GlobalAlloc for HugeAlloc {
    #[inline]
    unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
        let p = std::alloc::System.alloc(l);
        if l.size() >= (4 << 20) && !p.is_null() {
            advise(p, l.size());
        }
        p
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
        std::alloc::System.dealloc(p, l)
    }
    #[inline]
    unsafe fn realloc(&self, p: *mut u8, l: std::alloc::Layout, n: usize) -> *mut u8 {
        let q = std::alloc::System.realloc(p, l, n);
        if n >= (4 << 20) && !q.is_null() {
            advise(q, n);
        }
        q
    }
}
#[cfg(not(target_os = "linux"))]
unsafe fn advise(_p: *mut u8, _size: usize) {}
#[cfg(target_os = "linux")]
unsafe fn advise(p: *mut u8, size: usize) {
    let a = ((p as usize) + (2 << 20) - 1) & !((2 << 20) - 1);
    let end = ((p as usize) + size) & !((2 << 20) - 1);
    if end > a {
        libc::madvise(a as *mut libc::c_void, end - a, libc::MADV_HUGEPAGE);
    }
}
#[global_allocator]
static GLOBAL: HugeAlloc = HugeAlloc;

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};
static T: [AtomicU64; 10] = [const { AtomicU64::new(0) }; 10];
const TN: [&str; 10] = ["ev-parse", "ck-parse", "-", "dd-table", "dd-route", "ss-group", "ss-sort", "ss-walk", "write-u", ""];
#[inline]
fn tick(i: usize, t: std::time::Instant) -> std::time::Instant {
    let n = std::time::Instant::now();
    T[i].fetch_add((n - t).as_nanos() as u64, Relaxed);
    n
}

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const NB_BITS: u32 = 10; // 1024 buckets
const NB: usize = 1 << NB_BITS;
const CHUNK: usize = 4 << 20;
/// session/dedup tasks per thread (more = better balance; each writes one user_daily file)
const GROUPS_PER_THREAD: usize = 4;

/// wyhash-style 64-bit hash (folded 128-bit multiplies). Strong enough that a person is
/// identified by the hash of their name (collision odds ~1e-7 for millions of names).
#[inline]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}
#[inline]
fn h64(b: &[u8]) -> u64 {
    const P0: u64 = 0xa076_1d64_78bd_642f;
    const P1: u64 = 0xe703_7ed1_a0b4_28db;
    const P2: u64 = 0x8ebc_6af0_9c88_c6e3;
    #[inline(always)]
    fn r8(b: &[u8], i: usize) -> u64 {
        u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
    }
    #[inline(always)]
    fn r4(b: &[u8], i: usize) -> u64 {
        u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as u64
    }
    let n = b.len();
    let mut h = P0 ^ (n as u64).wrapping_mul(P2);
    // (a, c) together cover every byte (overlapping reads, no copies)
    let (a, c) = if n <= 16 {
        if n >= 8 {
            (r8(b, 0), r8(b, n - 8))
        } else if n >= 4 {
            (r4(b, 0), r4(b, n - 4))
        } else if n > 0 {
            (((b[0] as u64) << 16) | ((b[n >> 1] as u64) << 8) | b[n - 1] as u64, 0)
        } else {
            (0, 0)
        }
    } else {
        let mut i = 0;
        while i + 16 < n {
            h = mum(r8(b, i) ^ P1, r8(b, i + 8) ^ h);
            i += 16;
        }
        (r8(b, n - 16), r8(b, n - 8))
    };
    h = mum(a ^ P1, c ^ h);
    mum(h ^ P1, P2 ^ n as u64)
}
#[inline]
fn bucket(h: u64) -> usize {
    (h >> (64 - NB_BITS)) as usize
}

// ---------------------------------------------------------------- files
/// A small whole file, read into memory that lives until exit.
fn load(path: &Path) -> S {
    Box::leak(std::fs::read(path).unwrap_or_else(|e| panic!("read {path:?}: {e}")).into_boxed_slice())
}

#[cfg(unix)]
fn read_at(f: &File, buf: &mut [u8], off: u64) {
    use std::os::unix::fs::FileExt;
    f.read_exact_at(buf, off).expect("read");
}
#[cfg(windows)]
fn read_at(f: &File, buf: &mut [u8], mut off: u64) {
    use std::os::windows::fs::FileExt;
    let mut done = 0;
    while done < buf.len() {
        let n = f.seek_read(&mut buf[done..], off).expect("read");
        assert!(n > 0, "short read");
        done += n;
        off += n as u64;
    }
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

/// A byte range [s, e) of one input file, read on demand by the task that parses it, so reading
/// is spread over all threads (many requests in flight) and overlaps with parsing. The chunk
/// owns every line that starts inside its range.
struct Range {
    file: usize,
    s: u64,
    e: u64,
    len: u64, // file length
    skip_header: bool,
}
impl Range {
    #[inline]
    fn size(&self) -> usize {
        (self.e - self.s) as usize
    }
    /// Reads the lines this chunk owns into memory that lives until exit.
    fn fetch(&self, files: &[File]) -> S {
        const OVER: u64 = 64 << 10; // read past e to finish the last line
        let start = self.s.saturating_sub(1); // byte s-1 tells whether a line starts at s
        let mut want = (self.e + OVER).min(self.len);
        loop {
            let n = (want - start) as usize;
            let mut buf: Vec<u8> = Vec::with_capacity(n);
            unsafe { buf.set_len(n) };
            read_at(&files[self.file], &mut buf, start);
            // first line start
            let first = if self.s == 0 {
                Some(0)
            } else {
                memchr(b'\n', &buf).map(|p| p + 1)
            };
            let first = match first {
                Some(p) => p,
                None if want == self.len => return &[],
                None => {
                    want = (want + 16 * OVER).min(self.len);
                    continue;
                }
            };
            if start + first as u64 >= self.e {
                return &[]; // no line starts in this chunk
            }
            // end: just past the first '\n' at or after byte e-1
            let end = if self.e == self.len {
                n
            } else {
                let from = (self.e - 1 - start) as usize;
                match memchr(b'\n', &buf[from..]) {
                    Some(p) => from + p + 1,
                    None if want == self.len => n,
                    None => {
                        want = (want + 16 * OVER).min(self.len);
                        continue;
                    }
                }
            };
            let buf: &'static [u8] = Box::leak(buf.into_boxed_slice());
            return &buf[first..end];
        }
    }
}

struct Chunk {
    data: S,
    skip_header: bool,
}

/// Reads all CSV files of a folder as chunks of about CHUNK bytes, each chunk read by its own
/// task (so one big file is read by all threads at once), biggest chunks first.
fn read_chunks(dir: &Path) -> Vec<Chunk> {
    let (files, ranges) = plan_chunks(&list_csv(dir));
    ranges.par_iter().map(|r| Chunk { data: r.fetch(&files), skip_header: r.skip_header }).collect()
}

/// Opens the files and cuts them into ranges of about CHUNK bytes.
fn plan_chunks(paths: &[PathBuf]) -> (Vec<File>, Vec<Range>) {
    let files: Vec<File> =
        paths.iter().map(|p| File::open(p).unwrap_or_else(|e| panic!("open {p:?}: {e}"))).collect();
    let mut out = Vec::new();
    for (i, f) in files.iter().enumerate() {
        let len = f.metadata().map(|m| m.len()).unwrap_or(0);
        let mut s = 0u64;
        while s < len {
            let e = (s + CHUNK as u64).min(len);
            out.push(Range { file: i, s, e, len, skip_header: s == 0 });
            s = e;
        }
    }
    // big chunks first for better load balance
    out.sort_by(|a, b| b.size().cmp(&a.size()));
    (files, out)
}

/// Reads a small whole file (header skipped) into rows of N fields.
fn read_small<const N: usize>(path: &Path) -> Vec<[S; N]> {
    let d = load(path);
    let mut rows = Vec::new();
    scan::scan::<N>(d, true, |f| rows.push(*f));
    rows
}

// ---------------------------------------------------------------- timestamps
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
#[inline]
fn leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Days since epoch for every valid calendar date 1900-01-01..2199-12-31 (-1 = impossible date),
/// indexed by (year-1900)*372 + (month-1)*31 + (day-1).
const Y0: i64 = 1900;
const NY: i64 = 300;
static DAYTAB: std::sync::OnceLock<Vec<i32>> = std::sync::OnceLock::new();
fn daytab() -> &'static [i32] {
    DAYTAB.get_or_init(|| {
        let mut t = vec![-1i32; (NY * 372) as usize];
        for y in Y0..Y0 + NY {
            for m in 1..=12 {
                let dim = match m {
                    1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                    4 | 6 | 9 | 11 => 30,
                    _ => {
                        if leap(y) {
                            29
                        } else {
                            28
                        }
                    }
                };
                for d in 1..=dim {
                    t[((y - Y0) * 372 + (m - 1) * 31 + (d - 1)) as usize] = days_from_civil(y, m, d) as i32;
                }
            }
        }
        t
    })
}

#[derive(Clone, Copy)]
pub struct TsCache {
    tab: &'static [i32],
}
impl TsCache {
    pub fn new() -> Self {
        TsCache { tab: daytab() }
    }
}

/// F1 / F2 / F3 -> epoch ms. None for every bad form.
fn parse_ts(f: &[u8]) -> Option<i64> {
    parse_ts_c(f, &mut TsCache::new())
}

#[inline(always)]
fn ld8(f: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(f[i..i + 8].try_into().unwrap())
}
/// Every byte selected by `m` is an ASCII digit.
#[inline(always)]
fn digits_at(x: u64, m: u64) -> bool {
    let x = (x & m) | (0x3030_3030_3030_3030 & !m); // non-selected bytes become '0'
    ((x & 0xF0F0_F0F0_F0F0_F0F0) | (((x.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0) >> 4))
        == 0x3333_3333_3333_3333
}
/// 8 ASCII digits (first digit in the lowest byte) -> value.
#[inline(always)]
fn swar8(x: u64) -> u64 {
    let mut v = x.wrapping_sub(0x3030_3030_3030_3030);
    v = (v.wrapping_mul(10).wrapping_add(v >> 8)) & 0x00FF_00FF_00FF_00FF;
    v = (v.wrapping_mul(100).wrapping_add(v >> 16)) & 0x0000_FFFF_0000_FFFF;
    (v.wrapping_mul(10000).wrapping_add(v >> 32)) & 0xFFFF_FFFF
}
/// F1 / F2 / F3 -> epoch ms. None for every bad form (see rules.md).
#[inline]
fn parse_ts_c(f: &[u8], cache: &mut TsCache) -> Option<i64> {
    let n = f.len();
    if n != 20 && n != 25 {
        return parse_f3_fast(f);
    }
    // n >= 20: unchecked 8-byte loads are in bounds
    let p = f.as_ptr();
    let (a, b, t) = unsafe {
        (
            (p as *const u64).read_unaligned(),        // "YYYY-MM-"
            (p.add(8) as *const u64).read_unaligned(), // "DDTHH:MM"
            (p.add(12) as *const u64).read_unaligned(), // "HH:MM:SS" (bytes 12..20)
        )
    };
    // separators '-' at 4,7; 'T' at 10; ':' at 13,16
    if a & 0xFF00_00FF_0000_0000 != 0x2D00_002D_0000_0000
        || b & 0x0000_FF00_00FF_0000 != 0x0000_3A00_0054_0000
        || (t >> 32) & 0xFF != 0x3A
    {
        return parse_f3_fast(f); // e.g. an all-digit F3 of length 20 or 25
    }
    // digits: a bytes 0-3,5,6; b bytes 0,1,3,4,6,7; t bytes 5,6 (= f[17], f[18])
    if !digits_at(a, 0x00FF_FF00_FFFF_FFFF) || !digits_at(b, 0xFFFF_00FF_FF00_FFFF) || !digits_at(t, 0x00FF_FF00_0000_0000) {
        return None;
    }
    // non-digit bytes are replaced by '0' first so the subtraction never borrows across bytes
    let z = |x: u64, m: u64| ((x & m) | (0x3030_3030_3030_3030 & !m)).wrapping_sub(0x3030_3030_3030_3030);
    let da = z(a, 0x00FF_FF00_FFFF_FFFF);
    let db = z(b, 0xFFFF_00FF_FF00_FFFF);
    let dt = z(t, 0x00FF_FF00_0000_0000);
    let g = |x: u64, i: u32| ((x >> (8 * i)) & 0xff) as i64;
    let y = g(da, 0) * 1000 + g(da, 1) * 100 + g(da, 2) * 10 + g(da, 3);
    let mo = g(da, 5) * 10 + g(da, 6);
    let d = g(db, 0) * 10 + g(db, 1);
    let h = g(db, 3) * 10 + g(db, 4);
    let mi = g(db, 6) * 10 + g(db, 7);
    let sec = g(dt, 5) * 10 + g(dt, 6);
    let off_ms = if n == 20 {
        if f[19] != b'Z' {
            return None;
        }
        0
    } else {
        // f[19] sign, f[20..22] HH, f[22] ':', f[23..25] MM
        let o = unsafe { (p.add(17) as *const u64).read_unaligned() }; // "SS+HH:MM"
        if (o >> 40) & 0xFF != 0x3A || !digits_at(o, 0xFFFF_00FF_FF00_0000) {
            return None;
        }
        let sign = f[19];
        if sign != b'+' && sign != b'-' {
            return None;
        }
        let dz = z(o, 0xFFFF_00FF_FF00_0000);
        let oh = g(dz, 3) * 10 + g(dz, 4);
        let om = g(dz, 6) * 10 + g(dz, 7);
        if oh > 23 || om > 59 {
            return None;
        }
        let v = (oh * 3600 + om * 60) * 1000;
        if sign == b'+' {
            v
        } else {
            -v
        }
    };
    if h > 23 || mi > 59 || sec > 59 || mo.wrapping_sub(1) as u64 > 11 || d.wrapping_sub(1) as u64 > 30 {
        return None;
    }
    let days = if (y - Y0) as u64 >= NY as u64 {
        let dim = match mo {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => {
                if leap(y) {
                    29
                } else {
                    28
                }
            }
        };
        if d > dim {
            return None;
        }
        days_from_civil(y, mo, d)
    } else {
        let v = unsafe { *cache.tab.get_unchecked(((y - Y0) * 372 + (mo - 1) * 31 + (d - 1)) as usize) };
        if v < 0 {
            return None;
        }
        v as i64
    };
    Some((days * 86_400 + h * 3600 + mi * 60 + sec) * 1000 - off_ms)
}

/// F3: ASCII digits only, leading zeros allowed; a value too big for i64 is just out of the window.
#[inline]
fn parse_f3_fast(f: &[u8]) -> Option<i64> {
    if f.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < f.len() && f[i] == b'0' {
        i += 1;
    }
    let g = &f[i..];
    if g.len() > 19 {
        return if g.iter().all(|b| b.is_ascii_digit()) && f.iter().all(|b| b.is_ascii_digit()) { None } else { None };
    }
    let mut v: u64 = 0;
    let mut k = 0;
    while k + 8 <= g.len() {
        let x = ld8(g, k);
        if !digits_at(x, u64::MAX) {
            return None;
        }
        v = v * 100_000_000 + swar8(x);
        k += 8;
    }
    while k < g.len() {
        let b = g[k];
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u64;
        k += 1;
    }
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

/// Digits only, 1..=10,000,000.
#[inline]
fn parse_price(f: &[u8]) -> Option<u32> {
    if f.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in f {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

// ---------------------------------------------------------------- links
/// A byte string packed into one u64: pointer (48 bits) | length (16 bits).
#[inline]
fn pack(s: S) -> u64 {
    assert!(s.len() < 65536, "field too long");
    ((s.as_ptr() as u64) << 16) | s.len() as u64
}
#[inline]
fn unpack(v: u64) -> S {
    unsafe { std::slice::from_raw_parts((v >> 16) as *const u8, (v & 0xffff) as usize) }
}

/// `pre` followed by 1..=8 lowercase base-36 digits without a leading zero -> that number.
/// Exactly the IDs the generator writes; the map is one-to-one, so the string can be rebuilt.
#[inline(always)]
fn b36(s: &[u8], pre: u8) -> Option<u64> {
    let n = s.len();
    if !(2..=9).contains(&n) || s[0] != pre || (s[1] == b'0' && n > 2) {
        return None;
    }
    // branch-free digits: a table lookup per byte, one validity check at the end
    let mut v = 0u64;
    let mut bad = 0u8;
    for &c in &s[1..] {
        let d = T36[c as usize];
        bad |= d;
        v = v * 36 + d as u64;
    }
    if bad >= 0x80 {
        None
    } else {
        Some(v)
    }
}
/// Base-36 digit value of a byte ('0'-'9', 'a'-'z'), 0xFF otherwise.
static T36: [u8; 256] = {
    let mut t = [0xFFu8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = i as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[b'a' as usize + i] = 10 + i as u8;
        i += 1;
    }
    t
};

/// A user ID -> its key. A canonical ID (see b36) is its exact number (< 2^42), so its name can
/// be rebuilt from the key alone. Any other ID is its 64-bit hash with the top bit set and bit 0
/// clear (so key + 1 never wraps); its name is kept in a side table.
#[inline(always)]
fn ukey(s: &[u8]) -> u64 {
    match b36(s, b'u') {
        Some(v) => v,
        None => (h64(s) | (1 << 63)) & !1,
    }
}
#[inline(always)]
fn hashed(k: u64) -> bool {
    k >> 63 != 0
}
/// Writes the name of a canonical key.
fn ustr(k: u64, out: &mut Vec<u8>) {
    const D: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = [0u8; 16];
    let mut at = buf.len();
    let mut v = k;
    loop {
        at -= 1;
        buf[at] = D[(v % 36) as usize];
        v /= 36;
        if v == 0 {
            break;
        }
    }
    out.push(b'u');
    out.extend_from_slice(&buf[at..]);
}
/// A well-mixed 64-bit value of a key (keys of canonical IDs are not random in their high bits).
#[inline(always)]
fn mix(k: u64) -> u64 {
    mum(k ^ 0xa076_1d64_78bd_642f, 0xe703_7ed1_a0b4_28db)
}

/// Concurrent hash table of every linked ID: user key -> person key (the key of the
/// byte-wise smallest ID of its person).
pub struct LinkMap {
    mask: usize,
    slots: Vec<[AtomicU64; 2]>, // key + 1 (0 = empty), person key
    own: Vec<AtomicU64>,        // packed ID string of each slot (build only)
    filter: Vec<AtomicU64>,
    fbits: u32,
}
impl LinkMap {
    fn with_capacity(n: usize) -> Self {
        let cap = ((n * 5) / 4).next_power_of_two().max(16);
        let fbits = ((n * 8).next_power_of_two().max(1 << 12).trailing_zeros()).min(30);
        LinkMap {
            mask: cap - 1,
            slots: (0..cap).into_par_iter().map(|_| [AtomicU64::new(0), AtomicU64::new(0)]).collect(),
            own: (0..cap).into_par_iter().map(|_| AtomicU64::new(0)).collect(),
            filter: (0..(1usize << fbits) / 64).into_par_iter().map(|_| AtomicU64::new(0)).collect(),
            fbits,
        }
    }
    #[inline(always)]
    fn fbit(&self, m: u64) -> usize {
        (m >> (64 - self.fbits)) as usize
    }
    /// Inserts (if new) and returns the slot index.
    fn insert(&self, uk: u64, own: u64) -> u32 {
        let k = uk + 1;
        let m = mix(k);
        let mut pos = (m as usize) & self.mask;
        loop {
            let s = &self.slots[pos];
            match s[0].compare_exchange(0, k, Relaxed, Relaxed) {
                Ok(_) => {
                    self.own[pos].store(own, Relaxed);
                    let f = self.fbit(m);
                    self.filter[f >> 6].fetch_or(1 << (f & 63), Relaxed);
                    return pos as u32;
                }
                Err(x) if x == k => return pos as u32,
                Err(_) => pos = (pos + 1) & self.mask,
            }
        }
    }
    #[inline(always)]
    pub fn prefetch(&self, uk: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            let m = mix(uk + 1);
            let f = self.fbit(m);
            _mm_prefetch(self.filter.as_ptr().add(f >> 6) as *const i8, _MM_HINT_T0);
            _mm_prefetch(self.slots.as_ptr().add((m as usize) & self.mask) as *const i8, _MM_HINT_T0);
        }
    }
    /// The person key of a user key (the user key itself when the ID is in no link).
    #[inline(always)]
    pub fn person(&self, uk: u64) -> u64 {
        let k = uk + 1;
        let m = mix(k);
        let f = self.fbit(m);
        if self.filter[f >> 6].load(Relaxed) & (1 << (f & 63)) == 0 {
            return uk;
        }
        let mut pos = (m as usize) & self.mask;
        loop {
            let s = &self.slots[pos];
            let x = s[0].load(Relaxed);
            if x == 0 {
                return uk;
            }
            if x == k {
                return s[1].load(Relaxed);
            }
            pos = (pos + 1) & self.mask;
        }
    }
}

/// Concurrent union-find over all link lines, on the hash table's slot indices (CAS links the
/// larger root under the smaller). Every linked ID then gets its person: the key of the
/// byte-wise smallest ID of its component. Also returns the names of persons whose key is a hash.
fn build_links(dir: &Path) -> (LinkMap, Vec<(u64, u64)>) {
    let chunks = read_chunks(dir);
    let pairs: Vec<(u64, u64, S, S)> = chunks
        .par_iter()
        .flat_map_iter(|c| {
            let mut v = Vec::new();
            scan::scan::<2>(c.data, c.skip_header, |f| {
                if !f[0].is_empty() && !f[1].is_empty() && f[0] != f[1] {
                    v.push((ukey(f[0]), ukey(f[1]), f[0], f[1]));
                }
            });
            v
        })
        .collect();
    let map = LinkMap::with_capacity(pairs.len() * 2 + 16); // upper bound on distinct IDs
    let ip: Vec<(u32, u32)> =
        pairs.par_iter().map(|p| (map.insert(p.0, pack(p.2)), map.insert(p.1, pack(p.3)))).collect();
    let cap = map.mask + 1;
    let parent: Vec<AtomicU32> = (0..cap as u32).into_par_iter().map(AtomicU32::new).collect();
    let find = |mut x: u32| -> u32 {
        loop {
            let p = parent[x as usize].load(Relaxed);
            if p == x {
                return x;
            }
            let g = parent[p as usize].load(Relaxed);
            if g != p {
                let _ = parent[x as usize].compare_exchange(p, g, Relaxed, Relaxed);
            }
            x = g;
        }
    };
    ip.par_iter().for_each(|&(a0, b0)| {
        let (mut a, mut b) = (a0, b0);
        loop {
            a = find(a);
            b = find(b);
            if a == b {
                return;
            }
            let (lo, hi) = if a < b { (a, b) } else { (b, a) };
            if parent[hi as usize].compare_exchange(hi, lo, Relaxed, Relaxed).is_ok() {
                return;
            }
        }
    });
    let used = |i: usize| map.slots[i][0].load(Relaxed) != 0;
    let own = |i: usize| unpack(map.own[i].load(Relaxed));
    let roots: Vec<u32> = (0..cap as u32).into_par_iter().map(|i| if used(i as usize) { find(i) } else { i }).collect();
    let best: Vec<AtomicU32> = (0..cap as u32).into_par_iter().map(AtomicU32::new).collect();
    (0..cap).into_par_iter().filter(|&i| used(i)).for_each(|i| {
        let r = roots[i] as usize;
        let mut cur = best[r].load(Relaxed);
        while own(i) < own(cur as usize) {
            match best[r].compare_exchange(cur, i as u32, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(x) => cur = x,
            }
        }
    });
    (0..cap).into_par_iter().filter(|&i| used(i)).for_each(|i| {
        let b = best[roots[i] as usize].load(Relaxed) as usize;
        map.slots[i][1].store(map.slots[b][0].load(Relaxed) - 1, Relaxed);
    });
    // names of hashed person keys: each component's best slot
    let names: Vec<(u64, u64)> = (0..cap)
        .into_par_iter()
        .filter(|&i| used(i) && roots[i] as usize == i)
        .filter_map(|r| {
            let b = best[r].load(Relaxed) as usize;
            let k = map.slots[b][0].load(Relaxed) - 1;
            hashed(k).then(|| (k, map.own[b].load(Relaxed)))
        })
        .collect();
    (map, names)
}

/// Product -> category rank. IDs of the canonical form `p<digits>` (no leading zero) are looked
/// up in a dense array (cache resident); anything else goes through the exact string map.
struct ProdIndex {
    dense: Vec<u8>, // 255 = not in products.csv
    other: StrMap<u16>,
    unknown: u16,
}
impl ProdIndex {
    fn new(products: &[[S; 2]], cat_idx: &FxHashMap<S, u16>, unknown: u16) -> Self {
        let mut dense: Vec<u8> = Vec::new();
        let mut rest: Vec<(S, u16)> = Vec::new();
        for r in products {
            let c = cat_idx[&r[1]];
            match Self::canon(r[0]) {
                Some(n) if n < 50_000_000 => {
                    if dense.len() <= n {
                        dense.resize(n + 1, 255);
                    }
                    dense[n] = c as u8;
                }
                _ => rest.push((r[0], c)),
            }
        }
        let m = rest.len();
        ProdIndex { dense, other: StrMap::new(rest, m), unknown }
    }
    /// `p` followed by 1..=9 ASCII digits without a leading zero (or exactly "p0").
    #[inline(always)]
    fn canon(s: &[u8]) -> Option<usize> {
        let n = s.len();
        if n < 2 || n > 10 || s[0] != b'p' || (s[1] == b'0' && n > 2) {
            return None;
        }
        let mut v = 0usize;
        for &b in &s[1..] {
            if !b.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (b - b'0') as usize;
        }
        Some(v)
    }
    #[inline(always)]
    fn get(&self, s: &[u8]) -> u16 {
        match Self::canon(s) {
            Some(n) => match self.dense.get(n) {
                Some(&c) if c != 255 => c as u16,
                _ => self.unknown,
            },
            None => self.other.get(s).unwrap_or(self.unknown),
        }
    }
}

/// Campaign -> index. Canonical `c_<digits>` IDs (no leading zero) use a dense array; anything
/// else goes through the exact string map.
struct CampIndex {
    dense: Vec<u32>, // u32::MAX = not a campaign
    other: StrMap<u32>,
}
impl CampIndex {
    fn new(campaigns: &[[S; 2]]) -> Self {
        let mut dense: Vec<u32> = Vec::new();
        let mut rest: Vec<(S, u32)> = Vec::new();
        for (i, r) in campaigns.iter().enumerate() {
            match Self::canon(r[0]) {
                Some(n) if n < 10_000_000 => {
                    if dense.len() <= n {
                        dense.resize(n + 1, u32::MAX);
                    }
                    dense[n] = i as u32;
                }
                _ => rest.push((r[0], i as u32)),
            }
        }
        let m = rest.len();
        CampIndex { dense, other: StrMap::new(rest, m) }
    }
    #[inline(always)]
    fn canon(s: &[u8]) -> Option<usize> {
        let n = s.len();
        if n < 3 || n > 11 || s[0] != b'c' || s[1] != b'_' || (s[2] == b'0' && n > 3) {
            return None;
        }
        let mut v = 0usize;
        for &b in &s[2..] {
            if !b.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (b - b'0') as usize;
        }
        Some(v)
    }
    #[inline(always)]
    fn get(&self, s: &[u8]) -> Option<u32> {
        match Self::canon(s) {
            Some(n) => match self.dense.get(n) {
                Some(&c) if c != u32::MAX => Some(c),
                _ => None,
            },
            None => self.other.get(s),
        }
    }
}

// ---------------------------------------------------------------- records
/// One structurally valid event copy, bucketed by event_id hash for dedup (32 bytes).
#[derive(Clone, Copy)]
#[repr(C)]
struct Ev {
    eh: u64,     // hash of event_id (identity for dedup)
    pk: u64,     // person key (valid copies only)
    ingest: u32, // ingest_ts in whole seconds, biased (see ingest_key); u32::MAX = unparseable
    ts: u32,     // ms since window_start; u32::MAX = invalid or late copy
    pc: u32,     // price << 8 | category rank (price 0 unless a purchase)
    _pad: u32,
}

/// Dedup table slot: event_id hash, smallest ingest seen, index of that copy in the bucket.
#[derive(Clone, Copy)]
struct DSlot {
    eh: u64,
    ing: u32,
    idx: u32, // u32::MAX = empty
}

/// ingest_ts (always F1, so whole seconds) -> u32 that keeps the order of any two copies.
#[inline(always)]
fn ingest_key(ms: Option<i64>, ws: i64) -> u32 {
    match ms {
        Some(v) => (v.div_euclid(1000) - ws.div_euclid(1000) + (1i64 << 31)).clamp(0, u32::MAX as i64 - 1) as u32,
        None => u32::MAX,
    }
}

/// n slots of plain data in fresh, pre-faulted memory (one mmap + populate instead of page
/// faults one by one). Never unmapped: the process exits right after writing its output.
#[cfg(not(target_os = "linux"))]
fn populated<T: Copy + 'static>(n: usize) -> &'static mut [T] {
    let bytes = (n * std::mem::size_of::<T>()).max(4096);
    let l = std::alloc::Layout::from_size_align(bytes, 64).unwrap();
    unsafe { std::slice::from_raw_parts_mut(std::alloc::alloc_zeroed(l) as *mut T, n) }
}
#[cfg(target_os = "linux")]
fn populated<T: Copy + 'static>(n: usize) -> &'static mut [T] {
    let bytes = (n * std::mem::size_of::<T>()).max(4096);
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED, "mmap failed");
        libc::madvise(p, bytes, libc::MADV_HUGEPAGE);
        if libc::madvise(p, bytes, 23 /* MADV_POPULATE_WRITE */) != 0 {
            // older kernels: touch every page
            let b = p as *mut u8;
            let mut i = 0;
            while i < bytes {
                std::ptr::write_volatile(b.add(i), 0);
                i += 4096;
            }
        }
        std::slice::from_raw_parts_mut(p as *mut T, n)
    }
}

/// NB buckets carved out of one big allocation (huge-page friendly: few page faults), with a
/// per-bucket overflow Vec in case a bucket outgrows its share.
struct Buckets<T: Copy + 'static> {
    arena: &'static mut [T],
    cap: usize,
    len: Vec<u32>,
    over: Vec<Vec<T>>,
}
impl<T: Copy + 'static> Buckets<T> {
    fn new(total_est: usize) -> Self {
        let cap = total_est.div_ceil(NB) + 64;
        let arena = populated::<T>(cap * NB);
        Buckets { arena, cap, len: vec![0; NB], over: (0..NB).map(|_| Vec::new()).collect() }
    }
    #[inline(always)]
    fn push(&mut self, b: usize, x: T) {
        let l = self.len[b] as usize;
        if l < self.cap {
            unsafe { *self.arena.get_unchecked_mut(b * self.cap + l) = x };
        } else {
            self.over[b].push(x);
        }
        self.len[b] += 1;
    }
    #[inline]
    fn count(&self, b: usize) -> usize {
        self.len[b] as usize
    }
    #[inline]
    fn parts(&self, b: usize) -> (&[T], &[T]) {
        let l = (self.len[b] as usize).min(self.cap);
        (&self.arena[b * self.cap..b * self.cap + l], &self.over[b])
    }
}

/// Every element of bucket b across all parts, in a fixed order (no copy).
#[inline(always)]
fn each<T: Copy + 'static>(parts: &[Buckets<T>], b: usize, mut f: impl FnMut(&T)) {
    for p in parts {
        let (a, o) = p.parts(b);
        for x in a {
            f(x);
        }
        for x in o {
            f(x);
        }
    }
}

/// Per-bucket map: person key -> dense index (open addressing, load <= 1/2).
struct PersonTable {
    keys: Vec<u64>, // person key + 1; 0 = empty
    vals: Vec<u32>,
    mask: usize,
    n: usize,
}
impl PersonTable {
    fn new() -> Self {
        PersonTable { keys: vec![0; 1024], vals: vec![0; 1024], mask: 1023, n: 0 }
    }
    #[inline(always)]
    fn pos(k: u64, mask: usize) -> usize {
        (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & mask
    }
    fn reset(&mut self, nev: usize) {
        let want = (nev / 4).max(512).next_power_of_two() * 2;
        if self.keys.len() != want {
            self.keys = vec![0; want];
            self.vals = vec![0; want];
            self.mask = want - 1;
        } else {
            self.keys.fill(0);
        }
        self.n = 0;
    }
    #[inline(always)]
    fn idx(&mut self, pk: u64) -> u32 {
        if 2 * (self.n + 1) > self.keys.len() {
            self.grow();
        }
        let k = pk + 1;
        let mut p = Self::pos(k, self.mask);
        loop {
            let x = unsafe { *self.keys.get_unchecked(p) };
            if x == k {
                return unsafe { *self.vals.get_unchecked(p) };
            }
            if x == 0 {
                self.keys[p] = k;
                self.vals[p] = self.n as u32;
                self.n += 1;
                return self.n as u32 - 1;
            }
            p = (p + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let (ok, ov) = (std::mem::take(&mut self.keys), std::mem::take(&mut self.vals));
        let cap = ok.len() * 2;
        self.keys = vec![0; cap];
        self.vals = vec![0; cap];
        self.mask = cap - 1;
        for (k, v) in ok.into_iter().zip(ov) {
            if k != 0 {
                let mut p = Self::pos(k, self.mask);
                while self.keys[p] != 0 {
                    p = (p + 1) & self.mask;
                }
                self.keys[p] = k;
                self.vals[p] = v;
            }
        }
    }
    #[inline(always)]
    fn get(&self, pk: u64) -> u32 {
        let k = pk + 1;
        let mut p = Self::pos(k, self.mask);
        loop {
            let x = unsafe { *self.keys.get_unchecked(p) };
            if x == k {
                return unsafe { *self.vals.get_unchecked(p) };
            }
            if x == 0 {
                return u32::MAX;
            }
            p = (p + 1) & self.mask;
        }
    }
}

/// A surviving event, bucketed by person (16 bytes).
#[derive(Clone, Copy)]
struct PE {
    pk: u64,
    ts: u32,
    pc: u32,
}

/// A valid click, bucketed by person.
#[derive(Clone, Copy)]
struct PC {
    pk: u64,
    cid: u64, // packed click_id
    ts: u32,  // ms since window_start - 7 days
    camp: u32,
}

#[inline(always)]
fn pbucket(pk: u64) -> usize {
    bucket(mix(pk))
}

/// user_daily rows of one task, already in Parquet PLAIN layout.
struct UserCols {
    name: Vec<u8>,
    day: Vec<i32>,
    sessions: Vec<i64>,
    events: Vec<i64>,
    revenue: Vec<i64>,
    top: Vec<u8>,
    has_top: Vec<bool>,
}
impl UserCols {
    fn new() -> Self {
        UserCols {
            name: Vec::new(),
            day: Vec::new(),
            sessions: Vec::new(),
            events: Vec::new(),
            revenue: Vec::new(),
            top: Vec::new(),
            has_top: Vec::new(),
        }
    }
    #[inline]
    fn push(&mut self, name: &[u8], day: i32, s: u64, e: u64, r: u64, top: Option<&[u8]>) {
        pq::push_str(&mut self.name, name);
        self.day.push(day);
        self.sessions.push(s as i64);
        self.events.push(e as i64);
        self.revenue.push(r as i64);
        match top {
            Some(t) => {
                pq::push_str(&mut self.top, t);
                self.has_top.push(true);
            }
            None => self.has_top.push(false),
        }
    }
    fn write(self, path: &Path) {
        let n = self.day.len();
        pq::write(
            path,
            n,
            vec![
                ("user_id", pq::Col::Str(self.name)),
                ("day", pq::Col::Date(self.day)),
                ("sessions", pq::Col::I64(self.sessions)),
                ("events", pq::Col::I64(self.events)),
                ("revenue_cents", pq::Col::I64(self.revenue)),
                ("top_category", pq::Col::OptStr(self.top, self.has_top)),
            ],
        )
        .expect("write user_daily");
    }
}

fn main() {
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let mut pool = rayon::ThreadPoolBuilder::new();
    if ncpu > 0 {
        pool = pool.num_threads(ncpu);
    }
    pool.build_global().ok();
    let t0 = std::time::Instant::now();
    let timing = true; // a few stderr lines; shows where the time goes in the scorer's log
    since_exec();
    let lap = |what: &str| {
        if timing {
            eprintln!("{:>8.3}s {what}", t0.elapsed().as_secs_f64());
        }
    };

    // manifest
    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let grab = |key: &str| -> i64 {
        let i = man.find(key).expect("key") + key.len();
        let rest = &man[i..];
        let q1 = rest.find('"').unwrap();
        let rest = &rest[q1 + 1..];
        let q2 = rest.find('"').unwrap();
        parse_ts(rest[..q2].as_bytes()).expect("window ts")
    };
    let ws = grab("\"window_start\"");
    let we = grab("\"window_end\"");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let cws = ws - WEEK; // click window start
    assert!(we - cws < u32::MAX as i64, "window too long");

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");

    // small tables first (events need the products)
    let (products, campaigns) = rayon::join(
        || {
            std::fs::create_dir_all(&ud).unwrap();
            std::fs::create_dir_all(&cd).unwrap();
            read_small::<2>(&data.join("products.csv"))
        },
        || read_small::<2>(&data.join("campaigns.csv")),
    );
    // categories, ranked byte-wise so the smaller index wins ties
    let mut cats: Vec<S> = products.iter().map(|r| r[1]).collect();
    cats.push(b"unknown");
    cats.sort_unstable();
    cats.dedup();
    assert!(cats.len() < 255, "too many categories");
    let cat_idx: FxHashMap<S, u16> = cats.iter().enumerate().map(|(i, &c)| (c, i as u16)).collect();
    let unknown = cat_idx[&b"unknown"[..]];
    let prod = ProdIndex::new(&products, &cat_idx, unknown);
    let ncat = cats.len();
    let camp_idx = CampIndex::new(&campaigns);
    let ncamp = campaigns.len();
    let organic = ncamp as u32;
    lap("small tables");

    let nthreads = rayon::current_num_threads().max(1);
    let groups = |n: usize| -> Vec<Vec<usize>> {
        let g = (nthreads * 3).min(n.max(1));
        let mut v = vec![Vec::new(); g];
        for i in 0..n {
            let r = i / g;
            let k = if r % 2 == 0 { i % g } else { g - 1 - i % g };
            v[k].push(i);
        }
        v
    };
    const BATCH: usize = 32;

    // ------------------------------------------------ links, in parallel with mapping the inputs
    let ((links, mut names), (ev_chunks, ck_chunks)) = rayon::join(
        || build_links(&data.join("links")),
        || rayon::join(|| read_chunks(&data.join("events")), || read_chunks(&data.join("clicks"))),
    );
    lap("links + read");
    let ev_groups = groups(ev_chunks.len());
    let ck_groups = groups(ck_chunks.len());

    // ------------------------------------------------ parse events (bucket by event_id) and clicks (bucket by person)
    // Identity is resolved here: the link-table misses hide behind the parsing work.
    let (ev_parts, ck_parts) = rayon::join(
        || {
            ev_groups
                .par_iter()
                .map(|grp| {
                    let t0 = std::time::Instant::now();
                    let bytes: usize = grp.iter().map(|&i| ev_chunks[i].data.len()).sum();
                    let mut acc: Buckets<Ev> = Buckets::new(bytes / 70);
                    let mut side: Vec<(u64, u64)> = Vec::new(); // names of hashed user keys
                    let mut c1 = TsCache::new();
                    let mut pend: [Ev; BATCH] = [Ev { eh: 0, pk: 0, ingest: 0, ts: 0, pc: 0, _pad: 0 }; BATCH];
                    let mut np = 0usize;
                    let flush = |pend: &mut [Ev], acc: &mut Buckets<Ev>| {
                        for e in pend.iter_mut() {
                            e.pk = links.person(e.pk);
                            acc.push(bucket(e.eh), *e);
                        }
                    };
                    for &ci in grp {
                        let c = &ev_chunks[ci];
                        scan::scan::<7>(c.data, c.skip_header, |f| {
                            if f[0].is_empty() {
                                return;
                            }
                            let ingest = parse_ts_c(f[3], &mut c1);
                            let mut e =
                                Ev { eh: h64(f[0]), pk: 0, ingest: ingest_key(ingest, ws), ts: u32::MAX, pc: 0, _pad: 0 };
                            'v: {
                                if f[1].is_empty() || f[5].is_empty() {
                                    break 'v;
                                }
                                let ts = match parse_ts_c(f[2], &mut c1) {
                                    Some(t) if t >= ws && t < we => t,
                                    _ => break 'v,
                                };
                                let t = f[4];
                                let price = if t.len() == 4 && {
                                    let w = u32::from_le_bytes([t[0], t[1], t[2], t[3]]);
                                    w == u32::from_le_bytes(*b"view") || w == u32::from_le_bytes(*b"cart")
                                } {
                                    0
                                } else if t.len() == 8
                                    && u64::from_le_bytes([t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7]])
                                        == u64::from_le_bytes(*b"purchase")
                                {
                                    match parse_price(f[6]) {
                                        Some(p) => p,
                                        None => break 'v,
                                    }
                                } else {
                                    break 'v;
                                };
                                if ingest.unwrap_or(i64::MAX) - ts > DAY {
                                    break 'v; // late: dropped if this copy is the kept one
                                }
                                let cat = prod.get(f[5]);
                                e.ts = (ts - ws) as u32;
                                e.pc = (price << 8) | cat as u32;
                                let uk = ukey(f[1]);
                                if hashed(uk) {
                                    side.push((uk, pack(f[1])));
                                }
                                links.prefetch(uk);
                                e.pk = uk;
                                pend[np] = e;
                                np += 1;
                                if np == BATCH {
                                    flush(&mut pend, &mut acc);
                                    np = 0;
                                }
                                return;
                            }
                            acc.push(bucket(e.eh), e);
                        });
                    }
                    flush(&mut pend[..np], &mut acc);
                    tick(0, t0);
                    (acc, side)
                })
                .collect::<Vec<_>>()
        },
        || {
            // clicks: no dedup needed (copies are identical, so the credit and assist sets are the same)
            ck_groups
                .par_iter()
                .map(|grp| {
                    let t0 = std::time::Instant::now();
                    let bytes: usize = grp.iter().map(|&i| ck_chunks[i].data.len()).sum();
                    let mut acc: Buckets<PC> = Buckets::new(bytes / 43);
                    let mut c1 = TsCache::new();
                    let mut pend: [PC; BATCH] = [PC { pk: 0, cid: 0, ts: 0, camp: 0 }; BATCH];
                    let mut np = 0usize;
                    let flush = |pend: &mut [PC], acc: &mut Buckets<PC>| {
                        for c in pend.iter_mut() {
                            c.pk = links.person(c.pk);
                            acc.push(pbucket(c.pk), *c);
                        }
                    };
                    for &ci in grp {
                        let c = &ck_chunks[ci];
                        scan::scan::<4>(c.data, c.skip_header, |f| {
                            if f[0].is_empty() || f[1].is_empty() {
                                return;
                            }
                            let ts = match parse_ts_c(f[3], &mut c1) {
                                Some(t) if t >= cws && t < we => t,
                                _ => return,
                            };
                            let camp = match camp_idx.get(f[2]) {
                                Some(c) => c,
                                None => return,
                            };
                            let uk = ukey(f[1]);
                            links.prefetch(uk);
                            pend[np] = PC { pk: uk, cid: pack(f[0]), ts: (ts - cws) as u32, camp };
                            np += 1;
                            if np == BATCH {
                                flush(&mut pend, &mut acc);
                                np = 0;
                            }
                        });
                    }
                    flush(&mut pend[..np], &mut acc);
                    tick(1, t0);
                    acc
                })
                .collect::<Vec<_>>()
        },
    );
    let (ev_parts, sides): (Vec<Buckets<Ev>>, Vec<Vec<(u64, u64)>>) = ev_parts.into_iter().unzip();
    for s in sides {
        names.extend(s);
    }
    lap("parse events + clicks");

    // ------------------------------------------------ dedup per event bucket -> person buckets
    let bgroups: Vec<Vec<usize>> = {
        let g = (nthreads * GROUPS_PER_THREAD).min(NB);
        (0..g).map(|k| (k..NB).step_by(g).collect()).collect()
    };
    let pe_parts: Vec<Buckets<PE>> = bgroups
        .par_iter()
        .map(|bs| {
            let mine: usize = bs.iter().map(|&b| ev_parts.iter().map(|p| p.count(b)).sum::<usize>()).sum();
            let mut acc: Buckets<PE> = Buckets::new(mine);
            let mut table: Vec<DSlot> = Vec::new();
            let mut win: Vec<u64> = Vec::new();
            for &b in bs {
                let t = std::time::Instant::now();
                let n: usize = ev_parts.iter().map(|p| p.count(b)).sum();
                let cap = (n * 2).next_power_of_two().max(16);
                table.clear();
                table.resize(cap, DSlot { eh: 0, ing: 0, idx: u32::MAX });
                let mask = cap - 1;
                let mut i = 0u32;
                each(&ev_parts, b, |e| {
                    let mut pos = (e.eh as usize) & mask;
                    loop {
                        let s = unsafe { table.get_unchecked_mut(pos) };
                        if s.idx == u32::MAX {
                            *s = DSlot { eh: e.eh, ing: e.ingest, idx: i };
                            break;
                        }
                        if s.eh == e.eh {
                            if e.ingest < s.ing {
                                s.ing = e.ingest;
                                s.idx = i;
                            }
                            break;
                        }
                        pos = (pos + 1) & mask;
                    }
                    i += 1;
                });
                let t = tick(3, t);
                win.clear();
                win.resize(n.div_ceil(64), 0);
                for s in &table {
                    if s.idx != u32::MAX {
                        win[(s.idx >> 6) as usize] |= 1u64 << (s.idx & 63);
                    }
                }
                let mut i = 0usize;
                each(&ev_parts, b, |e| {
                    if e.ts != u32::MAX && win[i >> 6] & (1u64 << (i & 63)) != 0 {
                        acc.push(pbucket(e.pk), PE { pk: e.pk, ts: e.ts, pc: e.pc });
                    }
                    i += 1;
                });
                tick(4, t);
            }
            acc
        })
        .collect();
    std::mem::forget(ev_parts);
    lap("dedup");
    let names: FxHashMap<u64, u64> = names.into_iter().collect();

    // ------------------------------------------------ per person bucket: sessions, credit, assists
    let width = ncamp + 1;
    // each task writes its own user_daily part as soon as it is done (no barrier before writing)
    let results: Vec<Vec<[u64; 4]>> = bgroups
        .par_iter()
        .enumerate()
        .map(|(g, bs)| {
            let mut camp = vec![[0u64; 4]; width * ndays];
            let mut wcnt = vec![0u32; width];
            let mut wpos = vec![0u32; width];
            let mut active: Vec<u32> = Vec::new();
            let mut rows = UserCols::new();
            let mut catsum = vec![0u64; ncat];
            let mut touched: Vec<u16> = Vec::new();
            let mut evs: Vec<u64> = Vec::new(); // ts << 32 | pc, grouped by person
            let mut pkeys: Vec<u64> = Vec::new(); // person key of each dense person index
            let mut cks: Vec<PC> = Vec::new();
            let mut ptab = PersonTable::new();
            let mut pidx: Vec<u32> = Vec::new();
            let mut cidx: Vec<u32> = Vec::new();
            let mut pcount: Vec<u32> = Vec::new();
            let mut ccount: Vec<u32> = Vec::new();
            let mut pos: Vec<u32> = Vec::new();
            let mut nbuf: Vec<u8> = Vec::new();
            for &b in bs {
                let t = std::time::Instant::now();
                // group by person with a counting sort on a dense per-bucket person index (read
                // straight from the parts), then order each person's events by ts and clicks by (ts, click_id)
                let nev: usize = pe_parts.iter().map(|p| p.count(b)).sum();
                ptab.reset(nev);
                pidx.clear();
                pkeys.clear();
                each(&pe_parts, b, |e| {
                    let x = ptab.idx(e.pk);
                    if x as usize == pkeys.len() {
                        pkeys.push(e.pk);
                    }
                    pidx.push(x);
                });
                let np = ptab.n;
                pcount.clear();
                pcount.resize(np + 1, 0);
                for &x in &pidx {
                    pcount[x as usize + 1] += 1;
                }
                for x in 0..np {
                    pcount[x + 1] += pcount[x];
                }
                evs.clear();
                evs.reserve(nev);
                unsafe { evs.set_len(nev) };
                pos.clear();
                pos.extend_from_slice(&pcount[..np]);
                let mut i = 0usize;
                each(&pe_parts, b, |e| {
                    let k = unsafe { pos.get_unchecked_mut(*pidx.get_unchecked(i) as usize) };
                    unsafe { *evs.get_unchecked_mut(*k as usize) = ((e.ts as u64) << 32) | e.pc as u64 };
                    *k += 1;
                    i += 1;
                });
                let t = tick(5, t);
                for x in 0..np {
                    let r = &mut evs[pcount[x] as usize..pcount[x + 1] as usize];
                    if r.len() > 1 {
                        sort_u64(r);
                    }
                }
                // clicks of persons without surviving events can never credit anything: skip them
                cidx.clear();
                ccount.clear();
                ccount.resize(np + 1, 0);
                each(&ck_parts, b, |c| {
                    let x = ptab.get(c.pk);
                    cidx.push(x);
                    if x != u32::MAX {
                        ccount[x as usize + 1] += 1;
                    }
                });
                for x in 0..np {
                    ccount[x + 1] += ccount[x];
                }
                let nck = ccount[np] as usize;
                cks.clear();
                cks.reserve(nck);
                unsafe { cks.set_len(nck) };
                pos.clear();
                pos.extend_from_slice(&ccount[..np]);
                let mut i = 0usize;
                each(&ck_parts, b, |c| {
                    let x = unsafe { *cidx.get_unchecked(i) };
                    if x != u32::MAX {
                        let k = unsafe { pos.get_unchecked_mut(x as usize) };
                        unsafe { *cks.get_unchecked_mut(*k as usize) = *c };
                        *k += 1;
                    }
                    i += 1;
                });
                for x in 0..np {
                    let r = &mut cks[ccount[x] as usize..ccount[x + 1] as usize];
                    if r.len() > 1 {
                        r.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| unpack(a.cid).cmp(unpack(b.cid))));
                    }
                }
                let t = tick(6, t);
                for x in 0..np {
                    let (i, j) = (pcount[x] as usize, pcount[x + 1] as usize);
                    let pk = pkeys[x];
                    nbuf.clear();
                    let name: &[u8] = if hashed(pk) {
                        unpack(*names.get(&pk).expect("name of a hashed person key"))
                    } else {
                        ustr(pk, &mut nbuf);
                        &nbuf
                    };
                    let pc = &cks[ccount[x] as usize..ccount[x + 1] as usize];
                    let (mut lo, mut hi) = (0usize, 0usize); // clicks with s-WEEK <= ts <= s
                    // current user_daily row: day, sessions, events, revenue
                    let mut cur_day = i32::MIN;
                    let (mut us, mut ue, mut ur) = (0u64, 0u64, 0u64);
                    let mut k = i;
                    while k < j {
                        // one session; times relative to window_start
                        let start = (evs[k] >> 32) as i64;
                        let day = (ws + start).div_euclid(DAY);
                        let di = (day - day0) as usize;
                        let d32 = day as i32;
                        if d32 != cur_day {
                            if cur_day != i32::MIN {
                                let top = top_of(&mut catsum, &mut touched).map(|c| cats[c as usize]);
                                rows.push(name, cur_day, us, ue, ur, top);
                            }
                            cur_day = d32;
                            us = 0;
                            ue = 0;
                            ur = 0;
                        }
                        let mut last = start;
                        let mut n_ev = 0u64;
                        let mut rev = 0u64;
                        let mut m = k;
                        while m < j && (evs[m] >> 32) as i64 - last <= GAP {
                            let e = evs[m];
                            last = (e >> 32) as i64;
                            n_ev += 1;
                            let price = (e as u32 >> 8) as u64;
                            if price > 0 {
                                let c = (e & 0xff) as usize;
                                rev += price;
                                if catsum[c] == 0 {
                                    touched.push(c as u16);
                                }
                                catsum[c] += price;
                            }
                            m += 1;
                        }
                        // attribution: click ts relative to window_start is c.ts - WEEK.
                        // [lo, hi) = clicks with s-WEEK <= ts <= s; `active` = their distinct campaigns.
                        let s_c = start + WEEK; // session start in click time base
                        while hi < pc.len() && (pc[hi].ts as i64) <= s_c {
                            let c = pc[hi].camp as usize;
                            if wcnt[c] == 0 {
                                wpos[c] = active.len() as u32;
                                active.push(c as u32);
                            }
                            wcnt[c] += 1;
                            hi += 1;
                        }
                        while lo < hi && (pc[lo].ts as i64) < s_c - WEEK {
                            let c = pc[lo].camp as usize;
                            wcnt[c] -= 1;
                            if wcnt[c] == 0 {
                                let p = wpos[c] as usize;
                                let last = active.pop().unwrap();
                                if p < active.len() {
                                    active[p] = last;
                                    wpos[last as usize] = p as u32;
                                }
                            }
                            lo += 1;
                        }
                        let credited = if lo < hi {
                            let t = pc[hi - 1].ts;
                            let mut q = hi - 1;
                            while q > lo && pc[q - 1].ts == t {
                                q -= 1;
                            }
                            for &c in &active {
                                camp[c as usize * ndays + di][3] += 1;
                            }
                            pc[q].camp
                        } else {
                            organic
                        };
                        let cell = &mut camp[credited as usize * ndays + di];
                        cell[0] += 1;
                        if rev > 0 {
                            cell[1] += 1;
                        }
                        cell[2] += rev;
                        us += 1;
                        ue += n_ev;
                        ur += rev;
                        k = m;
                    }
                    if cur_day != i32::MIN {
                        let top = top_of(&mut catsum, &mut touched).map(|c| cats[c as usize]);
                        rows.push(name, cur_day, us, ue, ur, top);
                    }
                    for &c in &active {
                        wcnt[c as usize] = 0;
                    }
                    active.clear();
                }
                tick(7, t);
            }
            let tw = std::time::Instant::now();
            rows.write(&ud.join(format!("part-{g:05}.parquet")));
            tick(8, tw);
            camp
        })
        .collect();
    std::mem::forget(pe_parts);
    lap("sessions");

    // ------------------------------------------------ campaign_daily
    let mut camp_total = vec![[0u64; 4]; width * ndays];
    for c in &results {
        for (t, v) in camp_total.iter_mut().zip(c.iter()) {
            for x in 0..4 {
                t[x] += v[x];
            }
        }
    }
    let mut cid: Vec<u8> = Vec::new();
    let mut ch: Vec<u8> = Vec::new();
    let mut day: Vec<i32> = Vec::new();
    let mut cols = [Vec::<i64>::new(), Vec::new(), Vec::new(), Vec::new()];
    for c in 0..width {
        for d in 0..ndays {
            let v = camp_total[c * ndays + d];
            if v[0] == 0 && v[3] == 0 {
                continue;
            }
            if c == ncamp {
                pq::push_str(&mut cid, b"organic");
                pq::push_str(&mut ch, b"organic");
            } else {
                pq::push_str(&mut cid, campaigns[c][0]);
                pq::push_str(&mut ch, campaigns[c][1]);
            }
            day.push((day0 + d as i64) as i32);
            for x in 0..4 {
                cols[x].push(v[x] as i64);
            }
        }
    }
    let [a, b, c, d] = cols;
    pq::write(
        &cd.join("part-00000.parquet"),
        day.len(),
        vec![
            ("campaign_id", pq::Col::Str(cid)),
            ("channel", pq::Col::Str(ch)),
            ("day", pq::Col::Date(day)),
            ("sessions", pq::Col::I64(a)),
            ("purchasing_sessions", pq::Col::I64(b)),
            ("revenue_cents", pq::Col::I64(c)),
            ("assisted_sessions", pq::Col::I64(d)),
        ],
    )
    .expect("write campaign_daily");
    lap("write");
    if timing {
        for i in 0..9 {
            eprintln!("  {:>10} {:.3}s", TN[i], T[i].load(Relaxed) as f64 / 1e9);
        }
    }
    // all output files are closed and flushed; skip freeing gigabytes of memory one by one
    exit_now();
}

/// Prints how long the process existed before main (loader, runtime start), Linux only.
fn since_exec() {
    #[cfg(target_os = "linux")]
    {
        let st = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
        let up = std::fs::read_to_string("/proc/uptime").unwrap_or_default();
        let start = st.rsplit(')').next().and_then(|r| r.split_whitespace().nth(19)).and_then(|v| v.parse::<f64>().ok());
        let up = up.split_whitespace().next().and_then(|v| v.parse::<f64>().ok());
        if let (Some(s), Some(u)) = (start, up) {
            let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
            eprintln!("   main at {:.3}s after exec", u - s / hz);
        }
    }
}

#[cfg(target_os = "linux")]
fn exit_now() -> ! {
    unsafe { libc::_exit(0) }
}
#[cfg(not(target_os = "linux"))]
fn exit_now() -> ! {
    std::process::exit(0)
}

/// Sorts one person's events (ts << 32 | pc) by ts (order among equal ts is irrelevant: tied
/// events share a session, and the low bits only order them among themselves).
#[inline]
fn sort_u64(r: &mut [u64]) {
    if r.len() <= 16 {
        // insertion sort: most persons have few events
        for i in 1..r.len() {
            let x = r[i];
            let mut j = i;
            while j > 0 && r[j - 1] > x {
                r[j] = r[j - 1];
                j -= 1;
            }
            r[j] = x;
        }
    } else {
        r.sort_unstable();
    }
}

fn top_of(catsum: &mut [u64], touched: &mut Vec<u16>) -> Option<u16> {
    if touched.is_empty() {
        return None;
    }
    let mut best: Option<(u64, u16)> = None;
    for &c in touched.iter() {
        let v = catsum[c as usize];
        best = match best {
            None => Some((v, c)),
            Some((bv, bc)) if v > bv || (v == bv && c < bc) => Some((v, c)),
            x => x,
        };
        catsum[c as usize] = 0;
    }
    touched.clear();
    best.map(|b| b.1)
}

// ---------------------------------------------------------------- fast string map
/// Open-addressing map from byte strings to small values. Keys up to 16 bytes are stored inline,
/// so a lookup usually touches a single cache line. Exact: keys are always compared in full.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Slot<V: Copy> {
    h: u64,
    len: u32,
    used: bool,
    inl: [u8; 16],
    ptr: *const u8,
    val: V,
}
unsafe impl<V: Copy> Sync for Slot<V> {}
unsafe impl<V: Copy> Send for Slot<V> {}

impl<V: Copy> Slot<V> {
    #[inline]
    fn key(&self) -> &'static [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
}

pub struct StrMap<V: Copy> {
    mask: usize,
    slots: Vec<Slot<V>>,
    filter: Vec<u64>, // 1 bit per hash prefix: a clear bit means "not present"
    fbits: u32,
}

impl<V: Copy + Default> StrMap<V> {
    pub fn new(items: impl IntoIterator<Item = (S, V)>, n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        let fbits = ((n * 16).next_power_of_two().max(1 << 12).trailing_zeros()).min(30);
        let mut m = StrMap {
            mask: cap - 1,
            slots: vec![Slot { h: 0, len: 0, used: false, inl: [0; 16], ptr: std::ptr::null(), val: V::default() }; cap],
            filter: vec![0u64; (1usize << fbits) / 64],
            fbits,
        };
        for (k, v) in items {
            m.insert(k, v);
        }
        m
    }
    fn insert(&mut self, key: S, val: V) {
        let h = h64(key);
        let mut pos = (h as usize) & self.mask;
        loop {
            let s = &mut self.slots[pos];
            if !s.used {
                let mut inl = [0u8; 16];
                if key.len() <= 16 {
                    inl[..key.len()].copy_from_slice(key);
                }
                *s = Slot { h, len: key.len() as u32, used: true, inl, ptr: key.as_ptr(), val };
                let f = (h >> (64 - self.fbits)) as usize;
                self.filter[f >> 6] |= 1 << (f & 63);
                return;
            }
            if s.h == h && s.key() == key {
                s.val = val;
                return;
            }
            pos = (pos + 1) & self.mask;
        }
    }
    #[inline]
    pub fn get_h(&self, key: &[u8], h: u64) -> Option<V> {
        let f = (h >> (64 - self.fbits)) as usize;
        if self.filter[f >> 6] & (1 << (f & 63)) == 0 {
            return None;
        }
        let mut pos = (h as usize) & self.mask;
        loop {
            let s = &self.slots[pos];
            if !s.used {
                return None;
            }
            if s.h == h && s.len as usize == key.len() {
                let eq = if key.len() <= 16 { &s.inl[..key.len()] == key } else { s.key() == key };
                if eq {
                    return Some(s.val);
                }
            }
            pos = (pos + 1) & self.mask;
        }
    }
    #[inline]
    pub fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            let f = (h >> (64 - self.fbits)) as usize;
            _mm_prefetch(self.filter.as_ptr().add(f >> 6) as *const i8, _MM_HINT_T0);
            _mm_prefetch(self.slots.as_ptr().add((h as usize) & self.mask) as *const i8, _MM_HINT_T0);
        }
    }
    /// Lookup trusting the 64-bit hash alone (same identity assumption as persons): no key bytes read.
    #[inline]
    pub fn get_hash_only(&self, h: u64) -> Option<V> {
        let f = (h >> (64 - self.fbits)) as usize;
        if self.filter[f >> 6] & (1 << (f & 63)) == 0 {
            return None;
        }
        let mut pos = (h as usize) & self.mask;
        loop {
            let s = &self.slots[pos];
            if !s.used {
                return None;
            }
            if s.h == h {
                return Some(s.val);
            }
            pos = (pos + 1) & self.mask;
        }
    }
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<V> {
        self.get_h(key, h64(key))
    }
}

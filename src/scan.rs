//! Fast CSV line scanner for the rules.md dialect.
//! 64-byte blocks -> bitmasks of ',', '\n', '"' (SSE2), then walk the delimiter bits.
//! A line is kept iff it is non-empty, has exactly N fields, and every '"' in it is the
//! first or last byte of a field wrapped in quotes (checked by counting: quotes in the line
//! must equal 2 x the number of quote-wrapped fields).
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    let v = _mm512_loadu_si512(p as *const _);
    (
        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8)),
        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8)),
        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8)),
    )
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512bw")))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    let c = _mm256_set1_epi8(b',' as i8);
    let n = _mm256_set1_epi8(b'\n' as i8);
    let q = _mm256_set1_epi8(b'"' as i8);
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let m = |x: __m256i, y: __m256i| {
        (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, x)) as u32 as u64)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, y)) as u32 as u64) << 32)
    };
    (m(c, c), m(n, n), m(q, q))
}

#[cfg(not(any(target_feature = "avx2", target_feature = "avx512bw")))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    let c = _mm_set1_epi8(b',' as i8);
    let n = _mm_set1_epi8(b'\n' as i8);
    let q = _mm_set1_epi8(b'"' as i8);
    let (mut mc, mut mn, mut mq) = (0u64, 0u64, 0u64);
    for k in 0..4 {
        let v = _mm_loadu_si128(p.add(16 * k) as *const __m128i);
        mc |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, c)) as u32 as u64) << (16 * k);
        mn |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, n)) as u32 as u64) << (16 * k);
        mq |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, q)) as u32 as u64) << (16 * k);
    }
    (mc, mn, mq)
}

/// Calls f(fields) for every well-formed line with exactly N fields (quotes stripped).
/// The first line is skipped when skip_header. Only '\n' ends a line; a final unterminated line counts.
#[inline(always)]
pub fn scan<'a, const N: usize>(d: &'a [u8], skip_header: bool, mut f: impl FnMut(&[&'a [u8]; N])) {
    let len = d.len();
    let mut ends = [0usize; N];
    let mut n = 0usize;
    let mut bad = false;
    let mut qc = 0u32;
    let mut ls = 0usize;
    let mut skip = skip_header;
    let mut fields: [&'a [u8]; N] = [&[]; N];

    let mut emit = |p: usize, ls: usize, n: usize, bad: bool, qc: u32, ends: &[usize; N], skip: &mut bool| {
        if *skip {
            *skip = false;
            return;
        }
        if bad || n != N - 1 || p == ls {
            return;
        }
        let mut s = ls;
        for k in 0..N - 1 {
            fields[k] = unsafe { d.get_unchecked(s..ends[k]) };
            s = ends[k] + 1;
        }
        fields[N - 1] = unsafe { d.get_unchecked(s..p) };
        if qc > 0 {
            let mut w = 0u32;
            for fl in fields.iter_mut() {
                let l = fl.len();
                if l >= 2 && fl[0] == b'"' && fl[l - 1] == b'"' {
                    *fl = &fl[1..l - 1];
                    w += 2;
                }
            }
            if w != qc {
                return;
            }
        }
        f(&fields);
    };

    let mut i = 0usize;
    let mut pad: [u8; 64];
    while i < len {
        let (c, nl, q) = unsafe {
            if i + 64 <= len {
                masks(d.as_ptr().add(i))
            } else {
                let r = len - i;
                pad = [0u8; 64];
                pad[..r].copy_from_slice(&d[i..]);
                masks(pad.as_ptr())
            }
        };
        let mut delim = c | nl;
        let mut qrem = q;
        while delim != 0 {
            let tz = delim.trailing_zeros();
            let bit = 1u64 << tz;
            let p = i + tz as usize;
            if nl & bit != 0 {
                let below = bit - 1;
                qc += (qrem & below).count_ones();
                qrem &= !below;
                emit(p, ls, n, bad, qc, &ends, &mut skip);
                n = 0;
                bad = false;
                qc = 0;
                ls = p + 1;
            } else if n < N - 1 {
                ends[n] = p;
                n += 1;
            } else {
                bad = true;
            }
            delim &= delim - 1;
        }
        qc += qrem.count_ones();
        i += 64;
    }
    if ls < len {
        emit(len, ls, n, bad, qc, &ends, &mut skip);
    }
}

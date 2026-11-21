# Shopping Sessions — native Rust pipeline

Builders Buildathon · Databricks track. A single multi-threaded Rust binary that implements `rules.md` exactly.

## Status

- 36/36 edge-case fixtures pass.
- Exact `MATCH` against the official `dev-answer` (1M-event dev set, seed 1).
- Cross-checked against an independent, deliberately literal Python implementation (`scripts/reference.py`) on extra seeds, including a window that contains February 29.

Local timings on 2 slow vCPUs: 1M events ≈ 0.3 s, 20M events ≈ 5.5 s, peak RAM ≈ 4.7 GB at 20M.

## How it works

1. **Input.** All CSVs are memory-mapped and split into ~4 MB chunks on line boundaries, then parsed in parallel.
2. **CSV parser.** `src/scan.rs` builds bitmasks of `,`, `\n` and `"` per 64-byte block (one AVX-512BW compare each; AVX2 and SSE2 fallbacks) and walks them. The image is built for `x86-64-v4`, as the scorers are AVX-512 Xeons. A line is kept only if it has exactly N fields and every quote wraps a whole field: the quote count of the line must equal twice the number of quote-wrapped fields. `\r` is ordinary data, and empty lines are dropped.
3. **Timestamps.** F1, F2 and F3 are parsed with fixed byte positions. Dates come from a precomputed calendar table, so impossible dates are rejected and leap years are handled. Digits are ASCII only.
4. **Links.** Union-find over all link lines. The root of each component is kept as the byte-wise smallest ID, which is the person's name.
5. **Events.** Every structurally valid copy is validated up front and bucketed by `event_id` hash. Per bucket, an open-addressing table keeps the copy with the smallest `ingest_ts`, so dedup happens before validation. The kept copy is then dropped if it is invalid or late.
6. **Clicks.** Clicks are validated without dedup. Copies are identical, so the credit and the assist set are unaffected.
7. **Sessions and attribution.** Survivors are bucketed by person (64-bit hash of the name) and sorted by `(person, ts)`. A single fused scan then does four things: splits sessions on gaps over 30 min, credits each session with a two-pointer 7-day window (latest click, ties to the smallest `click_id`), counts distinct assisting campaigns with a stamp array, and streams `user_daily` rows (`top_category` uses byte-wise category ranks).
8. **Output.** `campaign_daily` is a dense campaign × day matrix. Both tables are written as uncompressed Parquet, with `day` as `DATE`.

## Layout

| Path | What |
| --- | --- |
| `src/main.rs` | The pipeline |
| `src/scan.rs` | SIMD CSV line scanner |
| `Cargo.toml`, `Cargo.lock` | Pinned dependencies |
| `Dockerfile` | Multi-stage build: Rust builder, then a slim Debian runtime that runs as uid 1000 |
| `fixtures/` | The 36 official edge-case fixtures |
| `scripts/` | Official fixture runner and diff tool, plus `reference.py` |
| `Makefile` | `make build`, `make fixtures`, `make run-dev`, `make check-dev` |

## Run locally

```sh
cargo build --release
pip install polars
python3 scripts/run_fixtures.py fixtures -- ./target/release/shopping-sessions   # 36/36
DATA_DIR=dev OUTPUT_DIR=out TIMING=1 ./target/release/shopping-sessions          # per-phase timings on stderr
python3 scripts/compare.py out dev-answer                                        # MATCH
```

With Docker, the way the scorer runs it: `make build && make fixtures`.

Environment: `DATA_DIR` (default `/data`), `OUTPUT_DIR` (default `/output`), `NCPU` (thread count, defaults to all cores), and `TIMING` (print phase timings).

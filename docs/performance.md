# Performance snapshot

A point-in-time benchmark of the bridge across dataset sizes and schema profiles.
This is **one cold run per configuration** on a single machine, intended as a sizing
reference — not a statistically rigorous or warmed-up benchmark (see
[Methodology and caveats](#methodology-and-caveats)).

## Environment

| | |
|---|---|
| Host | 8 logical cores, 31 GiB RAM, no swap |
| Java | 25.0.2 |
| Rust | 1.98.1 |
| DataFusion | 55.1.0 |
| arrow-rs | 59.3.0 |
| Ingest mode | zero-copy (default) |
| Batch strategy | one `RecordBatch` per table |
| Date | 2026-09-12 |

Each run was launched in a **bare JVM** (Maven only assembled the classpath), matching
`benchmark/run-benchmark.sh`. Output files: `bench/perf/s<N>-<profile>.{json,csv}`.

> Note: the request said profile `native`; that is the harness's **`normal`** profile.

## Scenarios

1. `1 × 10 × 100` = 1,000 transactions — `normal`, `wide`, `narrow`
2. `2 × 2 × 1000` = 4,000 transactions — `normal`, `wide`, `narrow`
3. `1 × 1 × 1,000,000` transactions — `normal`, `narrow`
4. `1 × 50 × 250` = 12,500 transactions — `normal`, `wide`, `narrow`
5. `1 × 10 × 50,000` = 500,000 transactions — `normal`, `wide`, `narrow`
6. `1 × 1 × 2,000,000` transactions — `normal`, `narrow`

`narrow` uses a flat pain.001-like schema (15 flat transaction columns), so the canonical
queries do not apply and the harness runs **LOAD only**.

## LOAD phase

| # | transactions | profile | txn cols / leaf fields | `arrow_build` | `load_into_datafusion` | Arrow logical | Arrow allocated | RSS after | peak RSS |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,000 | normal | 40 / 207 | 905 ms | 97 ms | 3.6 MB | 9.1 MB | 171 MB | 171 MB |
| 1 | 1,000 | wide | 74 / 1995 | 1,173 ms | 121 ms | 21.0 MB | 57.3 MB | 263 MB | 263 MB |
| 1 | 1,000 | narrow | 15 / 15 | 490 ms | 57 ms | 0.2 MB | 0.9 MB | 118 MB | 118 MB |
| 2 | 4,000 | normal | 40 / 207 | 1,262 ms | 83 ms | 14.5 MB | 23.8 MB | 210 MB | 210 MB |
| 2 | 4,000 | wide | 74 / 1995 | 1,856 ms | 114 ms | 84.0 MB | 132.4 MB | 470 MB | 470 MB |
| 2 | 4,000 | narrow | 15 / 15 | 581 ms | 64 ms | 0.7 MB | 1.6 MB | 123 MB | 123 MB |
| 3 | 1,000,000 | normal | 40 / 207 | 29,343 ms | 186 ms | 3,642 MB | 5,041 MB | 4,382 MB | 4,384 MB |
| 3 | 1,000,000 | narrow | 15 / 15 | 2,017 ms | 60 ms | 192 MB | 250 MB | 615 MB | 617 MB |
| 4 | 12,500 | normal | 40 / 207 | 1,927 ms | 80 ms | 45.4 MB | 60.6 MB | 410 MB | 410 MB |
| 4 | 12,500 | wide | 74 / 1995 | 3,050 ms | 135 ms | 263 MB | 368 MB | 753 MB | 753 MB |
| 4 | 12,500 | narrow | 15 / 15 | 627 ms | 55 ms | 2.3 MB | 4.0 MB | 132 MB | 132 MB |
| 5 | 500,000 | normal | 40 / 207 | 21,109 ms | 161 ms | 1,821 MB | 2,521 MB | 2,360 MB | 2,362 MB |
| 5 | 500,000 | wide | 74 / 1995 | 49,126 ms | 218 ms | 10,514 MB | 13,745 MB | 11,091 MB | 11,091 MB |
| 5 | 500,000 | narrow | 15 / 15 | 1,902 ms | 87 ms | 96 MB | 125 MB | 436 MB | 436 MB |
| 6 | 2,000,000 | normal | 40 / 207 | 67,431 ms | 318 ms | 7,294 MB | 10,080 MB | 8,516 MB | 8,516 MB |
| 6 | 2,000,000 | narrow | 15 / 15 | 3,935 ms | 78 ms | 387 MB | 500 MB | 835 MB | 835 MB |

`arrow_build` = building all Arrow columns in Java (JSON + nested structs). It **dominates
total wall time**; the bridge load (`load_into_datafusion`) stays in the tens-to-hundreds of
milliseconds because zero-copy makes registration O(1)-ish.

## QUERY phase

Per-query `execute / result_transfer` in milliseconds (producer-side execution /
JNI streaming time), for the profiles where the canonical queries apply. Plannning
(`plan`) is 1–7 ms in every run and is omitted. The `narrow` profile is LOAD-only.

| scenario / profile | q1 | q2 | q3 | q4 | q5 | q6 | q7 | q8 | q9 | q10 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 (1k) normal | 0/7 | 0/0 | 0/0 | 0/0 | 0/1 | 0/1 | 1/2 | 0/0 | 0/0 | 1/1 |
| 1 (1k) wide | 0/7 | 0/0 | 0/0 | 0/0 | 0/1 | 0/1 | 1/2 | 0/0 | 0/0 | 0/0 |
| 2 (4k) normal | 0/6 | 0/0 | 0/0 | 0/0 | 1/1 | 1/1 | 1/3 | 0/0 | 0/0 | 1/1 |
| 2 (4k) wide | 0/7 | 0/0 | 0/0 | 0/0 | 0/1 | 1/1 | 1/2 | 0/0 | 0/0 | 0/0 |
| 3 (1M) normal | 21/20 | 18/18 | 3/4 | 3/3 | 10/7 | 56/56 | 44/43 | 38/38 | 2/2 | 46/46 |
| 4 (12.5k) normal | 0/7 | 0/0 | 0/0 | 0/0 | 0/1 | 1/2 | 1/2 | 0/0 | 0/0 | 2/2 |
| 4 (12.5k) wide | 0/5 | 0/0 | 0/0 | 0/0 | 3/1 | 2/3 | 2/2 | 0/0 | 0/0 | 2/2 |
| 5 (500k) normal | 18/17 | 14/18 | 5/7 | 1/2 | 7/3 | 31/32 | 26/27 | 33/33 | 2/2 | 25/25 |
| 5 (500k) wide | 17/17 | 8/9 | 2/3 | 1/1 | 7/4 | 19/19 | 23/24 | 16/16 | 1/1 | 13/14 |
| 6 (2M) normal | 37/36 | 27/28 | 10/10 | 3/4 | 26/23 | 88/88 | 80/80 | 72/71 | 7/7 | 86/86 |

Result row counts are unchanged by size/profile (they scale with the dataset):
`q1`/`q2`/`q8` return all rows, `q3` ≈ 1/26 of rows, `q4` ≈ 1/1000, `q5` = 4 currency
groups, `q6`/`q7`/`q10` return one row per remittance (or one summary row), `q9` = 1.

## Memory model and the `wide` crash

Arrow memory scales **linearly with rows** and roughly **5–6× from `normal` to `wide`**
(nearly 2,000 leaf fields vs 207):

| profile | allocated at 1k | allocated at 500k | approximate slope | extrapolated 1M |
|---|---:|---:|---:|---:|
| narrow | 0.9 MB | 125 MB | ~0.00025 MB/row | ~250 MB |
| normal | 9.1 MB | 2,521 MB | ~0.00504 MB/row | ~5.0 GB |
| wide | 57.3 MB | 13,745 MB | ~0.0275 MB/row | **~27 GB** |

For large runs, peak RSS ≈ Arrow allocated + a few hundred MB of JVM/other overhead.

* **`wide × 500K`** allocated 13.7 GB (peak RSS 11.1 GB). It was run inside a
  `systemd-run --user --scope -p MemoryMax=15G -p MemorySwapMax=0` cgroup so an overrun
  could only OOM-kill the benchmark, not the login session. It completed.
* **`wide × 1M`** extrapolates to ~27 GB allocated / ~22 GB peak RSS — more than this
  machine has (31 GiB total, ~16 GiB free, no swap). This is the configuration that
  crashes the terminal: the kernel OOM-killer or outright allocation failure takes the
  process (and, without a cgroup, potentially the session). **Do not run `wide` above
  ~500K rows without a memory cgroup or a larger host.**

Practical limits on a 31 GiB host: `normal` up to ~2M rows (10.1 GB allocated, 8.5 GB peak
RSS), `narrow` up to many millions (500 MB at 2M), `wide` up to ~500K rows.

## Observations

* **Ingestion cost is the Java Arrow build, not the bridge or DataFusion.** At 2M `normal`
  rows the build is 67.4 s while the zero-copy registration is 318 ms. Narrow the schema
  (or reduce JSON/nested fields) to cut build time; `narrow` builds 2M rows in 3.9 s.
* **Zero-copy load is flat.** `load_into_datafusion` stays ~60–320 ms from 1K to 2M rows.
* **Queries are fast and stream-bound.** Even at 2M rows the heaviest shapes (`q6` join,
  `q10` mismatch count) are under ~90 ms, and `execute` ≈ `result_transfer`, so the
  producer is not the bottleneck. Per-row projections (`q1`) scale linearly with rows
  (37 ms for 2M).
* **`wide` buys structure at a memory price.** ~5–6× the footprint of `normal` for the
  same rows; it should be reserved for small/medium datasets.

## Methodology and caveats

* **One cold run per configuration; no warm-up and no repeats.** Numbers include JIT and
  allocator warm-up and are not medians. Treat them as magnitudes, not precise figures.
* Measured in a dedicated bare JVM, not `mvn exec:java` (which inflates RSS and startup).
* Zero-copy ingest: the Java Arrow buffers stay pinned until the engine is closed, so
  peak RSS includes the whole dataset (one copy).
* The large `wide` run used a 15 GB memory cgroup (see above); all other runs were
  unconstrained.
* Hardware/OS/versions are listed in [Environment](#environment). Absolute numbers will
  differ on another host.

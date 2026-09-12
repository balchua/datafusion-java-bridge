# Benchmarking

## What is benchmarked (and what is not)

Synthetic payment-like data is generated **directly into Arrow vectors** — no XML, no Java
POJO graphs, no `List<PaymentTransaction>`. The point is to measure Arrow construction, the
JNI bridge, DataFusion, Parquet and I/O — not an XML parser.

The harness runs in **two phases** so load cost and query cost never blur together.

## Phase 1 — LOAD

Generates the dataset in Java (hierarchical shape `messages × remittances × transactions`),
registers all three tables through the bridge, and reports:

| Metric | Meaning |
|---|---|
| `arrow_build` | build **all** Arrow columns in Java: one number for the whole dataset, including JSON payloads and nested Structs |
| `load_into_datafusion` | JNI export + Rust import + MemTable registration — **the load number** |
| `transactions schema` | `topLevelColumns` / `leafFields` for the transactions table |
| `arrow data size` | logical buffer bytes (`arrow_data_mb`) and actual off-heap allocation (`arrow_allocated_mb`, includes vector growth slack) |
| `process memory` | RSS before → after load, peak RSS, Java heap |

Ingest mode (`--zero-copy` is the default, `--deep-copy` opts out; see
`docs/arrow-lifetime.md`):

```text
                     deep-copy      zero-copy (default)
load_into_datafusion   5,054 ms         94 ms
peak RSS               7,980 MB       4,391 MB
```

## Phase 2 — QUERIES

Runs each selected SQL against the already-loaded tables. Every output row carries the full
SQL text and its one-line explanation, plus three timings:

| Stage | What it measures |
|---|---|
| `plan_ms` | parse → logical plan → optimize → physical plan |
| `execute_ms` | DataFusion execution, producer-side (scan/filter/aggregate/join) |
| `result_transfer_ms` | Java-side loop importing result batches through the JNI boundary |

`execute_ms` and `result_transfer_ms` overlap (the producer runs ahead into a bounded
channel); do not add them. The real query wall time is roughly `max(execute, transfer)`.

Select queries with `--queries all` (every query) or a subset like `--queries q1,q3,q5`;
with no `--queries` argument only the LOAD phase runs (useful for measuring ingestion alone).

## Dataset shape (mandatory)

```text
--messages M --remittances-per-message R --transactions-per-remittance T
```

`1 × 10 × 100000` = one million transactions. One RecordBatch per table (spec §5): the
whole dataset crosses the bridge as a single batch.

## Schema profiles

* `normal` — canonical payment model with real typed Structs (parties, accounts, postal
  addresses, agents, ultimate parties, FX) and faithful ISO 20022 JSON blocks, plus 6 named
  domain groups: 40 transactions top-level columns / **207 leaf fields**.
* `wide` — the same real model plus 40 named domain groups: 74 top-level / **~2,000 leaves**,
  **not** `field0001…field2000`. Intended for smaller datasets; `wide × 2M` is memory-dominated.

Counts are reported in the output so they are never assumed.

## Queries

| Name | SQL shape |
|---|---|
| `q1` | projection over all rows |
| `q2` | projection into a Struct field |
| `q3` | filter on a struct field (`debtor.name`) |
| `q4` | filter on a struct-in-struct field (`debtor.account.iban`) |
| `q5` | aggregation with exact Decimal128 (`GROUP BY currency, SUM(amount)`) |
| `q6` | three-table join (message × remittance × transaction) |
| `q7` | per-remittance control-sum validation (declared vs actual count/sum + `balanced` flag) |
| `q8` | scan of a real JSON payload column (`regulatory_reporting LIKE …`) |
| `q9` | deep typed Struct path (`ultimate_debtor.account.iban`) |
| `q10` | mismatch summary: count of remittances whose count/sum differ from the declared control sum |

New queries are one enum entry in `BenchmarkQuery`.

## Transaction sizes

10 / 100 / 1k / 10k / 100k / 500k / 1M / 2M (spec §19). RecordBatch size = row count by
default; multi-batch ingestion can be added later without an API change.

## Output

`--out FILE` writes `FILE.json` and `FILE.csv`. The JSON has a `load` block and a `queries`
array; the CSV is long-format with a `phase` column (`LOAD` / `QUERY`).

`--dump-parquet DEST` additionally exports the three loaded tables to
`DEST/<table>.parquet` (via the Rust Parquet writer) so the exact input can be inspected
externally, e.g. with DuckDB.

## Methodology rules

* Every stage timed separately — never one wall-clock number (spec §31).
* Nothing is assumed fast/slow; the harness exists to find the bottleneck.
* Run it through `benchmark/run-benchmark.sh` (bare JVM), never `mvn exec:java`, because
  Maven's own JVM inflates peak RSS, which the harness measures.

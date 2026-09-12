# Benchmark

Two-phase performance harness for the bridge: synthetic payment-like Arrow data is
generated **directly into Arrow vectors** — no XML, no Java object graphs — and the
pipeline is measured in two separate phases so load cost and query cost never blur
together (methodology: `docs/benchmarking.md`).

## The two phases

**PHASE 1 — LOAD.** Generates the dataset in Java (hierarchical shape, below), registers
all three tables in DataFusion through the JNI bridge, and reports:

| Metric | Meaning |
|---|---|
| `arrow_build` | build **all** Arrow columns in Java (JSON + nested Structs) — one number for the whole dataset |
| `load_into_datafusion` | JNI export + Rust import + MemTable registration — **the load number** |
| `transactions schema` | top-level columns / logical leaf fields |
| `arrow data size` | logical bytes (`arrow_data_mb`) and actual off-heap allocation (`arrow_allocated_mb`) |
| `process memory` | RSS before → after the load, plus peak RSS and Java heap |

**PHASE 2 — QUERIES.** Runs each SQL against the already-loaded tables. Every row prints
the SQL itself, a one-line explanation, the result row count, and three timings:
`plan` (parse → logical → optimize → physical), `execute` (DataFusion's own work,
producer-side), `result_transfer` (JNI streaming of batches into Java).

## The dataset shape (mandatory)

```text
messages × remittances-per-message × transactions-per-remittance
```

## Command-line arguments

| Argument | Default | Meaning |
|---|---|---|
| `--messages N` | `1` | number of messages |
| `--remittances-per-message N` | **required** | remittances per message |
| `--transactions-per-remittance N` | `25` | transactions per remittance |
| `--profile normal\|wide\|narrow` | `normal` | schema profile (see below) |
| `--queries all` or `q1,q3,q5` | *(none)* | SQLs to run; `all` = every query, short ids (`q3`) or full names (`q3_nested_filter`); **omit to run the LOAD phase only** |
| `--out FILE` | `bench/results.json` | output base; writes `FILE.json` and `FILE.csv` |
| `--dump-parquet DEST` | | export the loaded tables to `DEST/<table>.parquet` (local dir or `s3://` prefix) for inspection |
| `--memory-limit-gb N` | unlimited | DataFusion memory-pool limit (spillable operators only) |
| `--scratch DIR` | `${java.io.tmpdir}/dfe-scratch` | spill / Parquet staging directory |
| `--zero-copy` | **default** | zero-copy Arrow ingest |
| `--deep-copy` | | deep-copy Arrow ingest (Rust owns a copy) |
| `--s3-bucket B` | | enables S3 paths (`s3://B/...`) |
| `--s3-endpoint URL` | | custom S3-compatible endpoint (e.g. MinIO) |
| `--s3-region R` | `us-east-1` | S3 region |
| `--s3-access-key K` / `--s3-secret-key K` | | credentials (prefer `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) |
| `--s3-allow-http` | | allow plain-http endpoints |
| `--help` | | print usage |

### Running specific queries

Queries are SQL shapes `q1`–`q9` (table below). Select them with a comma-separated list:

```bash
# all nine queries
benchmark/run-benchmark.sh --messages 1 --remittances-per-message 10 \
    --transactions-per-remittance 100000 --queries all --out bench/all-1m

# just the aggregation query
benchmark/run-benchmark.sh --messages 1 --remittances-per-message 10 \
    --transactions-per-remittance 100000 --queries q5 --out bench/q5

# several, by id or by full name
benchmark/run-benchmark.sh --messages 1 --remittances-per-message 10 \
    --transactions-per-remittance 100000 --queries q1,q3,q5 --out bench/subset
benchmark/run-benchmark.sh --messages 1 --remittances-per-message 10 \
    --transactions-per-remittance 100000 --queries q3_nested_filter,q9_ultimate_party_filter \
    --out bench/named

# LOAD only (no SQL) — useful for ingestion timing
benchmark/run-benchmark.sh --messages 1 --remittances-per-message 10 \
    --transactions-per-remittance 100000 --out bench/load-only
```

## Running

Use the runner script — it assembles the classpath with Maven but launches the benchmark
in a **dedicated bare JVM**. Do NOT benchmark through `mvn exec:java`: Maven's own JVM
(core, plugins, guava/plexus) inflates heap, metaspace and peak RSS — and peak RSS is one
of the recorded numbers — and adds startup/classloading latency before the first
measurement.

```bash
# from the repo root
# 4 messages x 10 remittances x 2500 transactions = 100,000 transactions
benchmark/run-benchmark.sh --messages 4 --remittances-per-message 10 \
    --transactions-per-remittance 2500 --out bench/results.json

# 2,000,000 transactions in ONE RecordBatch (1 message x 80,000 remittances x 25)
DFE_JVM_OPTS="-Xmx12g" benchmark/run-benchmark.sh --messages 1 \
    --remittances-per-message 80000 --transactions-per-remittance 25 \
    --out bench/results-2m.json

# pick specific SQLs
benchmark/run-benchmark.sh --messages 4 --remittances-per-message 10 \
    --transactions-per-remittance 2500 --queries q1,q3,q5 --out bench/results-pick.json

# wide schema profile (~2000 leaf fields) — for smaller datasets, see below
benchmark/run-benchmark.sh --messages 1 --remittances-per-message 400 \
    --transactions-per-remittance 25 --profile wide --out bench/results-wide.json
```

Equivalent manual invocation (no script, still a bare JVM):

```bash
mvn -q -pl java test-compile
mvn -q -pl java dependency:copy-dependencies -DincludeScope=runtime \
    -DoutputDirectory="$PWD/java/target/bench-libs"
java --enable-native-access=ALL-UNNAMED \
     --add-opens=java.base/java.nio=ALL-UNNAMED \
     -Dio.netty.tryReflectionSetAccessible=true \
     -cp "java/target/classes:java/target/test-classes:java/target/bench-libs/*" \
     io.github.balchua.datafusion.bench.BenchmarkMain \
     --messages 4 --remittances-per-message 10 --transactions-per-remittance 2500
```

Notes:
* Ingest mode: zero-copy is the default (`--deep-copy` opts out; see
  `docs/arrow-lifetime.md` for the Java ownership rules). The load block reports
  `ingest_mode`, `arrow_data_mb` (logical) and `arrow_allocated_mb` (actual off-heap).
* The script builds the native library only if missing; `DFE_JVM_OPTS` passes extra JVM
  options (e.g. `-Xmx12g` for 2M-transaction runs).
* `mvn exec:java` still works for quick smoke checks, but its numbers are NOT comparable:
  it runs inside Maven's JVM.

## Ingest modes: deep-copy vs zero-copy

`EngineConfig.zeroCopyIngest(boolean)` selects how Arrow crosses into DataFusion. Both modes
use the Arrow C Data Interface with no serialization; the difference is whether Rust copies
the buffers. **Zero-copy is the default**; pass `--deep-copy` to opt out (or `--zero-copy`
explicitly). Ownership rules: `docs/arrow-lifetime.md`.

Measured, 1,000,000 transactions / 207 leaf fields (`normal`):

| metric | deep-copy | zero-copy (default) |
|---|---:|---:|
| `load_into_datafusion` | 5,054 ms | **94 ms** |
| peak RSS | 7,980 MB | **4,391 MB** |
| RSS after load | 6,097 MB | 4,391 MB |
| Arrow logical / allocated | 3,642 / 5,041 MB | 3,642 / 5,041 MB |

```bash
# default (zero-copy)
benchmark/run-benchmark.sh --messages 1 --remittances-per-message 10 \
    --transactions-per-remittance 100000 --out bench/zero-copy-1m

# opt out (Rust owns a copy; Java may reuse/refill its root immediately)
benchmark/run-benchmark.sh --messages 1 --remittances-per-message 10 \
    --transactions-per-remittance 100000 --deep-copy --out bench/deep-copy-1m
```

### When to use which mode

Both modes return identical query results; the choice is purely about **memory ownership
and lifetime**.

**Zero-copy (default) — a dataset that is built once and then frozen.**
Choose it when:

* the pipeline is "build the complete Arrow dataset -> register -> query repeatedly ->
  close", which is the intended payment flow;
* the dataset is large relative to available memory: only one copy exists, so peak RSS is
  roughly halved and `registerArrow` is O(1) instead of a multi-GB memcpy;
* the data is queried many times while it is registered (validation, screening, reporting);
* the allocator is guaranteed to outlive the engine (the engine is closed first).

**Deep-copy — the Java side keeps ownership of its buffers.**
Choose it when:

* the Java side **recycles buffers**: a streaming parser that `clear()`s and refills the same
  `VectorSchemaRoot` per batch. With zero-copy, DataFusion would observe the refilled data
  (silent corruption); deep-copy detaches the registered table from the live root.
* you must **reclaim Java off-heap memory before the table is dropped** — e.g. a tight
  allocator budget, or one long-lived allocator reused to build many datasets over time.
  Deep-copy transfers ownership to Rust so Java can free immediately.
* the vectors are mutated by code you do not control, or you cannot guarantee the allocator
  outlives the engine; deep-copy is the defensive, always-safe choice.
* the engine may **outlive the Java component that produced the batch** (e.g. the engine is
  passed to another subsystem) and you do not want to pin the producer's memory.

**Rule of thumb:** one immutable dataset -> zero-copy; recycled/mutable buffers or memory you
must reclaim early -> deep-copy.

## Inspecting the loaded data with DuckDB

`--dump-parquet DEST` exports the three loaded tables to `DEST/<table>.parquet` using the
engine's Rust Parquet writer, so you can inspect with DuckDB exactly the data DataFusion
received:

```bash
benchmark/run-benchmark.sh --messages 4 --remittances-per-message 10 \
    --transactions-per-remittance 2500 --dump-parquet /tmp/inspect --out bench/dump

ls /tmp/inspect
# payment_message.parquet  remittances.parquet  transactions.parquet

duckdb -c "SELECT count(*) FROM read_parquet('/tmp/inspect/transactions.parquet');"
duckdb -c "SELECT transaction_id, debtor.name, amount \
           FROM read_parquet('/tmp/inspect/transactions.parquet') LIMIT 5;"
```

`DEST` may also be an `s3://bucket/prefix`. The dump covers the *input* tables (what was
loaded), independent of which queries the benchmark runs. Works in both ingest modes.

## The queries

All ten run against the loaded tables unless narrowed with `--queries`
(the full SQL is printed for every row in console, CSV and JSON):

| Name | SQL | Measures |
|---|---|---|
| `q1` | `SELECT transaction_id FROM transactions` | plain projection over all rows |
| `q2` | `SELECT transaction_id, debtor.name …` | projection into a Struct field |
| `q3` | `… WHERE debtor.name = 'BBB'` | filter on a struct field |
| `q4` | `… WHERE debtor.account.iban = '…'` | filter on a struct-in-struct field |
| `q5` | `SELECT currency, COUNT(*), SUM(amount) GROUP BY currency` | aggregation with exact Decimal128 |
| `q6` | join of `payment_message` × `remittances` × `transactions` | three-table relational query |
| `q7` | per-remittance `declared_count/actual_count` and `declared_sum/actual_sum` with a `balanced` flag | **control-sum validation** (pain.001 CtrlSum / NbOfTxs) |
| `q8` | `WHERE regulatory_reporting LIKE '%Dmstcd%'` | scan of real JSON payloads |
| `q9` | `WHERE ultimate_debtor.account.iban = '…'` | deep typed Struct path |
| `q10` | `COUNT(*)` of remittances whose count/sum differ from the declared control sum | mismatch summary (0 = balanced) |

### Control-sum validation

`q7` compares the transactions of each remittance against its declared control values:

```sql
SELECT r.remittance_id AS rid,
       r.number_of_transactions AS declared_count,
       COUNT(t.transaction_id)   AS actual_count,
       r.control_sum             AS declared_sum,
       SUM(t.amount)             AS actual_sum,
       (COUNT(t.transaction_id) = r.number_of_transactions
        AND SUM(t.amount) = r.control_sum) AS balanced
FROM remittances r
LEFT JOIN transactions t ON t.remittance_id = r.remittance_id
GROUP BY r.remittance_id, r.number_of_transactions, r.control_sum;
```

`q10` reduces that to the mismatching remittances only (`HAVING ... <> ...`), so a count of
`0` means every remittance balances. The same pattern works at message level by joining
`payment_message` through `remittances` to `transactions`:

```sql
SELECT m.message_id, m.number_of_transactions AS declared_count,
       COUNT(t.transaction_id) AS actual_count,
       m.control_sum AS declared_sum, SUM(t.amount) AS actual_sum
FROM payment_message m
JOIN remittances r ON r.message_id = m.message_id
JOIN transactions t ON t.remittance_id = r.remittance_id
GROUP BY m.message_id, m.number_of_transactions, m.control_sum;
```

## Output

`--out bench/results.json` writes `.json` and `.csv` side by side.

* **JSON** — a `load` block (shape, per-table row counts, `ingest_mode`,
  `transactions_top_level_columns` / `transactions_leaf_fields`, `arrow_build_ms`,
  `load_into_datafusion_ms`, `arrow_data_mb` (logical), `arrow_allocated_mb` (off-heap),
  `rss_before/after_mb`, `peak_rss_mb`, `java_heap_mb`) and a `queries` array where each
  row carries the **full SQL text**, its explanation, `result_rows`, `plan_ms`,
  `execute_ms`, `result_transfer_ms` and memory.
* **CSV** — long format with a `phase` column: `LOAD` rows (`arrow_build`,
  `load_into_datafusion`, plus `schema` and `memory` values) and `QUERY` rows with the SQL
  quoted inline.

Sample console output — 10,000 transactions (`1 x 10 x 1000`), `--queries q3,q5`.

**Zero-copy (default):**

```text
== LOAD: 1 message(s) x 10 remittance(s)/message x 1000 transaction(s)/remittance = 10,000 transactions ==
  arrow_build                  2287.8 ms   (build all Arrow columns: JSON + nested Structs)
  load_into_datafusion          108.2 ms
  transactions schema              40 cols / 207 leaf fields   (ingest: zero-copy)
  arrow data size                36.3 MB logical / 56.2 MB allocated
  process memory                   55 MB -> 372 MB  (peak 372 MB, java heap 199 MB)
== QUERIES ==

[q3_nested_filter] nested filter: WHERE on a struct field (debtor.name = 'BBB')
  SQL: SELECT transaction_id FROM transactions WHERE debtor.name = 'BBB'  result_rows=385  plan=7ms  execute=0ms  result_transfer=9ms  peakRSS=396MB

[q5_aggregation] aggregation: COUNT(*) + SUM(amount) GROUP BY currency (exact Decimal128)
  SQL: SELECT currency, COUNT(*) AS n, SUM(amount) AS total FROM transactions GROUP BY currency  result_rows=4  plan=7ms  execute=1ms  result_transfer=1ms  peakRSS=400MB
>> wrote bench/sample-zero (+ .csv)
```

**Deep-copy (`--deep-copy`):**

```text
== LOAD: 1 message(s) x 10 remittance(s)/message x 1000 transaction(s)/remittance = 10,000 transactions ==
  arrow_build                  2413.2 ms   (build all Arrow columns: JSON + nested Structs)
  load_into_datafusion          175.6 ms
  transactions schema              40 cols / 207 leaf fields   (ingest: deep-copy)
  arrow data size                36.3 MB logical / 56.2 MB allocated
  process memory                   55 MB -> 351 MB  (peak 351 MB, java heap 135 MB)
== QUERIES ==

[q3_nested_filter] nested filter: WHERE on a struct field (debtor.name = 'BBB')
  SQL: SELECT transaction_id FROM transactions WHERE debtor.name = 'BBB'  result_rows=385  plan=8ms  execute=0ms  result_transfer=9ms  peakRSS=373MB

[q5_aggregation] aggregation: COUNT(*) + SUM(amount) GROUP BY currency (exact Decimal128)
  SQL: SELECT currency, COUNT(*) AS n, SUM(amount) AS total FROM transactions GROUP BY currency  result_rows=4  plan=6ms  execute=1ms  result_transfer=2ms  peakRSS=377MB
>> wrote bench/sample-deep (+ .csv)
```

At 10,000 transactions the two modes are close (36 MB is small). The difference shows at
scale — 1,000,000 transactions / 207 leaf fields:

| metric | deep-copy | zero-copy (default) |
|---|---:|---:|
| `load_into_datafusion` | 5,054 ms | **94 ms** |
| peak RSS | 7,980 MB | **4,391 MB** |
| RSS after load | 6,097 MB | 4,391 MB |

## Schema profiles

* `normal` — canonical model with real pain.001-style structs/JSON plus 6 named domain groups
  (34 real transaction columns + 6 domain structs; 40 top-level / ~207 leaf fields).
* `wide` — same real model plus 40 named domain groups (74 transaction top-level columns;
  ~2000 leaf fields), ISO 20022-like structure.
  Intended for smaller datasets; `wide × 2M` is opt-in and memory-dominated
  (see `docs/benchmarking.md`).
* `narrow` — flat pain.001-like schema mirroring the `java-arrow-pain` DuckDB benchmark
  (36 flat columns across the 3 tables, 15 per transaction; no Struct/List/JSON). For
  ingestion comparisons; the canonical queries do not apply, so the harness runs LOAD only.

### Load comparison at 1,000,000 transactions

| schema | transaction fields | Arrow payload | `arrow_build` | `load_into_datafusion` |
|---|---|---:|---:|---:|
| `narrow` (flat, like java-arrow-pain Type A) | 15 flat | 191.7 MB | **2,164 ms** | **72 ms** (zero-copy) |
| `normal` (real model + JSON) | 207 leaves | 3,642 MB | ~22,000–42,000 ms | 94 ms (zero-copy) |

For reference, the `java-arrow-pain` DuckDB path processed the same 1M transactions in
`Parse 12,783 ms` (including 734 MB StAX XML) + `INSERT` (~3,000 ms), with 294.6 MB of
Arrow and 463 MB peak off-heap. Our timings exclude XML parsing entirely. The conclusion:
field count and JSON payload size dominate ingestion — not the JNI bridge or DataFusion.

## Shape sizing guidance

One RecordBatch per table is the design point (spec §5): the whole dataset crosses the
bridge as a single batch. The largest supported dataset is **2M transactions**
(e.g. `1 × 80,000 × 25`); size the JVM with `DFE_JVM_OPTS="-Xmx12g"` and expect peak RSS
of roughly 8–12 GB, dominated by Arrow vector construction — not by DataFusion, which
answers the bundled queries over 2M rows in tens of milliseconds.
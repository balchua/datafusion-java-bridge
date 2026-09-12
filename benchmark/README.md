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

## The queries

All nine run against the loaded tables unless narrowed with `--queries`
(the full SQL is printed for every row in console, CSV and JSON):

| Name | SQL | Measures |
|---|---|---|
| `q1` | `SELECT transaction_id FROM transactions` | plain projection over all rows |
| `q2` | `SELECT transaction_id, debtor.name …` | projection into a Struct field |
| `q3` | `… WHERE debtor.name = 'BBB'` | filter on a struct field |
| `q4` | `… WHERE debtor.account.iban = '…'` | filter on a struct-in-struct field |
| `q5` | `SELECT currency, COUNT(*), SUM(amount) GROUP BY currency` | aggregation with exact Decimal128 |
| `q6` | join of `payment_message` × `remittances` × `transactions` | three-table relational query |
| `q7` | per-remittance declared vs actual count/sum | realistic validation report |
| `q8` | `WHERE regulatory_reporting LIKE '%Dmstcd%'` | scan of real JSON payloads |
| `q9` | `WHERE ultimate_debtor.account.iban = '…'` | deep typed Struct path |

## Output

`--out bench/results.json` writes `.json` and `.csv` side by side.

* **JSON** — a `load` block (shape, per-table row counts, the four load stages,
  `loaded_arrow_bytes`, `rss_before/after_mb`, `peak_rss_mb`, `java_heap_mb`) and a
  `queries` array where each row carries the **full SQL text**, its explanation,
  `result_rows`, `plan_ms`, `execute_ms`, `result_transfer_ms` and memory.
* **CSV** — long format with a `phase` column: `LOAD` rows (one per stage, plus a
  `memory` row carrying `loaded_arrow_mb`) and `QUERY` rows with the SQL quoted inline.

Sample console output:

```text
== LOAD: 2 message(s) x 200 remittance(s)/message x 25 transaction(s)/remittance = 10,000 transactions ==
  arrow_build                22,445 ms  (build all Arrow columns: JSON + nested Structs)
  load_into_datafusion          94 ms
  transactions schema           40 cols / 207 leaf fields   (ingest: zero-copy)
  arrow data size             3642.3 MB logical / 5040.8 MB allocated
  process memory              55 MB -> 4391 MB  (peak 4391 MB, java heap 446 MB)
== QUERIES ==

[q1_projection] projection: reads one plain column from every row
  SQL: SELECT transaction_id FROM transactions  result_rows=10,000  plan=4ms  execute=0ms  result_transfer=7ms  peakRSS=227MB
```

## Schema profiles

* `normal` — canonical model with real pain.001-style structs/JSON plus 6 named domain groups
  (34 real transaction columns + 6 domain structs; 40 top-level / ~207 leaf fields).
* `wide` — same real model plus 40 named domain groups (74 transaction top-level columns;
  ~2000 leaf fields), ISO 20022-like structure.
  Intended for smaller datasets; `wide × 2M` is opt-in and memory-dominated
  (see `docs/benchmarking.md`).

## Shape sizing guidance

One RecordBatch per table is the design point (spec §5): the whole dataset crosses the
bridge as a single batch. The largest supported dataset is **2M transactions**
(e.g. `1 × 80,000 × 25`); size the JVM with `DFE_JVM_OPTS="-Xmx12g"` and expect peak RSS
of roughly 8–12 GB, dominated by Arrow vector construction — not by DataFusion, which
answers the bundled queries over 2M rows in tens of milliseconds.
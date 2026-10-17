# datafusion-java-bridge

A Java bridge to [Apache DataFusion](https://datafusion.apache.org/): a Java application
loads **Apache Arrow** data into DataFusion as tables and executes SELECT queries against
it. Results stream back as Arrow, or Rust writes them **directly to Parquet** (filesystem
or S3-compatible object storage).

```
Java Arrow builders ──(Arrow C Data Interface)──> Rust / DataFusion
                                                      │  SELECT-only SQL
                                                      ▼
                                    Arrow RecordBatches ──> Java (streamed)
                                                      └──> Parquet (fs / S3)
```

The bridge is **data-model agnostic**. Payment-like data is only the bundled sample
dataset (test fixtures + benchmark harness) — chosen because it stresses nested structs,
`List<Struct>`, `Decimal128`, timestamps and nullability.

**Deliberately thin:** no SQL parser, planner, executor, Parquet reader/writer or S3 client
is implemented here. DataFusion, arrow-rs, the `parquet` crate and `object_store` provide
that. See `docs/architecture.md`.

## Layout

```text
rust/          Cargo crate datafusion_java_bridge (cdylib): JNI entry points, Arrow C Data
               Interface bridge, DataFusion integration, Parquet/object-store support
java/          Maven module io.github.balchua:datafusion-java-bridge
  src/main/java/io/github/balchua/datafusion/engine/      DataFusionEngine, EngineConfig, QueryResult, NativeEngine
  src/main/java/io/github/balchua/datafusion/exception/   typed exceptions
  src/test/java/io/github/balchua/datafusion/             tests + PaymentSchemas/TestPayments (sample data) + bench/
docs/          architecture, memory, arrow-lifetime, parquet-and-s3, benchmarking
benchmark/     how to run the benchmark scenarios
```

## Requirements

* Rust stable (built/tested with 1.98)
* Java 21+ (tests run on 21 or 25)
* Maven 3.9+
* JVM flags for Arrow's direct buffer access on modern JDKs:
  `--enable-native-access=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED`

## Build

```bash
# native library (rust/target/release/libdatafusion_java_bridge.so)
cd rust && cargo build --release && cd ..

# Java module + tests (the native build is also bound into the Maven lifecycle)
mvn -pl java test
```

`mvn test` from the repo root works too (the root `pom.xml` is a Maven aggregator).
VS Code (root directory): the included `.vscode/settings.json` points the Java/Maven
extension at `**/pom.xml` (it imports `java/pom.xml`) and rust-analyzer at
`rust/Cargo.toml`. After structural changes run `Java: Clean Java Language Server
Workspace` and reload the window. Keep the workspace in Trusted mode.

Library loading order (`NativeEngine`): system property `dfe.native.library` (must be an
absolute path) → env `DFE_NATIVE_LIBRARY` → `java.library.path`. The `rust/target/release`
development probe runs only with `-Ddfe.native.dev=true` (tests and the benchmark set it),
so a writable working directory cannot supply the library in production.

## Run tests

```bash
mvn -pl java test                 # unit + integration tests (fast, <1 min)
mvn -pl java test -Plarge         # adds 100K / 1M row large-data tests
```

## Quick example

```java
try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
    engine.registerArrow("transactions", root);          // Java Arrow -> Rust
    try (QueryResult result = engine.query(
            "SELECT transaction_id FROM transactions WHERE debtor.account.iban = '…'")) {
        VectorSchemaRoot batch;
        while ((batch = result.next()) != null) {        // streamed, Arrow-native
            try (VectorSchemaRoot current = batch) {     // close each returned root
                // read values: current.getVector("transaction_id").getObject(0)
            }
        }
    }
    engine.queryToParquet(sql, "s3://bucket/out.parquet"); // encoded in Rust
}
```

## Reading query results

Results stream back as Arrow `VectorSchemaRoot` batches — never `List<Row>`. Each returned
root wraps native memory and must be closed (try-with-resources). Read values straight off
the vectors with `getObject(i)`, which returns the natural Java type: `String`,
`Integer`/`Long`, `Boolean`, `BigDecimal`, `LocalDateTime`, and `Map`/`List` for nested
Struct/List columns.

```java
import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;

// 1. one engine per payment/job
try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {

    // 2. register Java Arrow batches as tables. (The Arrow builders for these roots live
    //    in the sample PaymentSchemas / TestPayments classes under java/src/test.)
    engine.registerArrow("payment_message", messageRoot);
    engine.registerArrow("remittances",     remittancesRoot);
    engine.registerArrow("transactions",    transactionsRoot);

    // 3. the control-sum check: transactions vs the remittance's declared control values
    String sql = """
        SELECT r.remittance_id          AS rid,
               r.number_of_transactions AS declared_count,
               COUNT(t.transaction_id)   AS actual_count,
               r.control_sum             AS declared_sum,
               SUM(t.amount)             AS actual_sum,
               (COUNT(t.transaction_id) = r.number_of_transactions
                AND SUM(t.amount) = r.control_sum) AS balanced
        FROM remittances r
        LEFT JOIN transactions t ON t.remittance_id = r.remittance_id
        GROUP BY r.remittance_id, r.number_of_transactions, r.control_sum
        ORDER BY r.remittance_id
        """;

    try (QueryResult result = engine.query(sql)) {
        VectorSchemaRoot batch;
        while ((batch = result.next()) != null) {           // streaming: zero or more batches
            try (VectorSchemaRoot current = batch) {        // always close a returned root
                for (int i = 0; i < current.getRowCount(); i++) {
                    String rid           = current.getVector("rid").getObject(i).toString();
                    Number declaredCount = (Number)     current.getVector("declared_count").getObject(i);
                    Number actualCount   = (Number)     current.getVector("actual_count").getObject(i);
                    BigDecimal declared  = (BigDecimal) current.getVector("declared_sum").getObject(i);
                    BigDecimal actual    = (BigDecimal) current.getVector("actual_sum").getObject(i);
                    Boolean balanced     = (Boolean)    current.getVector("balanced").getObject(i);
                    System.out.printf("%s declared=%s/%s actual=%s/%s balanced=%s%n",
                            rid, declaredCount, declared, actualCount, actual, balanced);
                }
            }
        }
    }
}
```

To get just the failures, run the mismatch summary and read the single count:

```java
try (QueryResult result = engine.query("""
        SELECT COUNT(*) AS mismatched FROM (
            SELECT r.remittance_id FROM remittances r
            LEFT JOIN transactions t ON t.remittance_id = r.remittance_id
            GROUP BY r.remittance_id, r.number_of_transactions, r.control_sum
            HAVING COUNT(t.transaction_id) <> r.number_of_transactions
                OR SUM(t.amount) <> r.control_sum) x
        """)) {
    VectorSchemaRoot batch = result.next();
    try (batch) {
        long mismatched = (Long) batch.getVector("mismatched").getObject(0); // COUNT(*) -> Int64
        System.out.println("remittances failing control-sum check: " + mismatched);
    }
}
```

Notes:

* String columns return Arrow's `org.apache.arrow.vector.util.Text`; call `.toString()`
  (or cast to `Text`) as shown for `rid`.
* Nested columns are read the same way: e.g. `batch.getVector("debtor").getObject(i)` returns
  a `Map` (`name`, `account`, `agent_bic`), and `structured_remittance` returns a `List`.
* Types: `COUNT(*)` is `Int64` (`Long`); `number_of_transactions` is `Int32` (`Integer`);
  decimal columns are `BigDecimal`; booleans are `Boolean` (may be `null`).
* This is the same query the benchmark ships as `q7` (per-remittance) and `q10` (mismatch
  count); see `benchmark/README.md`.
* With the default zero-copy ingest, close the engine before the Arrow `RootAllocator`
  (see "Ingest modes" below).

### Sharing results across threads

`QueryResult` is a single-consumer stream and each `VectorSchemaRoot` it returns wraps
native buffers with manual ownership, so **neither may be shared across threads**:

* `next()` is stateful — only one thread may pull batches.
* Arrow Java vectors are not thread-safe; concurrent reads of the same root (nested/`List`
  types, shared dictionary state) are undefined.
* Each returned root owns its buffers: close it once, on the thread that owns it. Closing
  while another thread reads is a use-after-free.

To process a batch in parallel, materialize the rows into plain immutable Java objects
(records) on the consuming thread, then hand those to the workers:

```java
record Txn(String id, BigDecimal amount) {}

List<Txn> rows = new ArrayList<>();
try (QueryResult result = engine.query("SELECT transaction_id, amount FROM transactions")) {
    VectorSchemaRoot batch;
    while ((batch = result.next()) != null) {
        try (batch) {                                        // close on this thread only
            var ids = batch.getVector("transaction_id");
            var amounts = batch.getVector("amount");
            for (int i = 0; i < batch.getRowCount(); i++) {
                rows.add(new Txn(ids.getObject(i).toString(),
                                 (BigDecimal) amounts.getObject(i)));
            }
        }
    }
}
rows.parallelStream().forEach(row -> process(row));          // safe: plain Java objects
```

If you must keep Arrow-native parallelism, deep-copy each batch into a fresh
`VectorSchemaRoot` backed by its own `RootAllocator` (one per worker); never share one
root's buffers across threads.

## Simple example program

[`SimpleExample.java`](java/src/test/java/io/github/balchua/datafusion/examples/SimpleExample.java)
is a self-contained runnable example that deliberately uses a trivial `people` table —
`id Int32, name Utf8, salary Decimal128(10,2), active Bool` — instead of payment fields. It
builds the Arrow table, registers it, runs projection/filter/aggregation SQL, reads the
result values, and round-trips the result through Parquet.

```bash
MAVEN_OPTS="--enable-native-access=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED" \
  mvn -pl java exec:java -Dexec.classpathScope=test \
      -Dexec.mainClass=io.github.balchua.datafusion.examples.SimpleExample
```

Output:

```text
built 'people': 8 rows, 4 columns

SQL: SELECT id, name, salary FROM people WHERE active = true ORDER BY id
  id=0  name=alice  salary=1000.00
  id=2  name=carol  salary=1250.00
  id=4  name=erin   salary=1500.00
  id=6  name=grace  salary=1750.00

SQL: SELECT COUNT(*) AS n, SUM(salary) AS total FROM people
  n=8  total=11500.00

SQL: SELECT name FROM people WHERE salary > 1500.00 ORDER BY name
  name=frank
  name=grace
  name=heidi

SQL: SELECT COUNT(*) AS n FROM people_parquet
  n=8
```

It also demonstrates the resource/ownership pattern for the default zero-copy ingest:
the Java `VectorSchemaRoot` is closed immediately after `registerArrow`, the engine is
closed before the `RootAllocator`, and every returned result root is closed.

## Benchmark

Always benchmark via `benchmark/run-benchmark.sh`, which builds the classpath with Maven
but runs the harness in a **dedicated bare JVM**. Never benchmark through
`mvn exec:java`: Maven's own JVM inflates heap and peak RSS (a recorded number) and adds
startup latency. Details and all scenarios: `benchmark/README.md`.

```bash
# shape is mandatory: messages x remittances-per-message x transactions-per-remittance
# 4 messages x 10 remittances x 2500 transactions = 100,000 transactions
benchmark/run-benchmark.sh --messages 4 --remittances-per-message 10 \
    --transactions-per-remittance 2500 --out bench/results.json

# 2,000,000 transactions in ONE RecordBatch (spec §5) — needs ~8-12 GB RAM
DFE_JVM_OPTS="-Xmx12g" benchmark/run-benchmark.sh --messages 1 \
    --remittances-per-message 80000 --transactions-per-remittance 25 \
    --out bench/results-2m.json
```

Two phases in the output: **LOAD** (Arrow generation + time/memory to load into
DataFusion) and **QUERY** (each SQL printed with its own plan/execute/result-transfer
numbers). Sizes up to **2M** transactions — always one RecordBatch per table.

Key arguments (full reference: `benchmark/README.md`):

| Argument | Default | Meaning |
|---|---|---|
| `--messages N` | 1 | number of messages |
| `--remittances-per-message N` | required | remittances per message |
| `--transactions-per-remittance N` | 25 | transactions per remittance |
| `--profile normal\|wide\|narrow` | normal | schema profile |
| `--queries all` \| `q1,q3,q5` | none | SQLs to run; `all` runs every query; omit for LOAD only |
| `--out FILE` | bench/results.json | writes FILE.json + FILE.csv |
| `--deep-copy` | *(zero-copy)* | deep-copy ingest |
| `--dump-parquet DEST` | | export loaded tables to `DEST/<table>.parquet` for DuckDB inspection |
| `--s3-bucket/-endpoint/-region/-access-key/-secret-key/-allow-http` | | S3 |

Queries (select with `--queries`, ids or full names; full SQL printed in every output row):

| id | SQL shape |
|---|---|
| `q1` | projection over all rows |
| `q2` | projection into a Struct field |
| `q3` | filter on `debtor.name` |
| `q4` | filter on `debtor.account.iban` |
| `q5` | `GROUP BY currency, COUNT(*), SUM(amount)` (exact Decimal128) |
| `q6` | three-table join |
| `q7` | per-remittance control-sum check (declared vs actual count/sum + `balanced`) |
| `q8` | scan real JSON column (`regulatory_reporting LIKE …`) |
| `q9` | deep Struct path (`ultimate_debtor.account.iban`) |
| `q10` | count of remittances failing the control-sum check (0 = all balanced) |

## Ingest modes: deep-copy vs zero-copy

`EngineConfig.zeroCopyIngest(boolean)` selects how Arrow data crosses into DataFusion.
Both modes use the Arrow C Data Interface with no serialization; the difference is whether
Rust copies the buffers.

Measured, 1,000,000 transactions / 207 leaf fields:

| metric | deep-copy | zero-copy (default) |
|---|---:|---:|
| `load_into_datafusion` | 5,054 ms | **94 ms** |
| peak RSS | 7,980 MB | **4,391 MB** |
| RSS after load | 6,097 MB | 4,391 MB |

**Zero-copy (default)** — Rust wraps the Java buffers; Java memory stays pinned until
`engine.close()`. This is the right mode for the intended pipeline: build the whole Arrow
dataset while parsing, register it once, query, close. Rules (full version:
`docs/arrow-lifetime.md`):

1. The `RootAllocator` must outlive the engine — close the engine **before** the allocator.
2. Never `clear()`/write/reuse a root that has been registered; allocate a fresh root per batch.
3. Keep the engine (and therefore the table) open while the data is needed.
4. Closing the Java root early is safe (Arrow's exporter retains the buffers); mutating it is not.

**Deep-copy (`zeroCopyIngest(false)`)** — Rust owns a copy; Java may close or reuse its
vectors immediately.

Both modes return identical results; the choice is about **ownership and lifetime**:

| choose zero-copy when | choose deep-copy when |
|---|---|
| you build the complete dataset once, then only read | you `clear()`/refill a root per batch (recycling buffers) |
| the dataset is large and memory is tight (one copy, ~½ peak) | you must reclaim Java off-heap memory before the table is dropped |
| registration must be O(1), not a multi-GB memcpy | the vectors are mutated by code you don't control |
| the allocator outlives the engine | the engine may outlive the producer of the batch |

```java
try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
     DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
    VectorSchemaRoot root = buildTransactions(allocator);
    engine.registerArrow("transactions", root);
    root.close();                       // safe: buffers retained until engine closes
    // ... query ...
}                                        // engine closes -> Rust releases -> Java frees
```

## S3 configuration

```java
EngineConfig config = EngineConfig.builder()
    .memoryLimitBytes(4L * 1024 * 1024 * 1024)
    .s3(s3 -> s3.endpoint("http://minio.internal:9000")   // omit for AWS
                  .region("us-east-1")
                  .accessKey(System.getenv("S3_ACCESS_KEY"))
                  .secretKey(System.getenv("S3_SECRET_KEY"))
                  .allowHttp(true))
    .build();
```

Bucket comes from the URL (`s3://bucket/path`). Credentials may also come from the
environment (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) by leaving them null.
Details: `docs/parquet-and-s3.md`.

## Parquet export / import

```java
engine.queryToParquet(sql, "/var/tmp/out/txn.parquet");            // filesystem
engine.queryToParquet(sql, "s3://bucket/out/txn.parquet");         // S3 multipart
engine.registerParquet("txn", "/data/payments/txn.parquet");       // import
engine.registerParquet("txn", "s3://bucket/path/file.parquet");
```

Java never touches the Parquet bytes; encoding and decoding happen entirely in
Rust/DataFusion.

## Documentation

* `docs/architecture.md` — components, responsibilities, data flow, concurrency model
* `docs/arrow-lifetime.md` — the ownership contract for the Java↔Rust Arrow boundary
* `docs/memory.md` — what DataFusion's memory limit does and does not cover
* `docs/parquet-and-s3.md` — export/import flows, S3 configuration
* `docs/benchmarking.md` — stage definitions and methodology
* `docs/performance.md` — measured load/query performance across shapes and schema profiles

## Versions

Rust: stable (built/tested with 1.98) · DataFusion 55.1 · arrow-rs 59.3 · object_store 0.13
Java: 21+ (developed on 25) · arrow-java 19.0 · JUnit 5
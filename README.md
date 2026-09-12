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

Library loading order (`NativeEngine`): system property `dfe.native.library` → env
`DFE_NATIVE_LIBRARY` → `java.library.path` → `rust/target/release`.

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
            try (batch) { /* read columns */ }
        }
    }
    engine.queryToParquet(sql, "s3://bucket/out.parquet"); // encoded in Rust
}
```

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
Query selection: `--queries q1,q3,q5`; with no `--queries` only the LOAD phase runs.

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
vectors immediately. Use it only when you recycle/refill a `VectorSchemaRoot` after
registering, or must free Java buffers before the table is dropped.

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

## Versions

Rust: stable (built/tested with 1.98) · DataFusion 55.1 · arrow-rs 59.3 · object_store 0.13
Java: 21+ (developed on 25) · arrow-java 19.0 · JUnit 5
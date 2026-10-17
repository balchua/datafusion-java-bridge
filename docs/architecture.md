# Architecture

## Goal

This project is a **Java bridge to Apache DataFusion**: a library that lets a Java
application load Apache Arrow data into DataFusion as tables and execute SELECT queries
against it.

1. a Java application builds data **directly as Apache Arrow** (VectorSchemaRoot /
   RecordBatch — no Java POJO graph, no intermediate object model),
2. hands that Arrow data to Rust through the **Arrow C Data Interface**,
3. registers it as tables in **Apache DataFusion**,
4. executes SQL (SELECT-only; DataFusion does the parsing, planning and execution),
5. receives results as Arrow, or has Rust write them **directly to Parquet**
   (filesystem or S3-compatible object storage),
6. imports Parquet from filesystem / S3-compatible storage **inside Rust/DataFusion**
   (never in Java),
7. can measure every stage of the pipeline separately.

The bridge is **data-model agnostic**: nothing in the API assumes payments. Payment data
(`PaymentMessage`, `PaymentRemittance`, `PaymentTransaction`) is the *sample dataset* used
by the test suite and the benchmark harness because it is large, nullable, deeply nested,
and uses exact-decimal monetary values — a realistic stress test for nested Arrow types,
Decimal128 and the JNI boundary. Applications can register any Arrow schema they want; the
Rust side never hard-codes a schema (see "Canonical model" for how the sample data is
shaped).

**Non-goals (explicitly):** we do not implement a SQL parser, planner, optimizer, executor,
Parquet reader/writer, S3 client, or memory allocator. DataFusion, arrow-rs, the `parquet`
crate and `object_store` provide all of that. This project is a *thin integration layer*.

## Component map

```text
Java application
   │  builds Arrow VectorSchemaRoot (any schema the app chooses)
   ▼
NativeEngine (JNI) ── Arrow C Data Interface (schema ptr + array ptr)
   ▼
rust/src/ffi/mod.rs          JNI entry points, handle registry, exception translation
rust/src/engine.rs           Engine = tokio Runtime + DataFusion SessionContext
rust/src/arrow_bridge.rs     C Data Interface import/export; deep-copy or zero-copy ingest
rust/src/sql_guard.rs        SELECT-only gate (uses DataFusion's bundled sqlparser)
rust/src/query.rs            QueryHandle: streaming result batches, cancellation
rust/src/parquet.rs          encode result to scratch Parquet, publish to fs/S3
rust/src/object_store.rs     build S3/Local object stores, register with DataFusion
rust/src/error.rs            EngineError -> Java exception mapping
```

### Responsibilities

| Module | Responsibility | Explicitly NOT responsible for |
|---|---|---|
| `engine.rs` | Owns `SessionContext` + tokio runtime. Registers tables, plans queries, spawns execution. Deterministic teardown. | SQL logic, Arrow plumbing |
| `arrow_bridge.rs` | Convert Java-exported C-Data-Interface structs into a Rust `RecordBatch`, and Rust batches into C structs for Java. Two modes: zero-copy (default; Rust wraps the Java buffers) and deep-copy (`zeroCopyIngest(false)`; ownership decoupled). Rules in `arrow-lifetime.md`. | Any data interpretation |
| `query.rs` | Push DataFusion result batches through a channel; Java pulls batch by batch; cancellation via task abort. | Planning, execution |
| `sql_guard.rs` | Reject everything that is not a plain `SELECT` (DDL, DML, `COPY`, `SET`, multi-statement). | Query rewriting |
| `parquet.rs` | Encode query result into a scratch Parquet file with the `parquet` crate, then publish (rename / object-store upload). | Encoding internals (uses `parquet::arrow::ArrowWriter`) |
| `object_store.rs` | Build `LocalFileSystem` / `AmazonS3` stores from `EngineConfig`; register them with DataFusion by URL. | Credentials handling beyond reading config/env |
| `ffi/mod.rs` | JNI function bodies; i64 handle <-> pointer with a liveness registry; throw typed Java exceptions. | Any business logic |
| Java `DataFusionEngine` | Small API: register/query/export/close. Translates native sentinels into exceptions. | Anything DataFusion-specific |
| Java `bench` (test sources) | Synthetic Arrow data generation (no XML), stage timing, memory sampling, CSV/JSON output. | — |

## Data flow

```text
Java Arrow builders (application's own schema)
   → VectorSchemaRoot (one RecordBatch per table; large datasets stay one batch)
   → export via Arrow C Data Interface (Java allocates ArrowSchema/ArrowArray structs)
   → JNI: stageArrow  (Rust imports; zero-copy wraps Java buffers, deep-copy clones them)
   → commitTable (MemTable registration in SessionContext)
   → SQL (sql_guard → DataFusion parse → logical → optimized → physical plan)
   → execution stream of Arrow RecordBatches
        ├─→ JNI resultNextBatch → Java imports C structs → VectorSchemaRoot (streamed, never List<Row>)
        └─→ parquet.rs scratch encode → publish to file:///… or s3://bucket/…
```

The design principle: **Java constructs canonical Arrow data; Rust/DataFusion queries it
directly.** There is no insert/materialization step.

Ingestion has two modes (`EngineConfig.zeroCopyIngest`):

* **zero-copy (default)** — Rust keeps the C Data Interface import, so its buffers point at
  the Java off-heap memory; Java must keep those buffers alive and unmodified until the
  table is dropped. No ingestion copy; roughly half the peak memory.
* **deep-copy (`zeroCopyIngest(false)`)** — the imported Java buffers are copied once into
  Rust-owned buffers, so Java can free/reuse its vectors immediately. Cost: one `memcpy`
  and transient 2× memory. For streaming parsers that recycle a root.

The choice is about memory ownership and lifetime: one immutable dataset that can stay
pinned -> zero-copy; recycled/mutable buffers or memory that must be reclaimed early ->
deep-copy. Both are described in detail, with Java best practices and use cases, in
`arrow-lifetime.md`.
Measured for 1,000,000 transactions / 207 leaf fields (`normal`):

| metric | deep-copy | zero-copy (default) |
|---|---:|---:|
| `load_into_datafusion` | 5,054 ms | **94 ms** |
| peak RSS | 7,980 MB | **4,391 MB** |

On a comparable flat schema (`narrow`, 15 transaction columns) building the full Arrow
batch takes ~2.2 s and the DataFusion load ~72 ms (zero-copy), so the payload's field
count and JSON columns — not the bridge — dominate ingestion cost.

## RecordBatch strategy

One logical dataset → one RecordBatch per table. The bridge does not chop data every
65,536 rows: a 2M-row dataset is registered as one 2M-row batch, matching an ingestion
shape like (decode source → Arrow builders → one RecordBatch). The sample benchmark
models this with a single payment containing up to 2M transactions.

The pipeline does not assume a single batch: `commitTable` builds a `MemTable` from a
`Vec<RecordBatch>`, so the Rust side already models multiple batches per table. Today
`stageArrow` stages exactly one batch per staged id (each call allocates a fresh id) and
`commitTable` registers one staged id at a time; true multi-batch ingestion can be added
later without an API change.

## Sample data model (test fixtures)

The payment model lives entirely in **test sources**
(`java/src/test/java/io/github/balchua/datafusion/PaymentSchemas.java` and
`SyntheticPaymentGenerator`) — it is not part of the bridge API. It defines three tables
with shared keys (`message_id`, `remittance_id`, `transaction_id`):

* `payment_message` — message-level facts.
* `remittances` — payment-group facts (`PaymentRemittance`).
* `transactions` — transaction facts, including nested Arrow structs:
  * `debtor` / `creditor`: `Struct{name, account: Struct{iban, number}, agent_bic}`
  * `structured_remittance`: `List<Struct{code, reference, amount}>` (1–5 elements, inline)

Monetary amounts are `Decimal128(18,4)`; the Java schema is authoritative and DataFusion sees
exactly the schema that crossed the C Data Interface — no second schema definition exists on the
Rust side, for ANY schema an application registers. Nested types stay nested (no JSON-string
blobs, no flattening) so SQL like `WHERE debtor.account.iban = '…'` works natively.

## Concurrency model

* `DataFusionEngine` is safe to share across threads for **queries**: `query()` clones DataFusion's
  session state per execution, and DataFusion's `SessionContext` uses interior locking for
  registration. Registration while a query is running is allowed but racy by definition
  (a query started before registration may not see the table).
* One tokio runtime per engine; each query is a task on that runtime. A long query blocks only
  the calling thread (JNI calls are synchronous). Virtual threads: a native call pins its carrier
  thread while blocked — this is documented and accepted for v1 (correctness over async JNI).
* Cancellation: `QueryResult.cancel()` aborts the query task; the next `next()` throws
  `QueryCancelledException`. Closing the engine cancels all live queries first.
* Teardown deadline: `engineClose` waits up to `EngineConfig.closeTimeoutMillis` (default
  60 s, `0` = forever) for in-flight native calls. `queryToParquet`/`registerParquet` run
  synchronously with no cancellation token, so a stalled object-store call can outlast the
  deadline; the engine is then leaked rather than freed, avoiding a use-after-free. The
  timeout is a safety net — it does not make a hung call finish.
* **Results are not shareable across threads.** `QueryResult` is a single-consumer pull
  stream (one thread may call `next()`) and each returned `VectorSchemaRoot` wraps native
  memory with manual ownership (see `arrow-lifetime.md`); Arrow Java vectors are not
  thread-safe. To process rows in parallel, materialize them into plain Java objects on the
  consuming thread first, or deep-copy each batch into a per-worker root. Never close a
  returned root while another thread is reading it.

## SQL safety

`sql_guard.rs` parses the SQL with DataFusion's own parser (`DFParser`) and accepts only a
single `Statement::Query`, rejecting `SELECT ... INTO`, `COPY`, `CREATE EXTERNAL TABLE`,
`INSERT/UPDATE/DELETE`, `SET`, DDL and multi-statements before reaching DataFusion. Only
explicitly registered tables are visible; there are no SQL functions in this build that touch
the filesystem or network. Parameterized queries are not supported in v1 (documented
limitation).

## Error handling

`error.rs` defines one Rust enum `EngineError` with a `kind`; the JNI layer throws the matching
Java exception class (`SqlParseException`, `SqlUnsupportedException`, `TableNotFoundException`,
`SchemaMismatchException`, `ArrowBridgeException`, `ParquetException`, `ObjectStoreException`,
`ResourceException`, `QueryCancelledException`, `EngineClosedException`,
`QueryNotFoundException`, base `DataFusionException`). No raw JNI/native errors reach
application code.

Every JNI entry point runs inside `ffi::jni_guard!`, which catches a Rust panic and throws it as
a `DataFusionException` rather than letting it unwind across the `extern "system"` boundary
(which would abort the whole JVM). A panic is a bug, but it degrades to a normal Java exception.
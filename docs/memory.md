# Memory

## What is measured

* **Process RSS / peak RSS** — read from `/proc/self/status` (`VmRSS`, `VmHWM`) by a sampling
  thread in the benchmark harness. This covers JVM, Java Arrow buffers, JNI, Rust allocations,
  DataFusion, object-store clients — everything in the process.
* **Java heap** — `Runtime` and `GarbageCollectorMXBean`/`MemoryMXBean` sampling.

Peak RSS is reported as the high-water mark `VmHWM`, sampled at 100 ms intervals during the
benchmark. This is approximate (misses sub-100ms spikes) but honest, and it is the number that
matters for container OOM behavior.

## DataFusion's memory limit is NOT a process memory limit

`EngineConfig.memoryLimitBytes` configures DataFusion's `RuntimeEnv` memory pool. It accounts
only for allocations that *opt into* the pool (spillable operators such as sorts, joins, and
hash aggregates when memory-regulation is applied). It does **not** account for:

* the JVM and Java heap,
* Java Arrow off-heap buffers (`arrow-memory`),
* JNI and the Rust allocator (all `RecordBatch` buffers),
* DataFusion internals that allocate outside the pool (task metadata, plans),
* object-store client buffers and the parquet writer's page buffers,
* OS page cache for files written/read.

Therefore the memory limit is a **safety mechanism for spillable operator behavior only**
(roughly: "keep memory-hungry aggregations within X and spill to `scratchDir`"), never a
container memory guarantee. For the 2M-transaction runs, size the container from the measured
RSS in the benchmark output, not from the configured limit.

## Where memory goes in this architecture

| Stage | Memory holders |
|---|---|
| Synthetic generation | Java-side value arrays (transient, e.g. `byte[][]` ids/ibans) + Arrow builders |
| After `registerArrow`, zero-copy (default) | the Java Arrow buffers stay pinned; Rust references them through the C Data Interface. One copy in the process. |
| After `registerArrow`, deep-copy (`zeroCopyIngest(false)`) | Rust owns a copy; during the call Java + Rust copies coexist (~2x logically), then Java may free. |
| Registered tables | zero-copy: DataFusion reads the Java buffers (pinned until engine close). deep-copy: Rust-owned `MemTable` batches. |
| Query execution | DataFusion intermediates (projections, hash tables) — governed by memory limit where spillable |
| Result streaming | Batches in flight (bounded channel) + the batch Java currently holds |
| `queryToParquet` | One scratch Parquet file on disk; writer buffers (row-group sized) |
| Parquet import | DataFusion parquet reader metadata + page buffers during scan |

Measured at 1,000,000 transactions / 207 leaf fields (`normal`):

```text
                    deep-copy      zero-copy (default)
arrow logical         3,642 MB       3,642 MB
arrow allocated       5,041 MB       5,041 MB   (includes vector growth slack)
peak RSS              7,980 MB       4,391 MB
RSS after load        6,097 MB       4,391 MB
```

The benchmark load block reports logical (`arrow_data_mb`) and actual off-heap
(`arrow_allocated_mb`) sizes. Java heap stays small (~400 MB) because Arrow data is
off-heap either way; the heap is dominated by per-row JSON string/byte[] churn during
generation, not by the dataset.

With the payload dominated by JSON columns (8 per transaction row, ~1.85 KB/row; roughly
half the table), memory scales with that payload. Sizing a 2M-row run: expect roughly
2x the 1M numbers.

## Deterministic release

* `QueryResult.close()` closes any in-flight root and cancels/reclaims the native query.
* `DataFusionEngine.close()` cancels all live queries, then drops the engine: `SessionContext`
  drop releases MemTables (registered batches), the tokio runtime shuts down with a timeout.
* There is no GC dependency for native resources. Java-side Arrow buffers are ref-counted and
  freed by closing roots/vectors explicitly.
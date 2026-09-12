# Arrow / JNI ownership and lifetime

This is the most correctness-critical part of the project. Every crossing of the Java/Rust
boundary via the Arrow C Data Interface is documented here, together with the **rules Java
code must follow** so a zero-copy handoff cannot become a use-after-free.

The invariant we must never break:

> Neither side may read Arrow buffers after the other side has released them, and neither
> side may *write* buffers the other side is still reading.

## Two ingest modes

`EngineConfig.zeroCopyIngest(boolean)` selects how `registerArrow` handles the Java buffers.

| | **zero-copy** (default) | **deep-copy** (`zeroCopyIngest(false)`) |
|---|---|---|
| What Rust gets | its own copy of every buffer | pointers into the Java buffers |
| `load_into_datafusion` (1M rows) | ~5.0 s | ~0.09 s |
| peak RSS (1M rows) | ~8.0 GB | ~4.4 GB |
| Java buffer lifetime | may be freed/reused immediately after `registerArrow` | must stay alive and **unmodified** until the table is dropped |
| Who frees the data | Rust frees its copy on table drop; Java frees its own on root close | Rust's release callback returns ownership; Java frees when the table drops |
| Safe when reusing/clearing builders | **no** | yes |
| Best for | one large dataset built once, tight memory (the default flow) | streaming parsers that recycle a root, or callers that must free Java buffers before engine close |

The C Data Interface transfer itself is identical and zero-copy in both modes; the
difference is only whether `arrow_bridge.rs` then runs `deep_copy()`.

## The C Data Interface contract

```text
Java exports schema + one ArrowArray per column   (Data.exportSchema / exportVector)
   • buffers are RETAINED (refcount++), not copied
   • a release callback + private data are attached
        │ pointers only
Rust imports (from_ffi_and_data_type)
   • buffers wrap the Java memory (C Data Interface owner)
   • deep-copy mode: copies them and immediately drops the import
     → release callback fires at once, Java gets its references back
   • zero-copy mode: keeps the import in the RecordBatch/MemTable
     → release callback fires when the batch is dropped (engine/table close)
```

Arrow Java's exporter keeps its own retained references, so under zero-copy the Java
`VectorSchemaRoot` **may be closed** right after `registerArrow` without freeing the data —
the memory lives until Rust releases it. What is *not* safe is **writing** to those buffers
(see the rules below), because Rust is reading the same memory.

## Java best practices (zero-copy mode)

1. **Order the lifetimes explicitly.** The allocator must outlive the engine:

   ```java
   try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        DataFusionEngine engine = new DataFusionEngine(config)) {   // closed first
       VectorSchemaRoot root = TestPayments.transactionsRoot(..., allocator);
       engine.registerArrow("transactions", root);
       root.close();                       // safe: exporter retained the buffers
       // ... queries ...
   }                                        // engine closes -> Rust releases -> Java frees
   ```

   Never close the `RootAllocator` (or any parent allocator) before `engine.close()`; that
   would free the memory DataFusion is still reading.

2. **Treat registered vectors as immutable.** Do not `clear()`, `setSafe()`, reallocate, or
   otherwise write to a root that has been registered. If a parser reuses its builders,
   either use deep-copy mode or allocate a **fresh root per batch**:

   ```java
   for (Batch b : batches) {
       VectorSchemaRoot root = buildFresh(allocator, b);  // new vectors each time
       engine.registerArrow("transactions", root);
       root.close();                                       // buffers stay via exporter
   }
   ```

3. **Keep the engine (and therefore the table) open while the data is needed.** Dropping
   the engine drops the MemTable and triggers the release callbacks.

4. **Do not depend on GC.** Nothing here needs finalization: the release path is driven by
   Rust's `Drop` (on `engine.close()`) calling back into Arrow Java. Still close query
   results and roots deterministically; tests assert zero leak bytes.

5. **Expect occasional hidden copies even in zero-copy mode.** arrow-rs requires stricter
   buffer alignment than the C Data Interface spec, so `align_buffers()` may copy an
   individual misaligned buffer. And DataFusion creates new buffers for filters,
   aggregations, joins, repartitioning and spilling regardless of ingest mode. Zero-copy
   removes the *ingestion* copy, not all copies.

6. **Only opt out to deep-copy when you actually recycle buffers** (or must free Java
   memory before the table is dropped). The default zero-copy is correct for the
   build-once-then-query pipeline.

## Deep-copy mode (default) ownership

1. Java exports (retain, no copy) → Rust imports (wrap) → Rust `deep_copy` → drops the
   import, firing the release callback. Java's references are back before `registerArrow`
   returns.
2. Java may close/reuse the root and its allocator immediately; the table is fully
   Rust-owned.
3. On `engine.close()`, the MemTable and its buffers are freed by Rust.

## Result path (Rust → Java) — always zero-copy

1. Rust exports per column with `to_ffi`; the exported structs hold `Arc` references to the
   buffers (no copy).
2. Java imports into a fresh `VectorSchemaRoot`; Arrow Java wraps the native memory
   (`unsafeAssociateAllocation`) and registers deallocators.
3. Closing each returned root triggers the release callbacks, dropping the Rust `Arc`s and
   freeing the batch buffers.

Therefore **Java must close every root returned by `QueryResult.next()`** (try-with-
resources). Failing to do so leaks the batch until the query/engine is dropped.

## Handles and safety inventory

* Engine/query handles are `i64` pointers validated against a live-registry before use; a
  stale handle throws instead of dereferencing.
* An `active_calls` counter makes `engineClose` wait for in-flight native calls to return
  (after cancelling their queries), so a blocked `resultNextBatch` can never touch a freed
  engine.
* All `unsafe` lives in `ffi/mod.rs` (handle casts, JNI strings) and `arrow_bridge.rs`
  (`from_raw`, `from_ffi_and_data_type`, `ptr::write`, `to_ffi`), each with a `SAFETY`
  comment stating the invariant and ownership.

## Measured comparison (1M transactions, `normal`, 207 leaf fields)

```text
                        deep-copy      zero-copy
load_into_datafusion     5,054 ms         94 ms
peak RSS                 7,980 MB       4,391 MB
rss after load           6,097 MB       4,391 MB
arrow logical             3,642 MB       3,642 MB
arrow allocated           5,041 MB       5,041 MB
```

Zero-copy removes the second copy of the payload (`load_into_datafusion`) and the
Java+Rust overlap that produced the 8 GB peak. The single remaining copy is the Java Arrow
memory, which stays pinned until `engine.close()`.

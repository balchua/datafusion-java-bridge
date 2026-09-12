package io.github.balchua.datafusion.engine;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import io.github.balchua.datafusion.exception.DataFusionException;
import io.github.balchua.datafusion.exception.EngineClosedException;

/**
 * The complete Java API of the native DataFusion engine.
 *
 * <pre>{@code
 * try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
 *     engine.registerArrow("transactions", root);        // Java Arrow -> Rust
 *     try (QueryResult result = engine.query("SELECT ...")) {
 *         VectorSchemaRoot batch;
 *         while ((batch = result.next()) != null) {
 *             try (batch) { ... }                        // consume Arrow, never List<Row>
 *         }
 *     }
 *     engine.queryToParquet(sql, "s3://bucket/out.parquet");
 * }
 * }</pre>
 *
 * <p>
 * Concurrency model (see docs/architecture.md): the engine may be shared across
 * threads for concurrent {@link #query} calls; registrations racing with
 * running queries
 * are racy by definition. A JNI call blocks its carrier thread while executing.
 */
public final class DataFusionEngine implements AutoCloseable {
    private final long handle;
    private final RootAllocator structAllocator;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String versionInfo;

    public DataFusionEngine(EngineConfig config) {
        long created = NativeEngine.engineCreate(config.toNativeJson());
        if (created == 0) {
            throw new DataFusionException(
                    "engine creation failed (see pending exception or prior log)");
        }
        this.handle = created;
        try {
            this.versionInfo = NativeEngine.engineVersion();
        } catch (RuntimeException e) {
            // The native engine was created but the constructor cannot return a usable
            // object; release it so the handle does not leak.
            NativeEngine.engineClose(created);
            throw e;
        }
        // Created last: at this point there is no remaining failure path, so the
        // allocator never needs conditional cleanup.
        this.structAllocator = new RootAllocator(Long.MAX_VALUE);
    }

    /** Engine + library version metadata (rustc, DataFusion, arrow). */
    public String versionInfo() {
        return versionInfo;
    }

    /**
     * Cleanup for an Arrow C Data Interface struct, usable as a try-with-resources
     * resource. {@code close()} does not throw and performs {@code release()} (returning
     * the retained Java buffers across the C Data Interface) before {@code close()}
     * (freeing the struct memory).
     */
    @FunctionalInterface
    private interface NativeStructCleanup extends AutoCloseable {
        @Override
        void close();
    }

    /**
     * Registers one Arrow record batch as a table (canonical model: one payment ->
     * one RecordBatch per table). The root is not modified; after this call
     * returns,
     * Java may close it immediately — the data has been copied into Rust-owned
     * memory.
     */
    public void registerArrow(String tableName, VectorSchemaRoot batch) {
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(batch, "batch");
        ensureOpen();
        int columns = batch.getSchema().getFields().size();

        // Export through the C Data Interface: one schema struct + one array struct
        // per column. Arrow Java retains the buffers (refcount++), zero copy. The
        // cleanup wrappers release() before close(); on the success path Rust's
        // `from_raw` already nulled each release pointer, so release() is a no-op there.
        ArrowSchema schemaStruct = ArrowSchema.allocateNew(structAllocator);
        try (NativeStructCleanup schemaCleanup = () -> {
            schemaStruct.release();
            schemaStruct.close();
        }) {
            Data.exportSchema(structAllocator, batch.getSchema(), null, schemaStruct);
            long[] arrayAddrs = new long[columns];
            ArrowArray[] arrayStructs = new ArrowArray[columns];
            try (NativeStructCleanup arraysCleanup = () -> {
                for (ArrowArray a : arrayStructs) {
                    if (a != null) {
                        a.release();
                        a.close();
                    }
                }
            }) {
                for (int i = 0; i < columns; i++) {
                    ArrowArray a = ArrowArray.allocateNew(structAllocator);
                    arrayStructs[i] = a;
                    Data.exportVector(structAllocator, batch.getVector(i), null, a);
                    arrayAddrs[i] = a.memoryAddress();
                }
                long stagedId = NativeEngine.stageArrow(
                        handle, tableName, schemaStruct.memoryAddress(), arrayAddrs);
                NativeEngine.commitTable(handle, tableName, stagedId);
            }
        }
    }

    /** Registers a Parquet table from a local path or s3:// location. */
    public void registerParquet(String tableName, String location) {
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(location, "location");
        ensureOpen();
        NativeEngine.registerParquet(handle, tableName, location);
    }

    /** Executes a SELECT query; results stream as Arrow batches. */
    @SuppressWarnings("java:S2093") // the native query is released only on construction failure
    public QueryResult query(String sql) {
        ensureOpen();
        long queryId = NativeEngine.query(handle, sql);
        if (queryId == 0) {
            throw new DataFusionException(
                    "query failed (see pending exception)");
        }
        try {
            return new QueryResult(this, queryId);
        } catch (RuntimeException e) {
            // The native query is already registered; if the Java wrapper cannot be
            // constructed, release it so it does not leak until engine.close().
            NativeEngine.resultClose(handle, queryId);
            throw e;
        }
    }

    /**
     * Executes SQL and writes the result directly to Parquet in Rust.
     * 
     * @return JSON {"encode_nanos":..,"publish_nanos":..} for benchmarking.
     */
    public String queryToParquet(String sql, String destination) {
        ensureOpen();
        return NativeEngine.queryToParquet(handle, sql, destination);
    }

    /**
     * Releases the engine, all registered tables, and all running queries. Any
     * in-flight
     * native calls are cancelled; their Java threads observe an
     * EngineClosedException.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            NativeEngine.engineClose(handle);
            structAllocator.close();
        }
    }

    /** Native handle; exposed for tests and the benchmark harness. */
    public long nativeHandle() {
        return handle;
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new EngineClosedException("DataFusionEngine is closed");
        }
    }
}
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
        this.structAllocator = new RootAllocator(Long.MAX_VALUE);
        long created;
        try {
            created = NativeEngine.engineCreate(config.toNativeJson());
        } catch (RuntimeException | Error e) {
            structAllocator.close();
            throw e;
        }
        if (created == 0) {
            structAllocator.close();
            throw new DataFusionException(
                    "engine creation failed (see pending exception or prior log)");
        }
        this.handle = created;
        try {
            this.versionInfo = NativeEngine.engineVersion();
        } catch (RuntimeException | Error e) {
            // The native engine was created but the constructor cannot return a
            // usable object; release it so the handle and allocator do not leak.
            try {
                NativeEngine.engineClose(created);
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            } finally {
                structAllocator.close();
            }
            throw e;
        }
    }

    /** Engine + library version metadata (rustc, DataFusion, arrow). */
    public String versionInfo() {
        return versionInfo;
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
        // per column. Arrow Java retains the buffers (refcount++), zero copy.
        try (ArrowSchema schemaStruct = ArrowSchema.allocateNew(structAllocator)) {
            Data.exportSchema(structAllocator, batch.getSchema(), null, schemaStruct);
            long[] arrayAddrs = new long[columns];
            ArrowArray[] arrayStructs = new ArrowArray[columns];
            try {
                for (int i = 0; i < columns; i++) {
                    ArrowArray a = ArrowArray.allocateNew(structAllocator);
                    arrayStructs[i] = a;
                    Data.exportVector(structAllocator, batch.getVector(i), null, a);
                    arrayAddrs[i] = a.memoryAddress();
                }
                long stagedId = NativeEngine.stageArrow(
                        handle, tableName, schemaStruct.memoryAddress(), arrayAddrs);
                NativeEngine.commitTable(handle, tableName, stagedId);
            } finally {
                // On the success path Rust's `from_raw` already consumed each struct and
                // nulled its release pointer, so `release()` is a no-op and `close()` just
                // frees the small Java-side struct. `release()` is only load-bearing when
                // `stageArrow` fails early (e.g. a column-count mismatch) and Rust never
                // consumed some of the arrays.
                for (ArrowArray a : arrayStructs) {
                    if (a != null) {
                        a.release();
                        a.close();
                    }
                }
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
    public QueryResult query(String sql) {
        ensureOpen();
        long queryId = NativeEngine.query(handle, sql);
        if (queryId == 0) {
            throw new DataFusionException(
                    "query failed (see pending exception)");
        }
        try {
            return new QueryResult(this, queryId);
        } catch (RuntimeException | Error e) {
            // The native query is already registered; if the Java wrapper fails to
            // construct, release it so it does not leak until engine.close().
            try {
                NativeEngine.resultClose(handle, queryId);
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
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
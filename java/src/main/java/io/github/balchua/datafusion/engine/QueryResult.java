package io.github.balchua.datafusion.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import io.github.balchua.datafusion.exception.DataFusionException;
import io.github.balchua.datafusion.exception.EngineClosedException;
import io.github.balchua.datafusion.exception.QueryCancelledException;

/**
 * A streaming Arrow result set. Batches are imported from the native engine one
 * at a
 * time; no rows are materialized as Java objects.
 *
 * <pre>{@code
 * try (QueryResult result = engine.query(sql)) {
 *     VectorSchemaRoot batch;
 *     while ((batch = result.next()) != null) {
 *         try (batch) {
 *             // read batch.getVector("amount") etc.
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p>
 * Ownership (docs/arrow-lifetime.md): each returned root wraps native memory without
 * copying; closing the root invokes the C Data Interface release callbacks, which free the
 * native buffers. A root that is never closed keeps its buffers allocated, and
 * {@link #close()} then fails in {@link org.apache.arrow.memory.RootAllocator#close()} with
 * {@link IllegalStateException} — Arrow's leak detection.
 *
 * <p>
 * A dictionary-encoded result column shares a single dictionary provider across batches, so
 * a returned batch must be fully consumed before calling {@link #next()} again.
 */
public final class QueryResult implements AutoCloseable {
    /** The native engine handle; the result does not hold the {@link DataFusionEngine}. */
    private final long engineHandle;
    private final long queryId;
    private final RootAllocator allocator;
    private final Schema schema;
    private final List<Field> fields;
    private final CDataDictionaryProvider dictionaryProvider = new CDataDictionaryProvider();
    // `closed` is atomic because `cancel()` is documented as safe to call from another
    // thread while the consuming thread drives `next()`/`close()`: the check-then-act in
    // `close()` must be a CAS, not a plain flag. `done` is only touched by the single
    // consuming thread, but is volatile for visibility via `stats()`-style reads.
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile boolean done;

    QueryResult(DataFusionEngine engine, long queryId) {
        this.engineHandle = engine.nativeHandle();
        this.queryId = queryId;
        RootAllocator newAllocator = new RootAllocator(Long.MAX_VALUE);
        Schema imported;
        // Import the result schema once, then reuse the Java Schema for every batch.
        try (ArrowSchema schemaStruct = ArrowSchema.allocateNew(newAllocator)) {
            NativeEngine.resultSchema(engineHandle, queryId, schemaStruct.memoryAddress());
            imported = Data.importSchema(newAllocator, schemaStruct, dictionaryProvider);
        } catch (RuntimeException e) {
            // Constructor failure means the caller never gets a reference to close(); free
            // the dictionary provider and allocator here, without masking the real error.
            try {
                dictionaryProvider.close();
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            } finally {
                try {
                    newAllocator.close();
                } catch (RuntimeException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw e;
        }
        this.allocator = newAllocator;
        this.schema = imported;
        this.fields = imported.getFields();
    }

    /** The result schema (same for every batch). */
    public Schema getSchema() {
        return schema;
    }

    /** Number of columns in the result. */
    public int columnCount() {
        return fields.size();
    }

    /**
     * Returns the next batch, or {@code null} at end of stream. The caller must
     * close
     * the returned root.
     *
     * @throws QueryCancelledException if the query was cancelled
     * @throws DataFusionException     for any execution error
     */
    @SuppressWarnings("java:S2093") // the returned root is the caller's to close, by design
    public VectorSchemaRoot next() {
        if (closed.get()) {
            throw new EngineClosedException("QueryResult is closed");
        }
        if (done) {
            return null;
        }
        int n = fields.size();
        long[] arrayAddrs = new long[n];
        ArrowArray[] arrayStructs = new ArrowArray[n];
        try {
            for (int i = 0; i < n; i++) {
                ArrowArray a = ArrowArray.allocateNew(allocator);
                arrayStructs[i] = a;
                arrayAddrs[i] = a.memoryAddress();
            }
            long rows = NativeEngine.resultNextBatch(engineHandle, queryId, arrayAddrs);
            if (rows == -1) {
                done = true;
                return null;
            }
            // Import per column: zero-copy wrap of the native buffers; closing the
            // vectors later triggers the native release callbacks.
            List<FieldVector> vectors = new ArrayList<>(n);
            try {
                for (int i = 0; i < n; i++) {
                    FieldVector vector = fields.get(i).createVector(allocator);
                    // Register the vector before the fallible import so the catch below
                    // cleans up the column whose import threw, not just earlier columns.
                    vectors.add(vector);
                    // closeImportedStructs=false: keep the Java ArrowArray wrapper open so the
                    // `finally` can release() any struct the importer did not consume.
                    Data.importIntoVector(
                            allocator, arrayStructs[i], vector, dictionaryProvider, false);
                }
                return new VectorSchemaRoot(vectors);
            } catch (RuntimeException e) {
                // A failure part-way (e.g. an incompatible column struct) leaves the
                // already-imported vectors holding real native buffers. Close them so the
                // failed batch cannot leak; the `finally` additionally releases the columns
                // that were never imported.
                for (FieldVector v : vectors) {
                    v.close();
                }
                throw e;
            }
        } finally {
            for (ArrowArray a : arrayStructs) {
                if (a != null) {
                    // release() before close(): a column whose import never ran still holds a
                    // live release callback. On success the importer has nulled the native
                    // release pointer, so release() is a no-op there.
                    a.release();
                    a.close();
                }
            }
        }
    }

    /**
     * Cancels the query. Safe to call from another thread at any time — including racing
     * the consuming thread's {@link #next()}/{@link #close()}, or after the engine has
     * been closed: cancelling a query that is already finished or gone is a no-op, never
     * an exception.
     */
    public void cancel() {
        if (closed.get()) {
            return;
        }
        NativeEngine.resultCancel(engineHandle, queryId);
    }

    /**
     * Producer-side statistics for benchmarking (JSON).
     *
     * @throws EngineClosedException if this result has already been closed
     */
    public String stats() {
        if (closed.get()) {
            throw new EngineClosedException("QueryResult is closed");
        }
        return NativeEngine.resultStats(engineHandle, queryId);
    }

    /**
     * Releases the native query and this result's allocator.
     *
     * <p>Idempotent and safe to call concurrently with {@link #cancel()}: the close flag is
     * a compare-and-set so the teardown runs exactly once. The allocator is closed even if
     * the native release throws, so a failed close cannot leak it. If a batch returned by
     * {@link #next()} was never closed,
     * {@link org.apache.arrow.memory.RootAllocator#close()} throws {@link IllegalStateException}
     * — that is Arrow's leak detection, not a bug.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            // Idempotent in native: after engine.close() this is a no-op.
            NativeEngine.resultClose(engineHandle, queryId);
        } finally {
            try {
                // The provider owns any imported dictionary vectors; close it before the
                // allocator so the allocator sees no outstanding children.
                dictionaryProvider.close();
            } finally {
                // Unconditional: a native failure must not strand the per-query allocator.
                allocator.close();
            }
        }
    }
}
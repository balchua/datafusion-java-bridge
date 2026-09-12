package io.github.balchua.datafusion.engine;

import java.util.ArrayList;
import java.util.List;

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
 * Ownership (docs/arrow-lifetime.md): each returned root wraps native memory
 * without
 * copying; closing the root invokes the C Data Interface release callbacks,
 * which free
 * the native buffers. Not closing returned roots leaks them until the engine is
 * dropped.
 */
public final class QueryResult implements AutoCloseable {
    private final DataFusionEngine engine;
    private final long queryId;
    private final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    private final Schema schema;
    private final List<Field> fields;
    private final CDataDictionaryProvider dictionaryProvider = new CDataDictionaryProvider();
    private boolean done;
    private boolean closed;

    QueryResult(DataFusionEngine engine, long queryId) {
        this.engine = engine;
        this.queryId = queryId;
        // Import the result schema once, then reuse the Java Schema for every batch.
        try (ArrowSchema schemaStruct = ArrowSchema.allocateNew(allocator)) {
            NativeEngine.resultSchema(engine.nativeHandle(), queryId, schemaStruct.memoryAddress());
            this.schema = Data.importSchema(allocator, schemaStruct, dictionaryProvider);
        }
        this.fields = schema.getFields();
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
    public VectorSchemaRoot next() {
        if (closed) {
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
            long rows = NativeEngine.resultNextBatch(engine.nativeHandle(), queryId, arrayAddrs);
            if (rows == -1) {
                done = true;
                return null;
            }
            // Import per column: zero-copy wrap of the native buffers; closing the
            // vectors later triggers the native release callbacks.
            List<FieldVector> vectors = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                FieldVector vector = fields.get(i).createVector(allocator);
                Data.importIntoVector(allocator, arrayStructs[i], vector, dictionaryProvider);
                vectors.add(vector);
            }
            return new VectorSchemaRoot(vectors);
        } finally {
            for (ArrowArray a : arrayStructs) {
                if (a != null) {
                    a.close();
                }
            }
        }
    }

    /**
     * Cancels the query. The next {@link #next()} throws QueryCancelledException.
     */
    public void cancel() {
        NativeEngine.resultCancel(engine.nativeHandle(), queryId);
    }

    /** Producer-side statistics for benchmarking (JSON). */
    public String stats() {
        return NativeEngine.resultStats(engine.nativeHandle(), queryId);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            NativeEngine.resultClose(engine.nativeHandle(), queryId);
            // Any imported vectors not closed by the caller are closed here via the
            // allocator; the allocator itself must be free of children now.
            allocator.close();
        }
    }
}
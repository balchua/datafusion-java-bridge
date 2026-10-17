package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.TestNativeAccess;
import io.github.balchua.datafusion.engine.QueryResult;
import io.github.balchua.datafusion.exception.ArrowBridgeException;
import io.github.balchua.datafusion.exception.DataFusionException;
import io.github.balchua.datafusion.exception.ObjectStoreException;
import io.github.balchua.datafusion.exception.SqlParseException;
import io.github.balchua.datafusion.exception.SqlUnsupportedException;
import io.github.balchua.datafusion.exception.TableNotFoundException;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Error paths must not leak native or Java memory, and must leave the engine usable.
 *
 * <p>Under zero-copy ingest the Arrow buffers are shared with Rust through the C Data
 * Interface, so any retained reference is visible to the Java {@link RootAllocator}. Once
 * every root is closed and the engine is closed (releasing Rust's references), the
 * allocator must report zero bytes — that is what catches an import that takes ownership
 * of the Java buffers and then fails without releasing them.
 */
class LeakTest {

    @Test
    void stageArrowColumnMismatchReleasesExportedArrays() {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
        try {
            try (SchoolData.Dataset data = SchoolData.build(allocator, "leak", 1, 1);
                 ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
                // Export a 4-field schema but only one ArrowArray: native import rejects
                // the column count before consuming any array, so Java must release them.
                Data.exportSchema(allocator, data.students().getSchema(), null, schema);
                ArrowArray array = ArrowArray.allocateNew(allocator);
                try {
                    Data.exportVector(allocator, data.students().getVector(0), null, array);
                    assertThrows(ArrowBridgeException.class, () -> TestNativeAccess.stageArrow(
                            TestNativeAccess.handle(engine), "t", schema.memoryAddress(),
                            new long[] {array.memoryAddress()}));
                } finally {
                    array.release();
                    array.close();
                }
            }
        } finally {
            engine.close();
        }
        assertEquals(0L, allocator.getAllocatedMemory(),
                "exported Arrow arrays leaked when native import rejected the batch");
        allocator.close();
    }

    @Test
    void repeatedFailedRegistrationsDoNotLeak() {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
        try {
            for (int i = 0; i < 25; i++) {
                try (SchoolData.Dataset data = SchoolData.build(allocator, "e" + i, 5, 10)) {
                    // Empty table name makes native staging fail after Java exported the
                    // schema and arrays, exercising the release-on-failure path.
                    assertThrows(DataFusionException.class,
                            () -> engine.registerArrow("", data.students()));
                }
            }
        } finally {
            engine.close(); // must not report a leaked structAllocator
        }
        assertEquals(0L, allocator.getAllocatedMemory(),
                "failed registrations leaked Java Arrow buffers");
        allocator.close();
    }

    @Test
    void failedQueriesDoNotLeakAndEngineStaysUsable() {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
        try {
            try (SchoolData.Dataset data = SchoolData.build(allocator, "q", 10, 40)) {
                engine.registerArrow(SchoolData.TABLE_STUDENTS, data.students());
                engine.registerArrow(SchoolData.TABLE_ENROLLMENTS, data.enrollments());

                assertThrows(SqlParseException.class, () -> engine.query("SELEC nope"));
                assertThrows(TableNotFoundException.class,
                        () -> engine.query("SELECT * FROM no_such_table"));
                assertThrows(SqlUnsupportedException.class,
                        () -> engine.query("DELETE FROM students"));

                // The engine must still work after the failures above.
                assertEquals(10L, scalarLong(engine, "SELECT COUNT(*) AS n FROM students"));
                assertEquals(40L, scalarLong(engine, "SELECT COUNT(*) AS n FROM enrollments"));
            }
        } finally {
            engine.close();
        }
        assertEquals(0L, allocator.getAllocatedMemory(), "failed queries leaked Arrow memory");
        allocator.close();
    }

    @Test
    void failedObjectStoreRegistrationLeavesEngineUsable() {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
        try {
            try (SchoolData.Dataset data = SchoolData.build(allocator, "p", 10, 40)) {
                engine.registerArrow(SchoolData.TABLE_STUDENTS, data.students());
                // s3:// with no S3 config fails before touching the network.
                assertThrows(ObjectStoreException.class,
                        () -> engine.registerParquet("remote", "s3://bucket/does-not-exist.parquet"));

                assertEquals(10L, scalarLong(engine, "SELECT COUNT(*) AS n FROM students"));
            }
        } finally {
            engine.close();
        }
        assertEquals(0L, allocator.getAllocatedMemory());
        allocator.close();
    }

    private static long scalarLong(DataFusionEngine engine, String sql) {
        try (QueryResult result = engine.query(sql)) {
            VectorSchemaRoot batch = result.next();
            try (batch) {
                return ((Number) batch.getVector(0).getObject(0)).longValue();
            }
        }
    }
}

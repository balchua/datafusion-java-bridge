package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;

import io.github.balchua.datafusion.exception.ArrowBridgeException;
import io.github.balchua.datafusion.exception.DataFusionException;
import io.github.balchua.datafusion.exception.EngineClosedException;
import io.github.balchua.datafusion.exception.SqlParseException;
import io.github.balchua.datafusion.exception.SqlUnsupportedException;
import io.github.balchua.datafusion.exception.TableNotFoundException;
import io.github.balchua.datafusion.engine.NativeEngine;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Typed error mapping (spec §27) and SQL safety (spec §14). */
class ErrorMappingTest {

    @Test
    void sqlSafetyRejectsDdlDmlAndCopy() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
            assertInstanceOf(SqlUnsupportedException.class, eOf(engine, "INSERT INTO t VALUES (1)"));
            assertInstanceOf(SqlUnsupportedException.class, eOf(engine, "DELETE FROM t"));
            assertInstanceOf(SqlUnsupportedException.class, eOf(engine, "UPDATE t SET a = 1"));
            assertInstanceOf(SqlUnsupportedException.class, eOf(engine, "CREATE TABLE t (a INT)"));
            assertInstanceOf(SqlUnsupportedException.class, eOf(engine, "DROP TABLE t"));
            assertInstanceOf(SqlUnsupportedException.class, eOf(engine, "COPY t TO '/tmp/out.parquet'"));
            assertInstanceOf(SqlUnsupportedException.class, eOf(engine, "SELECT 1; DROP TABLE t"));
            assertInstanceOf(SqlParseException.class, eOf(engine, "SELEC 1"));
            assertInstanceOf(SqlParseException.class, eOf(engine, ""));
        }
    }

    private static Exception eOf(DataFusionEngine engine, String sql) {
        try {
            engine.query(sql);
        } catch (DataFusionException e) {
            return e;
        }
        throw new AssertionError("no exception for: " + sql);
    }

    @Test
    void tableNotFound() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
            DataFusionException e = assertThrows(DataFusionException.class,
                    () -> engine.query("SELECT * FROM no_such_table"));
            assertInstanceOf(TableNotFoundException.class, e);
            assertTrue(e.getMessage().contains("not found"));
        }
    }

    @Test
    void unknownQueryIdThrowsEngineClosed() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
            DataFusionException e = assertThrows(DataFusionException.class,
                    () -> NativeEngine.resultNextBatch(engine.nativeHandle(), 999_999L, new long[0]));
            assertInstanceOf(EngineClosedException.class, e);
            assertTrue(e.getMessage().contains("not registered"));
        }
    }

    @Test
    void engineClosedAfterClose() {
        DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
        engine.close();
        assertThrows(EngineClosedException.class, () -> engine.query("SELECT 1"));
        // double close is safe
        engine.close();
    }
}
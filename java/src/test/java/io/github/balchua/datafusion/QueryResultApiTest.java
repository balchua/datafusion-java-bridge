package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;

import io.github.balchua.datafusion.exception.EngineClosedException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Teardown and argument-validation contract of {@link QueryResult} and the engine API,
 * including the state that is otherwise only exercised implicitly by other tests.
 */
class QueryResultApiTest {

    @Test
    void nextAfterCloseThrows() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
            QueryResult result = engine.query("SELECT 1 AS n");
            result.close();
            assertThrows(EngineClosedException.class, result::next);
        }
    }

    @Test
    void statsAfterCloseThrows() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
            QueryResult result = engine.query("SELECT 1 AS n");
            result.close();
            assertThrows(EngineClosedException.class, result::stats);
        }
    }

    @Test
    void cancelAfterCloseIsANoOp() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
            QueryResult result = engine.query("SELECT 1 AS n");
            result.close();
            // A watchdog thread cancelling after teardown must not throw.
            assertDoesNotThrow(result::cancel);
        }
    }

    @Test
    void closeIsIdempotent() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
            QueryResult result = engine.query("SELECT 1 AS n");
            result.close();
            assertDoesNotThrow(result::close);
        }
    }

    @Test
    void concurrentCancelAndCloseNeverThrows() throws Exception {
        // The exact race the cancel() javadoc claims is safe: a watchdog cancelling while
        // the consumer closes. Repeated to make the interleaving likely.
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
            for (int i = 0; i < 200; i++) {
                QueryResult result = engine.query("SELECT 1 AS n");
                AtomicReference<Throwable> failure = new AtomicReference<>();
                CountDownLatch start = new CountDownLatch(1);
                Thread canceller = new Thread(() -> {
                    try {
                        start.await();
                        result.cancel();
                    } catch (Throwable t) {
                        failure.set(t);
                    }
                });
                canceller.start();
                start.countDown();
                try {
                    result.close();
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
                canceller.join(5000);
                assertFalse(canceller.isAlive(), "canceller thread hung");
                assertNull(failure.get(), "cancel/close race threw: " + failure.get());
            }
        }
    }

    @Test
    void nullArgumentsAreRejected() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
            assertThrows(NullPointerException.class, () -> engine.query(null));
            assertThrows(NullPointerException.class,
                    () -> engine.queryToParquet(null, "/tmp/opencode/x.parquet"));
            assertThrows(NullPointerException.class,
                    () -> engine.queryToParquet("SELECT 1", null));
        }
    }
}

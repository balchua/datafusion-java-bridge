package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;

import io.github.balchua.datafusion.exception.DataFusionException;
import io.github.balchua.datafusion.exception.QueryCancelledException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cancellation: aborting a long-running query from another thread (spec §28). */
class CancelTest {

    @Test
    void cancelHeavyQueryWhileRunning() throws Exception {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
             VectorSchemaRoot message = TestPayments.messageRoot("MSG-1", 1000, allocator);
             VectorSchemaRoot remittances = TestPayments.remittancesRoot("MSG-1", 2, allocator);
             VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", 200_000, allocator)) {
            engine.registerArrow(PaymentSchemas.TABLE_MESSAGE, message);
            engine.registerArrow(PaymentSchemas.TABLE_REMITTANCES, remittances);
            engine.registerArrow(PaymentSchemas.TABLE_TRANSACTIONS, transactions);

            // Real execution (a bare COUNT(*) over a cross join is answered from
            // metadata by DataFusion's optimizer); this theta join must scan 40B pairs.
            QueryResult result = engine.query(
                    "SELECT COUNT(*) AS n FROM transactions a, transactions b "
                            + "WHERE a.amount + b.amount > 0");

            long started = System.nanoTime();
            Thread canceller = new Thread(() -> {
                try {
                    Thread.sleep(300);
                    result.cancel();
                } catch (Exception ignored) {
                }
            });
            canceller.start();

            // The consuming loop must terminate quickly with QueryCancelledException.
            long start = System.nanoTime();
            DataFusionException thrown = null;
            try {
                VectorSchemaRoot batch;
                while ((batch = result.next()) != null) {
                    batch.close();
                }
            } catch (DataFusionException e) {
                thrown = e;
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            result.close();

            assertTrue(thrown instanceof QueryCancelledException,
                    "expected cancellation, got " + thrown);
            assertTrue(elapsedMs < 30_000,
                    "cancelled query should end promptly, took " + elapsedMs + "ms");
            canceller.join(5000);
        }
    }
}
package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Concurrency: multiple concurrent queries over one shared engine (spec §13/§29). */
class ConcurrencyTest {

    @Test
    void concurrentQueriesOnSharedEngine() throws Exception {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
             VectorSchemaRoot message = TestPayments.messageRoot("MSG-1", 500, allocator);
             VectorSchemaRoot remittances = TestPayments.remittancesRoot("MSG-1", 20, allocator);
             VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", 500, allocator)) {
            engine.registerArrow(PaymentSchemas.TABLE_MESSAGE, message);
            engine.registerArrow(PaymentSchemas.TABLE_REMITTANCES, remittances);
            engine.registerArrow(PaymentSchemas.TABLE_TRANSACTIONS, transactions);

            ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
            CountDownLatch ready = new CountDownLatch(4);
            List<java.util.concurrent.Future<Integer>> futures = new java.util.ArrayList<>();
            for (int w = 0; w < 4; w++) {
                futures.add(pool.submit(() -> {
                    try (QueryResult result = engine.query(
                            "SELECT COUNT(*) AS n FROM transactions WHERE debtor.name = '"
                                    + TestPayments.MATCH_DEBTOR + "'")) {
                        ready.countDown();
                        VectorSchemaRoot batch = result.next();
                        try (batch) {
                            Object n = batch.getVector(0).getObject(0);
                            return ((Number) n).intValue();
                        }
                    }
                }));
            }
            assertTrue(ready.await(30, TimeUnit.SECONDS));
            long expected = 0;
            for (int r = 0; r < 500; r++) {
                if (TestPayments.partyName(r).equals(TestPayments.MATCH_DEBTOR)) {
                    expected++;
                }
            }
            for (java.util.concurrent.Future<Integer> f : futures) {
                assertEquals((int) expected, f.get(60, TimeUnit.SECONDS));
            }
            pool.shutdown();
        }
    }
}
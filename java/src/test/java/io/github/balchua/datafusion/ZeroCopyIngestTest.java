package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Zero-copy ingest ownership test. With {@code zeroCopyIngest(true)} Rust wraps the Java
 * Arrow buffers via the C Data Interface instead of deep-copying them, so:
 *
 * <ul>
 *   <li>the Java root may be closed right after registration (Arrow's exporter retains the
 *       buffers; Rust releases them when the table is dropped), and</li>
 *   <li>queries must return exactly the same values as the deep-copy mode.</li>
 * </ul>
 *
 * The engine is closed (releasing the Rust references) before the allocator is closed.
 */
class ZeroCopyIngestTest {

    @Test
    void zeroCopyIngestKeepsDataValidAfterJavaRootClosed() {
        EngineConfig config = EngineConfig.builder().zeroCopyIngest(true).build();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             DataFusionEngine engine = new DataFusionEngine(config)) {

            VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", 5000, allocator);
            engine.registerArrow(PaymentSchemas.TABLE_TRANSACTIONS, transactions);
            // Close the Java root immediately: the exported buffers are retained by the
            // C Data Interface owner and must remain readable by DataFusion.
            transactions.close();

            try (QueryResult result = engine.query(
                    "SELECT COUNT(*) AS n FROM transactions WHERE debtor.name = 'BBB'")) {
                VectorSchemaRoot batch = result.next();
                try (batch) {
                    assertEquals(5000 / 26 + 1, ((Number) batch.getVector(0).getObject(0)).intValue());
                }
            }

            // Deep nested struct + Decimal128 still intact through the zero-copy path.
            try (QueryResult result = engine.query(
                    "SELECT amount, debtor.account.iban AS iban FROM transactions "
                            + "WHERE transaction_id = 'TXN-0'")) {
                VectorSchemaRoot batch = result.next();
                try (batch) {
                    assertEquals("DE02999999990000009999",
                            batch.getVector("iban").getObject(0).toString());
                    assertEquals(new java.math.BigDecimal("0.1000"),
                            batch.getVector("amount").getObject(0));
                }
            }
        }
    }
}
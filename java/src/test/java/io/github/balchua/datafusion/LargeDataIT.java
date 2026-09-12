package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Large-data correctness: 100K and 1M rows in a single RecordBatch (spec §29).
 * Tagged 'large' — excluded from default runs, enabled with `mvn test -Plarge`.
 */
@Tag("large")
class LargeDataIT {

    @TempDir
    Path tempDir;

    @Test
    void hundredThousandTransactionsSingleBatch() {
        runPaymentValidation(100_000);
    }

    @Test
    @Tag("large")
    void oneMillionTransactionsSingleBatch() {
        runPaymentValidation(1_000_000);
    }

    private void runPaymentValidation(int rows) {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        try (DataFusionEngine engine = new DataFusionEngine(
                EngineConfig.builder().memoryLimitBytes(4L * 1024 * 1024 * 1024).build());
             VectorSchemaRoot message = TestPayments.messageRoot("MSG-1", rows, allocator);
             VectorSchemaRoot remittances = TestPayments.remittancesRoot(
                     "MSG-1", (rows + 24) / 25, rows, allocator);
             VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", rows, allocator)) {
            engine.registerArrow(PaymentSchemas.TABLE_MESSAGE, message);
            engine.registerArrow(PaymentSchemas.TABLE_REMITTANCES, remittances);
            engine.registerArrow(PaymentSchemas.TABLE_TRANSACTIONS, transactions);

            // Spec §21 Q7: per-remittance control validation report.
            try (QueryResult result = engine.query(
                    "SELECT r.remittance_id AS rid, "
                            + "COUNT(t.transaction_id) AS actual, "
                            + "r.number_of_transactions AS declared, "
                            + "SUM(t.amount) AS txn_sum "
                            + "FROM remittances r LEFT JOIN transactions t "
                            + "ON t.remittance_id = r.remittance_id "
                            + "GROUP BY r.remittance_id, r.number_of_transactions "
                            + "ORDER BY rid")) {
                long remittancesChecked = 0;
                VectorSchemaRoot batch;
                while ((batch = result.next()) != null) {
                    remittancesChecked += batch.getRowCount();
                    batch.close();
                }
                assertEquals((rows + 24) / 25, (int) remittancesChecked);
            }
        }
    }
}
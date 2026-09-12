package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;

import io.github.balchua.datafusion.exception.DataFusionException;
import io.github.balchua.datafusion.exception.ParquetException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Parquet export -> import -> round trip, incl. nested types and Decimal128 (spec §29). */
class ParquetTest {

    @TempDir
    Path tempDir;

    @Test
    void exportImportRoundTripPreservesTypesAndValues() throws Exception {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
             VectorSchemaRoot message = TestPayments.messageRoot("MSG-1", 10, allocator);
             VectorSchemaRoot remittances = TestPayments.remittancesRoot("MSG-1", 1, allocator);
             VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", 100, allocator)) {
            engine.registerArrow(PaymentSchemas.TABLE_MESSAGE, message);
            engine.registerArrow(PaymentSchemas.TABLE_REMITTANCES, remittances);
            engine.registerArrow(PaymentSchemas.TABLE_TRANSACTIONS, transactions);

            Path out = tempDir.resolve("transactions.parquet");
            engine.queryToParquet(
                    "SELECT transaction_id, amount, debtor, structured_remittance FROM transactions ORDER BY transaction_id",
                    out.toString());
            assertTrue(Files.exists(out));
            assertTrue(Files.size(out) > 0);

            engine.registerParquet("txn_rt", out.toString());

            // Decimal128 survives the round trip exactly.
            try (QueryResult result = engine.query(
                    "SELECT transaction_id, amount, debtor.name AS dn FROM txn_rt")) {
                Set<String> actualIds = new HashSet<>();
                int rows = 0;
                VectorSchemaRoot batch;
                while ((batch = result.next()) != null) {
                    try (VectorSchemaRoot current = batch) {
                        org.apache.arrow.vector.ValueVector batchIds = current.getVector("transaction_id");
                        org.apache.arrow.vector.ValueVector amounts = current.getVector("amount");
                        org.apache.arrow.vector.ValueVector names = current.getVector("dn");
                        for (int i = 0; i < current.getRowCount(); i++) {
                            String id = batchIds.getObject(i).toString();
                            int r = Integer.parseInt(id.substring("TXN-".length()));
                            BigDecimal expected = BigDecimal.valueOf(1000L + r % 9000, 4)
                                    .setScale(4, RoundingMode.UNNECESSARY);
                            assertEquals(expected, amounts.getObject(i));
                            assertEquals(TestPayments.partyName(r), names.getObject(i).toString());
                            actualIds.add(id);
                        }
                        rows += current.getRowCount();
                    }
                }
                assertEquals(100, rows);
                Set<String> expectedIds = new HashSet<>();
                for (int r = 0; r < 100; r++) {
                    expectedIds.add("TXN-" + r);
                }
                assertEquals(expectedIds, actualIds);
            }

            // Nested struct survives: deep filter over the imported table.
            try (QueryResult result = engine.query(
                    "SELECT COUNT(*) AS n FROM txn_rt WHERE debtor.account.iban = '"
                            + TestPayments.MATCH_IBAN + "'")) {
                VectorSchemaRoot batch = result.next();
                try (batch) {
                    assertEquals(1L, batch.getVector(0).getObject(0));
                }
            }
        }
    }

    @Test
    void importMissingFileThrowsParquetError() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
            DataFusionException e = assertThrows(DataFusionException.class,
                    () -> engine.registerParquet("missing", tempDir.resolve("does_not_exist.parquet").toString()));
            // Registration performs schema inference on the file and fails there.
            assertInstanceOf(ParquetException.class, e);
        }
    }
}
package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** End-to-end: Java Arrow -> JNI/Rust -> DataFusion SQL -> Arrow results (spec §29). */
class EngineTest {
    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private void registerPayment(DataFusionEngine engine, VectorSchemaRoot message,
                                 VectorSchemaRoot remittances, VectorSchemaRoot transactions) {
        engine.registerArrow(PaymentSchemas.TABLE_MESSAGE, message);
        engine.registerArrow(PaymentSchemas.TABLE_REMITTANCES, remittances);
        engine.registerArrow(PaymentSchemas.TABLE_TRANSACTIONS, transactions);
    }

    @Test
    void projection() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
             VectorSchemaRoot message = TestPayments.messageRoot("MSG-1", 50, allocator);
             VectorSchemaRoot remittances = TestPayments.remittancesRoot("MSG-1", 2, allocator);
             VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", 50, allocator)) {
            registerPayment(engine, message, remittances, transactions);

            try (QueryResult result = engine.query("SELECT transaction_id FROM transactions")) {
                assertEquals(1, result.columnCount());
                int rows = 0;
                VectorSchemaRoot batch;
                while ((batch = result.next()) != null) {
                    try (VectorSchemaRoot current = batch) {
                        VarCharVector ids = (VarCharVector) current.getVector("transaction_id");
                        assertEquals("TXN-" + rows, new String(ids.get(0)));
                        rows += current.getRowCount();
                    }
                }
                assertEquals(50, rows);
            }
        }
    }

    @Test
    void nestedProjection() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
             VectorSchemaRoot message = TestPayments.messageRoot("MSG-1", 50, allocator);
             VectorSchemaRoot remittances = TestPayments.remittancesRoot("MSG-1", 2, allocator);
             VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", 50, allocator)) {
            registerPayment(engine, message, remittances, transactions);

            try (QueryResult result = engine.query(
                    "SELECT transaction_id, debtor.name AS debtor_name "
                            + "FROM transactions ORDER BY transaction_id LIMIT 3")) {
                VectorSchemaRoot batch = result.next();
                try (batch) {
                    assertEquals(3, batch.getRowCount());
                    VarCharVector ids = (VarCharVector) batch.getVector("transaction_id");
                    VarCharVector names = (VarCharVector) batch.getVector("debtor_name");
                    assertEquals("TXN-0", new String(ids.get(0)));
                    assertEquals(TestPayments.partyName(0), names.getObject(0).toString());
                    assertEquals("TXN-10", new String(ids.get(2))); // lexicographic order
                }
            }
        }
    }

    @Test
    void nestedFilter() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
             VectorSchemaRoot message = TestPayments.messageRoot("MSG-1", 50, allocator);
             VectorSchemaRoot remittances = TestPayments.remittancesRoot("MSG-1", 2, allocator);
             VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", 52, allocator)) {
            registerPayment(engine, message, remittances, transactions);

            try (QueryResult result = engine.query(
                    "SELECT transaction_id, debtor.name AS debtor_name "
                            + "FROM transactions WHERE debtor.name = 'BBB' ORDER BY transaction_id")) {
                int rows = 0;
                VectorSchemaRoot batch;
                while ((batch = result.next()) != null) {
                    try (VectorSchemaRoot current = batch) {
                        rows += current.getRowCount();
                        VarCharVector names = (VarCharVector) current.getVector("debtor_name");
                        for (int i = 0; i < current.getRowCount(); i++) {
                            assertEquals("BBB", names.getObject(i).toString());
                        }
                    }
                }
                // partyName(r) has second char 'B' iff r % 26 == 1 -> rows 25 and 51
                assertEquals(2, rows);
            }
        }
    }

    @Test
    void deepNestedFilter() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
             VectorSchemaRoot message = TestPayments.messageRoot("MSG-1", 50, allocator);
             VectorSchemaRoot remittances = TestPayments.remittancesRoot("MSG-1", 2, allocator);
             VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", 1200, allocator)) {
            registerPayment(engine, message, remittances, transactions);

            try (QueryResult result = engine.query(
                    "SELECT transaction_id FROM transactions WHERE debtor.account.iban = '"
                            + TestPayments.MATCH_IBAN + "'")) {
                int rows = 0;
                VectorSchemaRoot batch;
                while ((batch = result.next()) != null) {
                    try (VectorSchemaRoot current = batch) {
                        rows += current.getRowCount();
                    }
                }
                assertEquals(2, rows); // rows 0 and 1000
            }
        }
    }

    @Test
    void aggregation() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
             VectorSchemaRoot message = TestPayments.messageRoot("MSG-1", 50, allocator);
             VectorSchemaRoot remittances = TestPayments.remittancesRoot("MSG-1", 2, allocator);
             VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", 400, allocator)) {
            registerPayment(engine, message, remittances, transactions);

            try (QueryResult result = engine.query(
                    "SELECT currency, COUNT(*) AS n, SUM(amount) AS total "
                            + "FROM transactions GROUP BY currency ORDER BY currency")) {
                VectorSchemaRoot batch = result.next();
                try (batch) {
                    VarCharVector cur = (VarCharVector) batch.getVector("currency");
                    BigIntVector n = (BigIntVector) batch.getVector("n");
                    DecimalVector total = (DecimalVector) batch.getVector("total");
                    assertEquals(4, batch.getRowCount());
                    assertEquals("CHF", cur.getObject(0).toString());
                    assertEquals("EUR", cur.getObject(1).toString());
                    assertEquals("GBP", cur.getObject(2).toString());
                    assertEquals("USD", cur.getObject(3).toString());
                    String[] currencies = {"EUR", "USD", "GBP", "CHF"};
                    for (int i = 0; i < 4; i++) {
                        String group = cur.getObject(i).toString();
                        assertEquals(100L, n.getObject(i));
                        BigDecimal expected = BigDecimal.ZERO;
                        for (int r = 0; r < 400; r++) {
                            if (currencies[r % currencies.length].equals(group)) {
                                expected = expected.add(BigDecimal.valueOf(1000L + r % 9000, 4));
                            }
                        }
                        assertEquals(expected, total.getObject(i));
                    }
                }
            }
        }
    }

    @Test
    void joinAllThreeTables() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
             VectorSchemaRoot message = TestPayments.messageRoot("MSG-1", 100, allocator);
             VectorSchemaRoot remittances = TestPayments.remittancesRoot("MSG-1", 4, allocator);
             VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", 100, allocator)) {
            registerPayment(engine, message, remittances, transactions);

            try (QueryResult result = engine.query(
                    "SELECT m.message_id, r.remittance_id, COUNT(t.transaction_id) AS txn_count "
                            + "FROM payment_message m "
                            + "JOIN remittances r ON r.message_id = m.message_id "
                            + "JOIN transactions t ON t.remittance_id = r.remittance_id "
                            + "GROUP BY m.message_id, r.remittance_id "
                            + "ORDER BY r.remittance_id")) {
                VectorSchemaRoot batch = result.next();
                try (batch) {
                    assertEquals(4, batch.getRowCount());
                    for (int i = 0; i < 4; i++) {
                        assertEquals(25L, batch.getVector("txn_count").getObject(i));
                    }
                }
            }
        }
    }

    @Test
    void decimalSumIsExact() {
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
             VectorSchemaRoot message = TestPayments.messageRoot("MSG-1", 10, allocator);
             VectorSchemaRoot remittances = TestPayments.remittancesRoot("MSG-1", 1, allocator);
             VectorSchemaRoot transactions = TestPayments.transactionsRoot("MSG-1", 10, allocator)) {
            registerPayment(engine, message, remittances, transactions);

            try (QueryResult result = engine.query("SELECT SUM(amount) AS s FROM transactions")) {
                VectorSchemaRoot batch = result.next();
                try (batch) {
                    DecimalVector s = (DecimalVector) batch.getVector(0);
                    BigDecimal total = s.getObject(0);
                    BigDecimal expected = BigDecimal.ZERO;
                    for (int r = 0; r < 10; r++) {
                        expected = expected.add(BigDecimal.valueOf(1000L + r % 9000, 4));
                    }
                    assertEquals(expected, total);
                }
            }
        }
    }
}
package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Engine lifecycle: creation, registration, repeated create/destroy (spec §29). */
class LifecycleTest {

    @Test
    void repeatedCreateDestroy() {
        for (int i = 0; i < 10; i++) {
            BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            try {
                DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
                try (VectorSchemaRoot root = VectorSchemaRoot.create(
                        PaymentSchemas.messageSchema(), allocator)) {
                    VarCharVector id = (VarCharVector) root.getVector("message_id");
                    id.setSafe(0, ("M-" + i).getBytes());
                    ((org.apache.arrow.vector.IntVector) root.getVector("number_of_transactions"))
                            .setSafe(0, 1);
                    root.setRowCount(1);
                    engine.registerArrow(PaymentSchemas.TABLE_MESSAGE, root);
                    try (QueryResult r = engine.query("SELECT count(*) AS n FROM payment_message")) {
                        VectorSchemaRoot batch = r.next();
                        try (batch) {
                            assertEquals(1L, batch.getVector(0).getObject(0));
                        }
                    }
                } finally {
                    engine.close();
                }
            } finally {
                allocator.close();
            }
        }
    }

    @Test
    void closingResultTwiceIsSafe() {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
             VectorSchemaRoot root = TestPayments.messageRoot("MSG-1", 1, allocator)) {
            engine.registerArrow(PaymentSchemas.TABLE_MESSAGE, root);
            QueryResult r = engine.query("SELECT count(*) AS n FROM payment_message");
            VectorSchemaRoot batch = r.next();
            if (batch != null) {
                batch.close();
            }
            r.close();
            r.close(); // idempotent
        }
    }
}
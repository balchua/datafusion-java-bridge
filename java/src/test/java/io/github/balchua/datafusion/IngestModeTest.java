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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The engine's default ingest mode is zero-copy; deep-copy remains available as an opt-out
 * for callers that reuse/refill their VectorSchemaRoot after registering, or that want to
 * free the Java buffers before the table is dropped.
 */
class IngestModeTest {

    @Test
    void zeroCopyIsTheDefault() {
        assertTrue(EngineConfig.defaults().zeroCopy(),
                "zero-copy should be the default ingest mode");
    }

    @Test
    void deepCopyOptOutAllowsImmediateRootReuse() {
        EngineConfig config = EngineConfig.builder().zeroCopyIngest(false).build();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             DataFusionEngine engine = new DataFusionEngine(config)) {

            // Register a batch, then REUSE the very same root for fresh data. Under
            // deep-copy the engine owns its own copy, so the first table must be unaffected.
            VectorSchemaRoot root = TestPayments.messageRoot("MSG-1", 10, allocator);
            engine.registerArrow(PaymentSchemas.TABLE_MESSAGE, root);

            // clear/refill the same root with different content
            root.clear();
            VarCharVector id = (VarCharVector) root.getVector("message_id");
            VarCharVector type = (VarCharVector) root.getVector("message_type");
            ((org.apache.arrow.vector.IntVector) root.getVector("number_of_transactions")).setSafe(0, 999);
            id.setSafe(0, "MSG-2".getBytes());
            type.setSafe(0, "pain.001.001.11".getBytes());
            root.setRowCount(1);

            try (QueryResult result = engine.query(
                    "SELECT message_id FROM payment_message")) {
                VectorSchemaRoot batch = result.next();
                try (batch) {
                    // Still the first registration's value, not the refilled one.
                    assertEquals("MSG-1", batch.getVector(0).getObject(0).toString());
                }
            }
            root.close();
        }
    }
}
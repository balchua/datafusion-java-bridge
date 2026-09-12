package io.github.balchua.datafusion.bench.pain;

import org.apache.arrow.vector.VectorSchemaRoot;

/** The three parsed pain.001 Arrow tables; the caller owns their lifecycle. */
public record PainTables(VectorSchemaRoot message,
                         VectorSchemaRoot remittance,
                         VectorSchemaRoot transaction,
                         long messageRows,
                         long remittanceRows,
                         long transactionRows) implements AutoCloseable {
    @Override
    public void close() {
        message.close();
        remittance.close();
        transaction.close();
    }
}

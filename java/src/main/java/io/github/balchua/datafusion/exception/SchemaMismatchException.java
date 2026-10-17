package io.github.balchua.datafusion.exception;

/**
 * Thrown when Arrow data or a Parquet file does not match the expected schema, or when a
 * table name is empty or a table is registered twice.
 */
public class SchemaMismatchException extends DataFusionException {
    private static final long serialVersionUID = 1L;

    public SchemaMismatchException(String message) {
        super(message);
    }

    public SchemaMismatchException(String message, Throwable cause) {
        super(message, cause);
    }
}

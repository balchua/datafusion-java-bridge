package io.github.balchua.datafusion.exception;

/**
 * Thrown when SQL parses but is not permitted: DDL, DML, COPY, multi-statement strings,
 * and {@code SELECT ... INTO} are all rejected because this bridge is read-only.
 */
public class SqlUnsupportedException extends DataFusionException {
    private static final long serialVersionUID = 1L;

    public SqlUnsupportedException(String message) {
        super(message);
    }

    public SqlUnsupportedException(String message, Throwable cause) {
        super(message, cause);
    }
}

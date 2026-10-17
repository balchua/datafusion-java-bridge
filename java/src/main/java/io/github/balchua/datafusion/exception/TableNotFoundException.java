package io.github.balchua.datafusion.exception;

/** Thrown when a query references a table that does not exist. */
public class TableNotFoundException extends DataFusionException {
    private static final long serialVersionUID = 1L;

    public TableNotFoundException(String message) {
        super(message);
    }

    public TableNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}

package io.github.balchua.datafusion.exception;

/** TableNotFoundException. Thrown by the native engine; see docs/architecture.md for the error map. */
public class TableNotFoundException extends DataFusionException {
    private static final long serialVersionUID = 1L;
    public TableNotFoundException(String message) {
        super(message);
    }
}

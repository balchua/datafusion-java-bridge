package io.github.balchua.datafusion.exception;

/** SchemaMismatchException. Thrown by the native engine; see docs/architecture.md for the error map. */
public class SchemaMismatchException extends DataFusionException {
    private static final long serialVersionUID = 1L;
    public SchemaMismatchException(String message) {
        super(message);
    }
}

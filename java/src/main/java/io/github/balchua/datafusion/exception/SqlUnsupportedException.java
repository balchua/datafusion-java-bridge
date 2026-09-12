package io.github.balchua.datafusion.exception;

/** SqlUnsupportedException. Thrown by the native engine; see docs/architecture.md for the error map. */
public class SqlUnsupportedException extends DataFusionException {
    private static final long serialVersionUID = 1L;
    public SqlUnsupportedException(String message) {
        super(message);
    }
}

package io.github.balchua.datafusion.exception;

/** SqlParseException. Thrown by the native engine; see docs/architecture.md for the error map. */
public class SqlParseException extends DataFusionException {
    private static final long serialVersionUID = 1L;
    public SqlParseException(String message) {
        super(message);
    }
}

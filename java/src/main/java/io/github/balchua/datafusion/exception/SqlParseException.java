package io.github.balchua.datafusion.exception;

/** Thrown when SQL text cannot be parsed (syntax error, empty statement). */
public class SqlParseException extends DataFusionException {
    private static final long serialVersionUID = 1L;

    public SqlParseException(String message) {
        super(message);
    }

    public SqlParseException(String message, Throwable cause) {
        super(message, cause);
    }
}

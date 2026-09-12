package io.github.balchua.datafusion.exception;

/** Base exception for all errors raised by the native DataFusion engine. */
public class DataFusionException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public DataFusionException(String message) {
        super(message);
    }

    public DataFusionException(String message, Throwable cause) {
        super(message, cause);
    }
}

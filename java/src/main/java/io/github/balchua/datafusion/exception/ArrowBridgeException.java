package io.github.balchua.datafusion.exception;

/**
 * Thrown when data cannot cross the Arrow C Data Interface: import/export of a schema,
 * record batch, or column failed (e.g. a null pointer, column-count mismatch, or an
 * unsupported type).
 */
public class ArrowBridgeException extends DataFusionException {
    private static final long serialVersionUID = 1L;

    public ArrowBridgeException(String message) {
        super(message);
    }

    public ArrowBridgeException(String message, Throwable cause) {
        super(message, cause);
    }
}

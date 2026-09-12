package io.github.balchua.datafusion.exception;

/** ArrowBridgeException. Thrown by the native engine; see docs/architecture.md for the error map. */
public class ArrowBridgeException extends DataFusionException {
    private static final long serialVersionUID = 1L;
    public ArrowBridgeException(String message) {
        super(message);
    }
}

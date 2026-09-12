package io.github.balchua.datafusion.exception;

/** ObjectStoreException. Thrown by the native engine; see docs/architecture.md for the error map. */
public class ObjectStoreException extends DataFusionException {
    private static final long serialVersionUID = 1L;
    public ObjectStoreException(String message) {
        super(message);
    }
}

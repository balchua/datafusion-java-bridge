package io.github.balchua.datafusion.exception;

/** ResourceException. Thrown by the native engine; see docs/architecture.md for the error map. */
public class ResourceException extends DataFusionException {
    private static final long serialVersionUID = 1L;
    public ResourceException(String message) {
        super(message);
    }
}

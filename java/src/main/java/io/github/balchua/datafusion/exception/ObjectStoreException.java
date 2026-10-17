package io.github.balchua.datafusion.exception;

/**
 * Thrown when an object-store operation or its configuration fails, e.g. an {@code s3://}
 * URL used without S3 configuration, an unsupported URL scheme, or missing credentials.
 */
public class ObjectStoreException extends DataFusionException {
    private static final long serialVersionUID = 1L;

    public ObjectStoreException(String message) {
        super(message);
    }

    public ObjectStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}

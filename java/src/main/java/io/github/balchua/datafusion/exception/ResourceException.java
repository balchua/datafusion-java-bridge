package io.github.balchua.datafusion.exception;

/**
 * Thrown when the engine exhausts a configured resource, typically the DataFusion memory
 * pool limit (spillable operators only — see docs/memory.md).
 */
public class ResourceException extends DataFusionException {
    private static final long serialVersionUID = 1L;

    public ResourceException(String message) {
        super(message);
    }

    public ResourceException(String message, Throwable cause) {
        super(message, cause);
    }
}

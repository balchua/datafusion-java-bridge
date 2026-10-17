package io.github.balchua.datafusion.exception;

/**
 * Thrown when an operation targets an engine, result, or native handle that has already
 * been closed. The native engine treats a close that races another close as a no-op; this
 * is raised for operations that genuinely need a live engine (e.g. {@code query} after
 * {@link io.github.balchua.datafusion.engine.DataFusionEngine#close()}).
 */
public class EngineClosedException extends DataFusionException {
    private static final long serialVersionUID = 1L;

    public EngineClosedException(String message) {
        super(message);
    }

    public EngineClosedException(String message, Throwable cause) {
        super(message, cause);
    }
}

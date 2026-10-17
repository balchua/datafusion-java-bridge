package io.github.balchua.datafusion.exception;

/**
 * Thrown when a result method references a query id that is not registered — an unknown
 * id, or a query whose result has already been closed. Distinct from
 * {@link EngineClosedException}, which means the whole engine is gone.
 */
public class QueryNotFoundException extends DataFusionException {
    private static final long serialVersionUID = 1L;

    public QueryNotFoundException(String message) {
        super(message);
    }

    public QueryNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}

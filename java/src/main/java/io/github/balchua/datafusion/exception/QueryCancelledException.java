package io.github.balchua.datafusion.exception;

/**
 * Thrown when a query is cancelled — either explicitly via
 * {@link io.github.balchua.datafusion.engine.QueryResult#cancel()} or because its engine
 * was closed while it was running.
 */
public class QueryCancelledException extends DataFusionException {
    private static final long serialVersionUID = 1L;

    public QueryCancelledException(String message) {
        super(message);
    }

    public QueryCancelledException(String message, Throwable cause) {
        super(message, cause);
    }
}

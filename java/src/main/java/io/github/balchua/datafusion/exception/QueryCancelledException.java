package io.github.balchua.datafusion.exception;

/** QueryCancelledException. Thrown by the native engine; see docs/architecture.md for the error map. */
public class QueryCancelledException extends DataFusionException {
    private static final long serialVersionUID = 1L;
    public QueryCancelledException(String message) {
        super(message);
    }
}

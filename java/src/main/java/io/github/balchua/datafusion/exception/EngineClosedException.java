package io.github.balchua.datafusion.exception;

/** EngineClosedException. Thrown by the native engine; see docs/architecture.md for the error map. */
public class EngineClosedException extends DataFusionException {
    private static final long serialVersionUID = 1L;
    public EngineClosedException(String message) {
        super(message);
    }
}

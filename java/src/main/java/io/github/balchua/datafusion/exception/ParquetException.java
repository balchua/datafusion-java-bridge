package io.github.balchua.datafusion.exception;

/** ParquetException. Thrown by the native engine; see docs/architecture.md for the error map. */
public class ParquetException extends DataFusionException {
    private static final long serialVersionUID = 1L;
    public ParquetException(String message) {
        super(message);
    }
}

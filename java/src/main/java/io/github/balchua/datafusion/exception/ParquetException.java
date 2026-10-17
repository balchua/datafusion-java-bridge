package io.github.balchua.datafusion.exception;

/** Thrown when a Parquet file cannot be read, encoded, or written. */
public class ParquetException extends DataFusionException {
    private static final long serialVersionUID = 1L;

    public ParquetException(String message) {
        super(message);
    }

    public ParquetException(String message, Throwable cause) {
        super(message, cause);
    }
}

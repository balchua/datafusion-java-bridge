package io.github.balchua.datafusion.exception;

/**
 * Base exception for all errors raised by the native DataFusion engine.
 *
 * <p><b>Sensitivity:</b> messages originate in native code (DataFusion, `object_store`, the
 * OS) and may include filesystem paths, object-store URLs, bucket/key names, SQL text, or
 * table names. Do not surface them verbatim to untrusted end users; log them server-side.
 * Credentials are redacted (S3 keys are never included; endpoints with embedded userinfo
 * are rejected), but treat the text as internal.
 */
public class DataFusionException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DataFusionException(String message) {
        super(message);
    }

    public DataFusionException(String message, Throwable cause) {
        super(message, cause);
    }
}

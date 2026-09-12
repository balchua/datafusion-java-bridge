package io.github.balchua.datafusion.engine;

/**
 * Configuration for {@link DataFusionEngine}, passed to the native layer as JSON.
 *
 * <p>Defaults: no DataFusion memory limit (0), parallelism = min(4, cores),
 * scratch dir = system temp dir, snappy Parquet compression.
 *
 * <p>Credentials are never hard-coded: pass them via the builder or read them from the
 * environment in application code. See docs/parquet-and-s3.md.
 */
public final class EngineConfig {
    /** DataFusion memory pool limit in bytes (spillable operators only; NOT an RSS limit).
     *  0 = unlimited. See docs/memory.md. */
    private final long memoryLimitBytes;
    /** Tokio worker threads / DataFusion target partitions. 0 = default. */
    private final int parallelism;
    /** Scratch directory for spilling and Parquet staging; null = system temp dir. */
    private final String scratchDir;
    /** "snappy" (default) | "zstd" | "uncompressed". */
    private final String parquetCompression;
    private final S3 s3;
    private final boolean zeroCopy;

    private EngineConfig(Builder b) {
        this.memoryLimitBytes = b.memoryLimitBytes;
        this.parallelism = b.parallelism;
        this.scratchDir = b.scratchDir;
        this.parquetCompression = b.parquetCompression;
        this.s3 = b.s3;
        this.zeroCopy = b.zeroCopy;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static EngineConfig defaults() {
        return builder().build();
    }

    /** S3-compatible storage configuration. Bucket is taken from the s3:// URL. */
    public static final class S3 {
        final String endpoint;
        final String region;
        final String accessKey;
        final String secretKey;
        final boolean allowHttp;

        private S3(Builder b) {
            this.endpoint = b.endpoint;
            this.region = b.region;
            this.accessKey = b.accessKey;
            this.secretKey = b.secretKey;
            this.allowHttp = b.allowHttp;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private String endpoint;
            private String region;
            private String accessKey;
            private String secretKey;
            private boolean allowHttp;

            /** Custom endpoint (e.g. http://minio.internal:9000). Omit for AWS. */
            public Builder endpoint(String endpoint) { this.endpoint = endpoint; return this; }
            public Builder region(String region) { this.region = region; return this; }
            /** Access key; may be null to use the AWS default credential chain. */
            public Builder accessKey(String accessKey) { this.accessKey = accessKey; return this; }
            public Builder secretKey(String secretKey) { this.secretKey = secretKey; return this; }
            /** Required for plain-http on-premises endpoints. */
            public Builder allowHttp(boolean allowHttp) { this.allowHttp = allowHttp; return this; }

            public S3 build() { return new S3(this); }
        }
    }

    public static final class Builder {
        private long memoryLimitBytes = 0;
        private int parallelism = 0;
        private String scratchDir;
        private String parquetCompression = "snappy";
        private S3 s3;
        private boolean zeroCopy = true; // zero-copy is the default ingest mode

        public Builder memoryLimitBytes(long bytes) { this.memoryLimitBytes = bytes; return this; }
        public Builder parallelism(int parallelism) { this.parallelism = parallelism; return this; }
        public Builder scratchDir(String dir) { this.scratchDir = dir; return this; }
        public Builder parquetCompression(String compression) { this.parquetCompression = compression; return this; }
        /**
         * Ingest mode. <b>Default: true (zero-copy)</b> — Rust wraps the Java Arrow buffers
         * via the C Data Interface; they stay alive (and must stay unmodified) until the
         * table is dropped / the engine closed. Suits the build-once-then-query flow.
         *
         * <p>Set to {@code false} (deep-copy) when the Java side reuses/refills its
         * VectorSchemaRoot after registering, or wants to free those buffers before the
         * table is dropped: Rust then owns its own copy. See docs/arrow-lifetime.md.
         */
        public Builder zeroCopyIngest(boolean zeroCopy) { this.zeroCopy = zeroCopy; return this; }
        /** Configures S3-compatible storage. */
        public Builder s3(java.util.function.Consumer<S3.Builder> customizer) {
            S3.Builder b = S3.builder();
            customizer.accept(b);
            this.s3 = b.build();
            return this;
        }
        public EngineConfig build() { return new EngineConfig(this); }
    }

    /** JSON string literal with escaping. */
    private static String jsonString(String s) {
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    /** Whether zero-copy Arrow ingest is enabled (see {@link Builder#zeroCopyIngest}). */
    public boolean zeroCopy() {
        return zeroCopy;
    }

    /** Serializes to the JSON shape consumed by rust/src/engine.rs (serde). */
    String toNativeJson() {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"memory_limit_bytes\":").append(memoryLimitBytes);
        sb.append(",\"parallelism\":").append(parallelism);
        if (scratchDir != null) {
            sb.append(",\"scratch_dir\":").append(jsonString(scratchDir));
        }
        if (parquetCompression != null) {
            sb.append(",\"parquet_compression\":").append(jsonString(parquetCompression));
        }
        sb.append(",\"zero_copy\":").append(zeroCopy);
        if (s3 != null) {
            sb.append(",\"s3\":{");
            StringBuilder s3Json = new StringBuilder();
            if (s3.endpoint != null) {
                s3Json.append("\"endpoint\":").append(jsonString(s3.endpoint));
            }
            if (s3.region != null) {
                if (s3Json.length() > 0) s3Json.append(',');
                s3Json.append("\"region\":").append(jsonString(s3.region));
            }
            if (s3.accessKey != null) {
                if (s3Json.length() > 0) s3Json.append(',');
                s3Json.append("\"access_key\":").append(jsonString(s3.accessKey));
            }
            if (s3.secretKey != null) {
                if (s3Json.length() > 0) s3Json.append(',');
                s3Json.append("\"secret_key\":").append(jsonString(s3.secretKey));
            }
            if (s3Json.length() > 0) s3Json.append(',');
            s3Json.append("\"allow_http\":").append(s3.allowHttp);
            sb.append(s3Json).append('}');
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public String toString() {
        return "EngineConfig{memoryLimitBytes=" + memoryLimitBytes
                + ", parallelism=" + parallelism
                + ", scratchDir=" + scratchDir
                + ", parquetCompression=" + parquetCompression
                + ", zeroCopy=" + zeroCopy
                + ", s3=" + (s3 != null ? "configured" : "none") + '}';
    }
}
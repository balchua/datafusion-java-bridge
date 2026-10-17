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
    /** Parquet compression names accepted by the native layer. */
    private static final java.util.Set<String> VALID_COMPRESSIONS =
            java.util.Set.of("snappy", "zstd", "uncompressed", "none");

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
    /** Validate imported Arrow data (offsets, UTF-8, dictionary keys) before use. */
    private final boolean validateIngest;
    /** Expose DataFusion's {@code information_schema} (opt-in; reveals settings). */
    private final boolean informationSchema;
    /** Local roots a Parquet destination/registration must resolve under (empty = any). */
    private final java.util.List<String> allowedRoots;
    /** Overwrite an existing Parquet destination (default false). */
    private final boolean overwriteParquet;
    /** Max bytes DataFusion may spill to the scratch dir (null = DataFusion default). */
    private final Long spillDirMaxBytes;
    /** Planning timeout in millis (null = no timeout). */
    private final Long planTimeoutMillis;
    /** How long {@link DataFusionEngine#close()} waits for in-flight native calls before
     *  giving up and leaking the engine. Milliseconds; 0 = wait forever. */
    private final long closeTimeoutMillis;

    private EngineConfig(Builder b) {
        this.memoryLimitBytes = b.memoryLimitBytes;
        this.parallelism = b.parallelism;
        this.scratchDir = b.scratchDir;
        this.parquetCompression = b.parquetCompression;
        this.s3 = b.s3;
        this.zeroCopy = b.zeroCopy;
        this.validateIngest = b.validateIngest;
        this.informationSchema = b.informationSchema;
        this.allowedRoots = java.util.List.copyOf(b.allowedRoots);
        this.overwriteParquet = b.overwriteParquet;
        this.spillDirMaxBytes = b.spillDirMaxBytes;
        this.planTimeoutMillis = b.planTimeoutMillis;
        this.closeTimeoutMillis = b.closeTimeoutMillis;
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
        final java.util.List<String> allowedBuckets;

        private S3(Builder b) {
            this.endpoint = b.endpoint;
            this.region = b.region;
            this.accessKey = b.accessKey;
            this.secretKey = b.secretKey;
            this.allowHttp = b.allowHttp;
            this.allowedBuckets = java.util.List.copyOf(b.allowedBuckets);
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
            private final java.util.List<String> allowedBuckets = new java.util.ArrayList<>();

            /** Custom endpoint (e.g. http://minio.internal:9000). Omit for AWS. */
            public Builder endpoint(String endpoint) { this.endpoint = endpoint; return this; }
            public Builder region(String region) { this.region = region; return this; }
            /** Access key; may be null to use the AWS default credential chain. */
            public Builder accessKey(String accessKey) { this.accessKey = accessKey; return this; }
            public Builder secretKey(String secretKey) { this.secretKey = secretKey; return this; }
            /** Required for plain-http on-premises endpoints. */
            public Builder allowHttp(boolean allowHttp) { this.allowHttp = allowHttp; return this; }
            /** Restrict targets to these buckets (empty = any bucket the credentials reach). */
            public Builder allowedBuckets(String... buckets) { this.allowedBuckets.addAll(java.util.List.of(buckets)); return this; }

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
        private boolean validateIngest = false; // off by default: keeps registration flat
        private boolean informationSchema = false; // opt-in: reveals settings
        private final java.util.List<String> allowedRoots = new java.util.ArrayList<>();
        private boolean overwriteParquet = false; // refuse to clobber by default
        private Long spillDirMaxBytes;
        private Long planTimeoutMillis;
        private long closeTimeoutMillis = 60_000; // 60 s safety net for in-flight calls

        public Builder memoryLimitBytes(long bytes) { this.memoryLimitBytes = bytes; return this; }
        public Builder parallelism(int parallelism) { this.parallelism = parallelism; return this; }
        public Builder scratchDir(String dir) { this.scratchDir = dir; return this; }
        /**
         * Parquet compression: {@code "snappy"} (default) | {@code "zstd"} |
         * {@code "uncompressed"} | {@code "none"}. {@code null} means "use the native
         * default (snappy)" — it does <b>not</b> disable compression; use
         * {@code "uncompressed"} for that.
         */
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
        /**
         * Whether to validate imported Arrow data (offsets, UTF-8 validity, dictionary
         * keys) before DataFusion reads it. <b>Default: false</b> — the O(rows) scan is
         * skipped so registration stays flat. Enable it when the producer may supply
         * malformed data: the C Data Interface import is otherwise unchecked, so bad data
         * is undefined behaviour in native code. Deep-copy ingest validates regardless.
         */
        public Builder validateOnIngest(boolean validateIngest) { this.validateIngest = validateIngest; return this; }
        /**
         * Expose DataFusion's {@code information_schema} to SQL. Default {@code false};
         * enabling it lets queries read session/engine settings metadata.
         */
        public Builder informationSchema(boolean informationSchema) { this.informationSchema = informationSchema; return this; }
        /** Restrict local Parquet destinations/registrations to these roots (canonicalized). */
        public Builder allowedRoots(String... roots) { this.allowedRoots.addAll(java.util.List.of(roots)); return this; }
        /** Overwrite an existing Parquet destination. Default {@code false} (refuse to clobber). */
        public Builder overwriteParquet(boolean overwrite) { this.overwriteParquet = overwrite; return this; }
        /** Cap DataFusion spill to the scratch directory, in bytes. {@code 0} disables
         *  spilling entirely. */
        public Builder spillDirMaxBytes(long bytes) { this.spillDirMaxBytes = bytes; return this; }
        /** Planning timeout in milliseconds (0 = no timeout). */
        public Builder planTimeoutMillis(long millis) { this.planTimeoutMillis = millis; return this; }
        /**
         * How long {@link DataFusionEngine#close()} waits for in-flight native calls
         * (e.g. a slow {@code queryToParquet} upload) before giving up and leaking the
         * engine rather than risking a use-after-free. Default 60,000 ms.
         *
         * <p><b>0 waits forever</b> — no leak is possible, but a genuinely hung call makes
         * close block indefinitely. This is a safety-net knob, not a substitute for
         * cancellation; a longer timeout does not make a hung call finish.
         */
        public Builder closeTimeoutMillis(long millis) { this.closeTimeoutMillis = millis; return this; }
        /** Convenience overload of {@link #closeTimeoutMillis(long)}. */
        public Builder closeTimeout(java.time.Duration timeout) {
            return closeTimeoutMillis(timeout.toMillis());
        }
        /** Configures S3-compatible storage. */
        public Builder s3(java.util.function.Consumer<S3.Builder> customizer) {
            S3.Builder b = S3.builder();
            customizer.accept(b);
            this.s3 = b.build();
            return this;
        }
        /**
         * Validates the configuration and freezes it.
         *
         * @throws IllegalArgumentException if a numeric value is negative or the
         *         compression name is unknown, so misconfiguration fails here rather than
         *         deep inside the native layer.
         */
        public EngineConfig build() {
            if (memoryLimitBytes < 0) {
                throw new IllegalArgumentException(
                        "memoryLimitBytes must be >= 0, was " + memoryLimitBytes);
            }
            if (parallelism < 0) {
                throw new IllegalArgumentException(
                        "parallelism must be >= 0, was " + parallelism);
            }
            if (closeTimeoutMillis < 0) {
                throw new IllegalArgumentException(
                        "closeTimeoutMillis must be >= 0, was " + closeTimeoutMillis);
            }
            if (spillDirMaxBytes != null && spillDirMaxBytes < 0) {
                throw new IllegalArgumentException(
                        "spillDirMaxBytes must be >= 0, was " + spillDirMaxBytes);
            }
            if (planTimeoutMillis != null && planTimeoutMillis < 0) {
                throw new IllegalArgumentException(
                        "planTimeoutMillis must be >= 0, was " + planTimeoutMillis);
            }
            if (parquetCompression != null
                    && !VALID_COMPRESSIONS.contains(parquetCompression.toLowerCase(java.util.Locale.ROOT))) {
                throw new IllegalArgumentException(
                        "unsupported parquetCompression '" + parquetCompression
                                + "' (supported: " + VALID_COMPRESSIONS + ")");
            }
            return new EngineConfig(this);
        }
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
                    // Escape C0 controls; replace unpaired surrogates with U+FFFD (a lone
                    // surrogate is not valid UTF-16 and would become invalid UTF-8 at the
                    // JNI boundary; serde_json may reject a `\uD800` escape).
                    boolean loneSurrogate = Character.isHighSurrogate(c)
                            ? !(i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1)))
                            : Character.isLowSurrogate(c);
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else if (loneSurrogate) {
                        sb.append('\uFFFD');
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

    /**
     * Serializes to the JSON shape consumed by rust/src/engine.rs (serde).
     *
     * <p>The result contains S3 access/secret keys in clear text and <b>must never be
     * logged or otherwise persisted</b>.
     */
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
        sb.append(",\"validate_ingest\":").append(validateIngest);
        sb.append(",\"information_schema\":").append(informationSchema);
        sb.append(",\"overwrite_parquet\":").append(overwriteParquet);
        if (spillDirMaxBytes != null) {
            sb.append(",\"spill_dir_max_bytes\":").append(spillDirMaxBytes);
        }
        if (planTimeoutMillis != null && planTimeoutMillis > 0) {
            sb.append(",\"plan_timeout_millis\":").append(planTimeoutMillis);
        }
        sb.append(",\"allowed_roots\":[");
        for (int i = 0; i < allowedRoots.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(jsonString(allowedRoots.get(i)));
        }
        sb.append(']');
        sb.append(",\"close_timeout_millis\":").append(closeTimeoutMillis);
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
            s3Json.append(",\"allowed_buckets\":[");
            for (int i = 0; i < s3.allowedBuckets.size(); i++) {
                if (i > 0) s3Json.append(',');
                s3Json.append(jsonString(s3.allowedBuckets.get(i)));
            }
            s3Json.append(']');
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
                + ", closeTimeoutMillis=" + closeTimeoutMillis
                + ", s3=" + (s3 != null ? "configured" : "none") + '}';
    }
}
package io.github.balchua.datafusion.engine;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The native config JSON is the contract with Rust ({@code rust/src/engine.rs}); these tests
 * parse the output and assert the field names and values, so a misspelling or a broken
 * comma cannot silently drop a setting.
 */
class EngineConfigJsonTest {

    private static JsonNode parse(EngineConfig config) {
        return JsonMapper.builder().build().readTree(config.toNativeJson());
    }

    @Test
    void roundTripsEveryFieldWithNonDefaultValues() {
        EngineConfig config = EngineConfig.builder()
                .memoryLimitBytes(1024)
                .parallelism(2)
                .scratchDir("/tmp/x")
                .parquetCompression("zstd")
                .zeroCopyIngest(false)
                .validateOnIngest(true)
                .informationSchema(true)
                .allowedRoots("/data", "/srv")
                .overwriteParquet(true)
                .spillDirMaxBytes(4096)
                .planTimeoutMillis(3000)
                .closeTimeoutMillis(1234)
                .s3(s3 -> s3.endpoint("http://h:1").region("r").accessKey("a").secretKey("s")
                        .allowHttp(true).allowedBuckets("b1", "b2"))
                .build();

        JsonNode n = parse(config);
        assertEquals(1024, n.get("memory_limit_bytes").asLong());
        assertEquals(2, n.get("parallelism").asInt());
        assertEquals("/tmp/x", n.get("scratch_dir").asString());
        assertEquals("zstd", n.get("parquet_compression").asString());
        assertFalse(n.get("zero_copy").asBoolean());
        assertTrue(n.get("validate_ingest").asBoolean());
        assertTrue(n.get("information_schema").asBoolean());
        assertTrue(n.get("overwrite_parquet").asBoolean());
        assertEquals(4096, n.get("spill_dir_max_bytes").asLong());
        assertEquals(3000, n.get("plan_timeout_millis").asLong());
        assertEquals(1234, n.get("close_timeout_millis").asLong());
        assertEquals(2, n.get("allowed_roots").size());
        assertEquals("/data", n.get("allowed_roots").get(0).asString());
        assertEquals("/srv", n.get("allowed_roots").get(1).asString());

        JsonNode s3 = n.get("s3");
        assertEquals("http://h:1", s3.get("endpoint").asString());
        assertEquals("r", s3.get("region").asString());
        assertEquals("a", s3.get("access_key").asString());
        assertEquals("s", s3.get("secret_key").asString());
        assertTrue(s3.get("allow_http").asBoolean());
        assertEquals("b1", s3.get("allowed_buckets").get(0).asString());
        assertEquals("b2", s3.get("allowed_buckets").get(1).asString());
    }

    @Test
    void defaultsAreEmitted() {
        JsonNode n = parse(EngineConfig.defaults());
        assertTrue(n.get("zero_copy").asBoolean(), "zero-copy is the default");
        assertFalse(n.get("validate_ingest").asBoolean(), "validation is off by default");
        assertFalse(n.get("overwrite_parquet").asBoolean(), "no-clobber is the default");
        assertEquals(0, n.get("allowed_roots").size());
        // plan_timeout is 0 (no timeout) and must be omitted; spill dir is unset.
        assertTrue(n.get("plan_timeout_millis") == null || n.get("plan_timeout_millis").isNull(),
                "plan_timeout_millis=0 must be omitted: " + n);
        assertTrue(n.get("spill_dir_max_bytes") == null || n.get("spill_dir_max_bytes").isNull());
    }

    @Test
    void escapesQuotesBackslashesAndControlCharacters() {
        String scratch = "a\"b\\c\nd\u0001";
        JsonNode n = parse(EngineConfig.builder().scratchDir(scratch).build());
        assertEquals(scratch, n.get("scratch_dir").asString());
    }

    @Test
    void replacesLoneSurrogates() {
        // A lone surrogate would be invalid UTF-8 across the JNI boundary; it becomes U+FFFD.
        JsonNode n = parse(EngineConfig.builder().scratchDir("x\uD800y").build());
        assertEquals("x\uFFFDy", n.get("scratch_dir").asString());
    }
}

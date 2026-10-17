package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.EngineConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** {@link EngineConfig.Builder} validates its inputs and fails fast on misconfiguration. */
class EngineConfigTest {

    @Test
    void defaultsAreValid() {
        assertDoesNotThrow(EngineConfig::defaults);
    }

    @Test
    void rejectsNegativeMemoryLimit() {
        assertThrows(IllegalArgumentException.class,
                () -> EngineConfig.builder().memoryLimitBytes(-1).build());
    }

    @Test
    void rejectsNegativeParallelism() {
        assertThrows(IllegalArgumentException.class,
                () -> EngineConfig.builder().parallelism(-1).build());
    }

    @Test
    void rejectsNegativeCloseTimeout() {
        assertThrows(IllegalArgumentException.class,
                () -> EngineConfig.builder().closeTimeoutMillis(-1).build());
    }

    @Test
    void rejectsUnknownCompression() {
        assertThrows(IllegalArgumentException.class,
                () -> EngineConfig.builder().parquetCompression("brotli").build());
    }

    @Test
    void acceptsKnownCompressionsCaseInsensitively() {
        for (String c : new String[] {"snappy", "zstd", "uncompressed", "none", "SNAPPY", "Zstd"}) {
            assertDoesNotThrow(() -> EngineConfig.builder().parquetCompression(c).build(),
                    "should accept " + c);
        }
    }

    @Test
    void acceptsZeroValues() {
        assertDoesNotThrow(() -> EngineConfig.builder()
                .memoryLimitBytes(0)
                .parallelism(0)
                .closeTimeoutMillis(0)
                .spillDirMaxBytes(0)
                .planTimeoutMillis(0)
                .build());
    }

    @Test
    void rejectsNegativeSpillDirMaxBytes() {
        assertThrows(IllegalArgumentException.class,
                () -> EngineConfig.builder().spillDirMaxBytes(-1).build());
    }

    @Test
    void rejectsNegativePlanTimeout() {
        assertThrows(IllegalArgumentException.class,
                () -> EngineConfig.builder().planTimeoutMillis(-1).build());
    }
}

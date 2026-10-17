package io.github.balchua.datafusion.engine;

/**
 * Test-only seam: exposes the package-private native access to tests that live in other
 * packages (e.g. {@code io.github.balchua.datafusion}). This class is compiled from test
 * sources only and is not part of the shipped API — production code must go through
 * {@link DataFusionEngine}/{@link QueryResult}.
 */
public final class TestNativeAccess {
    private TestNativeAccess() {}

    /** The native engine handle backing {@code engine}. */
    public static long handle(DataFusionEngine engine) {
        return engine.nativeHandle();
    }

    public static long stageArrow(long handle, String table, long schemaAddr, long[] arrayAddrs) {
        return NativeEngine.stageArrow(handle, table, schemaAddr, arrayAddrs);
    }

    public static long resultNextBatch(long handle, long queryId, long[] arrayAddrs) {
        return NativeEngine.resultNextBatch(handle, queryId, arrayAddrs);
    }
}

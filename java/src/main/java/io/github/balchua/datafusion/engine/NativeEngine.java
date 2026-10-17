package io.github.balchua.datafusion.engine;

import java.io.File;

/**
 * Single place where the native library is loaded and JNI entry points are declared.
 *
 * <p>This is an <b>internal</b> class: the native methods are package-private so that only
 * this package's public API ({@link DataFusionEngine}) can reach them, and no other code on
 * the classpath can pass a forged handle/pointer to the JNI layer.
 *
 * <p>Library resolution order:
 * <ol>
 *   <li>System property {@code dfe.native.library} — <b>absolute</b> path to the shared
 *       object</li>
 *   <li>Environment variable {@code DFE_NATIVE_LIBRARY}</li>
 *   <li>{@code java.library.path} lookup by name {@code datafusion_java_bridge}</li>
 *   <li>Development only: with {@code -Ddfe.native.dev=true}, probe
 *       {@code rust/target/release} relative to the working directory. This is
 *       <b>off by default</b> so a writable working directory cannot supply the library.</li>
 * </ol>
 *
 * <p>This class makes exactly one {@code System.load} call per JVM. All native methods
 * take the engine handle (and query id where applicable) as first arguments; Rust throws
 * typed Java exceptions mapped from {@code EngineError} (see rust/src/error.rs).
 */
final class NativeEngine {
    private static final String LIB_NAME = "datafusion_java_bridge";

    private NativeEngine() {}

    static void load() {
        String explicit = System.getProperty("dfe.native.library");
        if (explicit == null || explicit.isBlank()) {
            explicit = System.getenv("DFE_NATIVE_LIBRARY");
        }
        if (explicit != null && !explicit.isBlank()) {
            File f = new File(explicit);
            if (!f.isAbsolute()) {
                throw new UnsatisfiedLinkError(
                        "dfe.native.library/DFE_NATIVE_LIBRARY must be an absolute path: " + explicit);
            }
            System.load(f.getAbsolutePath());
            return;
        }
        if (trySystemLoad()) {
            return;
        }
        // The CWD-relative dev probe can be hijacked if the working directory is writable,
        // so it only runs when explicitly enabled.
        if (Boolean.getBoolean("dfe.native.dev") && loadFromDevPath()) {
            return;
        }
        throw new UnsatisfiedLinkError(
                "native library 'datafusion_java_bridge' not found on java.library.path; set "
                        + "-Ddfe.native.library=/abs/path (or DFE_NATIVE_LIBRARY), or "
                        + "-Ddfe.native.dev=true to probe rust/target/release");
    }

    private static boolean trySystemLoad() {
        try {
            System.loadLibrary(LIB_NAME);
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }

    /** Probes the cargo output directories relative to the working directory (dev only). */
    private static boolean loadFromDevPath() {
        // Platform-correct file name (lib*.so / *.dylib / *.dll).
        String lib = System.mapLibraryName(LIB_NAME);
        for (String dir : new String[] {"rust/target/release", "../rust/target/release"}) {
            File f = new File(dir, lib);
            if (f.isFile()) {
                System.load(f.getAbsolutePath());
                return true;
            }
        }
        return false;
    }

    // -- lifecycle -----------------------------------------------------------

    /** Creates an engine; returns the native handle. Throws on config error. */
    static native long engineCreate(String configJson);

    /** Releases the engine and everything it owns. */
    static native void engineClose(long engineHandle);

    /** Version metadata string (rust/datafusion/arrow versions). */
    static native String engineVersion();

    // -- registration --------------------------------------------------------

    /**
     * Imports an Arrow record batch exported through the C Data Interface and stages it.
     * {@code schemaAddr} is the address of a Java-allocated ArrowSchema struct;
     * {@code arrayAddrs} the addresses of one ArrowArray struct per column.
     *
     * @return an internal staged-table id for {@link #commitTable}
     */
    static native long stageArrow(
            long engineHandle, String tableName, long schemaAddr, long[] arrayAddrs);

    /** Registers a staged batch as a table in DataFusion. */
    static native void commitTable(long engineHandle, String tableName, long stagedId);

    /** Registers a Parquet-backed table (filesystem or s3:// location). */
    static native void registerParquet(long engineHandle, String tableName, String location);

    // -- queries -------------------------------------------------------------

    /** Validates and plans SQL, starts execution; returns the query id. */
    static native long query(long engineHandle, String sql);

    /** Exports the result schema into a Java-allocated ArrowSchema struct. */
    static native void resultSchema(long engineHandle, long queryId, long schemaAddr);

    /**
     * Exports the next result batch into Java-allocated ArrowArray structs (one per
     * column). Returns the row count, or -1 at end of stream.
     */
    static native long resultNextBatch(long engineHandle, long queryId, long[] arrayAddrs);

    /** Cancels a running query. */
    static native void resultCancel(long engineHandle, long queryId);

    /** Producer-side statistics (JSON) for benchmarking. */
    static native String resultStats(long engineHandle, long queryId);

    /** Releases a finished/cancelled query and all of its state. */
    static native void resultClose(long engineHandle, long queryId);

    /** Executes SQL and writes the result directly to Parquet in Rust.
     *  @return JSON: {"encode_nanos":..,"publish_nanos":..} */
    static native String queryToParquet(long engineHandle, String sql, String destination);

    static {
        load();
    }
}

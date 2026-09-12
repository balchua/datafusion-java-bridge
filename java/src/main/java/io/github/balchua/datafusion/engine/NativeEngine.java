package io.github.balchua.datafusion.engine;

import java.io.File;

/**
 * Single place where the native library is loaded and JNI entry points are declared.
 *
 * <p>Library resolution order:
 * <ol>
 *   <li>System property {@code dfe.native.library} — absolute path to the shared object
 *   <li>Environment variable {@code DFE_NATIVE_LIBRARY}</li>
 *   <li>{@code java.library.path} lookup by name {@code datafusion_engine}</li>
 *   <li>Fallback: {@code target/release} in the working directory (development)</li>
 * </ol>
 *
 * <p>This class makes exactly one {@code System.load} call per JVM. All native methods
 * take the engine handle (and query id where applicable) as first arguments; Rust throws
 * typed Java exceptions mapped from {@code EngineError} (see rust/src/error.rs).
 */
public final class NativeEngine {
    private static final String LIB_NAME = "datafusion_java_bridge";
    private static volatile boolean loaded;

    private NativeEngine() {}

    public static synchronized void load() {
        if (loaded) {
            return;
        }
        String explicit = System.getProperty("dfe.native.library");
        if (explicit == null || explicit.isBlank()) {
            explicit = System.getenv("DFE_NATIVE_LIBRARY");
        }
        if (explicit != null && !explicit.isBlank()) {
            System.load(new File(explicit).getAbsolutePath());
        } else {
            // Try java.library.path first, then the cargo dev/release outputs.
            boolean ok = trySystemLoad();
            if (!ok) {
                String lib = "libdatafusion_java_bridge.so";
                for (String dir : new String[] {"rust/target/release", "../rust/target/release"}) {
                    File f = new File(dir, lib);
                    if (f.exists()) {
                        System.load(f.getAbsolutePath());
                        ok = true;
                        break;
                    }
                }
            }
            if (!ok) {
                throw new UnsatisfiedLinkError(
                        "native library 'datafusion_engine' not found; build it with "
                                + "'cargo build --release' in rust/ or set dfe.native.library");
            }
        }
        loaded = true;
    }

    private static boolean trySystemLoad() {
        try {
            System.loadLibrary(LIB_NAME);
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }

    // -- lifecycle -----------------------------------------------------------

    /** Creates an engine; returns the native handle. Throws on config error. */
    public static native long engineCreate(String configJson);

    /** Releases the engine and everything it owns. */
    public static native void engineClose(long engineHandle);

    /** Version metadata string (rust/datafusion/arrow versions). */
    public static native String engineVersion();

    // -- registration --------------------------------------------------------

    /**
     * Imports an Arrow record batch exported through the C Data Interface and stages it.
     * {@code schemaAddr} is the address of a Java-allocated ArrowSchema struct;
     * {@code arrayAddrs} the addresses of one ArrowArray struct per column.
     *
     * @return an internal staged-table id for {@link #commitTable}
     */
    public static native long stageArrow(
            long engineHandle, String tableName, long schemaAddr, long[] arrayAddrs);

    /** Registers a staged batch as a table in DataFusion. */
    public static native void commitTable(long engineHandle, String tableName, long stagedId);

    /** Registers a Parquet-backed table (filesystem or s3:// location). */
    public static native void registerParquet(long engineHandle, String tableName, String location);

    // -- queries -------------------------------------------------------------

    /** Validates and plans SQL, starts execution; returns the query id. */
    public static native long query(long engineHandle, String sql);

    /** Exports the result schema into a Java-allocated ArrowSchema struct. */
    public static native void resultSchema(long engineHandle, long queryId, long schemaAddr);

    /**
     * Exports the next result batch into Java-allocated ArrowArray structs (one per
     * column). Returns the row count, or -1 at end of stream.
     */
    public static native long resultNextBatch(long engineHandle, long queryId, long[] arrayAddrs);

    /** Cancels a running query. */
    public static native void resultCancel(long engineHandle, long queryId);

    /** Producer-side statistics (JSON) for benchmarking. */
    public static native String resultStats(long engineHandle, long queryId);

    /** Releases a finished/cancelled query and all of its state. */
    public static native void resultClose(long engineHandle, long queryId);

    /** Executes SQL and writes the result directly to Parquet in Rust.
     *  @return JSON: {"encode_nanos":..,"publish_nanos":..} */
    public static native String queryToParquet(long engineHandle, String sql, String destination);

    static {
        load();
    }
}
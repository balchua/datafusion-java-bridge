package io.github.balchua.datafusion.bench;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;

import java.nio.file.Path;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Two-phase benchmark (spec §22/§25/§30), restructured for clarity:
 *
 * <ol>
 * <li><b>LOAD</b> — generate the Arrow dataset in Java, bridge it into
 * DataFusion,
 * measure load time and memory (DataFusion accounting + RSS delta).</li>
 * <li><b>QUERY</b> — run each SQL shape against the already-loaded tables and
 * measure
 * plan / execute / result-transfer separately. Every row carries the full
 * SQL.</li>
 * </ol>
 *
 * Console output mirrors this with plain-language stage names and the SQL
 * itself.
 */
public final class BenchmarkRunner {

    /**
     * One dataset load: its shape, Arrow build time, load time and memory
     * footprint.
     */
    public record LoadResult(
            SyntheticPaymentGenerator.Shape shape,
            String profile,
            long messages,
            long remittances,
            long transactions,
            int transactionsTopLevelColumns,
            int transactionsLeafFields,
            long arrowBuildNanos,
            long loadIntoDatafusionNanos,
            long loadedArrowBytes,
            long arrowAllocatedBytes,
            boolean zeroCopy,
            long rssBeforeBytes,
            long rssAfterBytes,
            long peakRssBytes,
            long javaHeapBytes,
            String[] versions,
            String timestamp) {
    }

    /** One SQL execution against a loaded dataset. */
    public record QueryResultRow(
            String timestamp,
            String javaVersion,
            String rustVersion,
            String dataFusionVersion,
            String arrowVersion,
            long messages,
            long remittancesPerMessage,
            long transactionsPerRemittance,
            String profile,
            String queryName,
            String sql,
            String queryDescription,
            long resultRows,
            long planMs,
            long executeMs,
            long resultTransferMs,
            long peakRssBytes,
            long javaHeapBytes) {
    }

    private static final Pattern PRODUCE_NANOS = Pattern.compile("\"produceNanos\":(\\d+)");
    private static final Pattern EXPORT_NANOS = Pattern.compile("\"(\\w+)_nanos\":(\\d+)");

    private final BufferAllocator allocator;
    private final EngineConfig config;
    private final MemoryMonitor memoryMonitor;

    public BenchmarkRunner(BufferAllocator allocator, EngineConfig config,
            MemoryMonitor memory) {
        this.allocator = allocator;
        this.config = config;
        this.memoryMonitor = memory;
    }

    // ------------------------------------------------------------------
    // PHASE 1 — LOAD
    // ------------------------------------------------------------------

    /**
     * Generates the dataset and loads it into DataFusion. Returns load timings and
     * the
     * memory delta; the engine is NOT closed (phase 2 reuses the loaded tables).
     */
    public LoadedDataset load(SyntheticPaymentGenerator.Shape shape, String profile) {
        String loadTimestamp = Instant.now().toString();
        long rssBefore = memoryMonitor.currentRssBytes();

        // One number for building the complete Arrow dataset (all columns, JSON,
        // nested).
        long buildStart = System.nanoTime();
        SyntheticPaymentGenerator.Generated g = SyntheticPaymentGenerator.generate(shape, profile, allocator);
        long arrowBuildNanos = System.nanoTime() - buildStart;
        long arrowBytes = measureArrowBytes(g);
        // Actual Java off-heap Arrow allocation (includes vector growth slack), sampled
        // while all roots are still open.
        long arrowAllocatedBytes = allocator.getAllocatedMemory();

        // One number for moving it into DataFusion (JNI + Rust deep copy +
        // registration).
        long loadStart = System.nanoTime();
        DataFusionEngine engine = new DataFusionEngine(config);
        try (VectorSchemaRoot m = g.message();
                VectorSchemaRoot r = g.remittances();
                VectorSchemaRoot txn = g.transactions()) {
            engine.registerArrow("payment_message", m);
            engine.registerArrow("remittances", r);
            engine.registerArrow("transactions", txn);
        }
        long loadIntoDatafusionNanos = System.nanoTime() - loadStart;
        long rssAfter = memoryMonitor.currentRssBytes();

        return new LoadedDataset(engine, new LoadResult(
                shape, profile, shape.messages(), shape.remittances(), shape.transactions(),
                g.transactionsStats().topLevelColumns(), g.transactionsStats().leafFields(),
                arrowBuildNanos, loadIntoDatafusionNanos, arrowBytes, arrowAllocatedBytes,
                config.zeroCopy(),
                rssBefore, rssAfter, memoryMonitor.peakRssBytes(),
                memoryMonitor.peakJavaHeapBytes(), null, loadTimestamp));
    }

    /** A loaded dataset: the live engine plus the LOAD measurement. */
    public record LoadedDataset(DataFusionEngine engine, LoadResult result) {
        public void close() {
            engine.close();
        }
    }

    /**
     * Arrow buffer bytes of the transactions batch, recursing into struct/list
     * children.
     */
    private long measureArrowBytes(SyntheticPaymentGenerator.Generated g) {
        long bytes = 0;
        for (var vector : g.transactions().getFieldVectors()) {
            bytes += vectorBytes(vector);
        }
        return bytes;
    }

    /**
     * Sums the buffer bytes of a vector and its nested children. StructVector's own
     * getFieldBuffers() returns only its validity buffer, so children must be
     * visited
     * explicitly or the reported size is wildly understated.
     */
    private static long vectorBytes(FieldVector vector) {
        long bytes = 0;
        for (var buffer : vector.getFieldBuffers()) {
            bytes += buffer.writerIndex();
        }
        if (vector instanceof StructVector struct) {
            for (FieldVector child : struct.getChildrenFromFields()) {
                bytes += vectorBytes(child);
            }
        } else if (vector instanceof ListVector list) {
            bytes += vectorBytes(list.getDataVector());
        }
        return bytes;
    }

    // ------------------------------------------------------------------
    // PHASE 2 — QUERIES (against a loaded dataset)
    // ------------------------------------------------------------------

    /** Runs one query against a loaded dataset; returns a descriptive row. */
    public QueryResultRow query(LoadedDataset dataset, BenchmarkQuery query) {
        String[] versions = parseVersions(dataset.engine.versionInfo());
        long start = System.nanoTime();
        QueryResult[] handle = new QueryResult[1];
        handle[0] = dataset.engine.query(query.sql);
        long planNanos = System.nanoTime() - start;

        long consumeStart = System.nanoTime();
        long resultRows = 0;
        try (QueryResult result = handle[0]) {
            VectorSchemaRoot batch;
            while ((batch = result.next()) != null) {
                resultRows += batch.getRowCount();
                batch.close();
            }
            long executeNanos = extractProduceNanos(result.stats());
            long transferNanos = System.nanoTime() - consumeStart;
            return new QueryResultRow(
                    Instant.now().toString(),
                    System.getProperty("java.version"),
                    versions[0], versions[1], versions[2],
                    dataset.result.shape().messages(),
                    dataset.result.shape().remittancesPerMessage(),
                    dataset.result.shape().transactionsPerRemittance(),
                    dataset.result.profile(),
                    query.name,
                    query.sql,
                    query.description,
                    resultRows,
                    planNanos / 1_000_000L,
                    executeNanos / 1_000_000L,
                    transferNanos / 1_000_000L,
                    memoryMonitor.peakRssBytes(),
                    memoryMonitor.peakJavaHeapBytes());
        }
    }

    // ------------------------------------------------------------------
    // Parquet helpers (scenarios B/C/E use them; kept from the previous design)
    // ------------------------------------------------------------------

    /**
     * Encodes a query result to Parquet in Rust; returns (encode_ms, publish_ms).
     */
    public long[] exportToParquet(LoadedDataset dataset, BenchmarkQuery query, Path scratchDir) {
        String destination = scratchDir.resolve(
                "bench_" + query.name + ".parquet").toString();
        String stats = dataset.engine.queryToParquet(query.sql, destination);
        return parseEncodePublish(stats);
    }

    /** Registers a Parquet file as a table (scenario C/E input path). */
    public long registerParquetTable(LoadedDataset dataset, String table, String location) {
        long start = System.nanoTime();
        dataset.engine.registerParquet(table, location);
        return (System.nanoTime() - start) / 1_000_000L;
    }

    private long extractProduceNanos(String stats) {
        if (stats == null) {
            return 0L;
        }
        Matcher m = PRODUCE_NANOS.matcher(stats);
        return m.find() ? Long.parseLong(m.group(1)) : 0L;
    }

    private long[] parseEncodePublish(String json) {
        long encode = 0;
        long publish = 0;
        if (json != null) {
            Matcher m = EXPORT_NANOS.matcher(json);
            while (m.find()) {
                if (m.group(1).equals("encode_nanos")) {
                    encode = Long.parseLong(m.group(2));
                }
                if (m.group(1).equals("publish_nanos")) {
                    publish = Long.parseLong(m.group(2));
                }
            }
        }
        return new long[] { encode / 1_000_000L, publish / 1_000_000L };
    }

    private static String[] parseVersions(String info) {
        String rust = "unknown", df = "unknown", arrow = "unknown";
        for (String part : info.split(" ")) {
            if (part.startsWith("rust="))
                rust = part.substring("rust=".length());
            if (part.startsWith("datafusion="))
                df = part.substring("datafusion=".length());
            if (part.startsWith("arrow="))
                arrow = part.substring("arrow=".length());
        }
        return new String[] { rust, df, arrow };
    }
}
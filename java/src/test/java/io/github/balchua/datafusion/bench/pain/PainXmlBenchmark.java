package io.github.balchua.datafusion.bench.pain;

import io.github.balchua.datafusion.bench.BenchmarkRunner;
import io.github.balchua.datafusion.bench.MemoryMonitor;
import io.github.balchua.datafusion.bench.ResultsWriter;
import io.github.balchua.datafusion.bench.SyntheticPaymentGenerator;
import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Special benchmark run: pain.001 XML → Arrow (single root per table) → DataFusion.
 *
 * <p>Complements the generated-Arrow scenarios by measuring a real XML ingest. It uses the
 * flat pain.001 schema and StAX parser copied from the sibling {@code java-arrow-pain}
 * project, so the parse is the same code; the addition is our zero-copy registration and
 * (optionally) our query engine on top.
 *
 * <p>Stages reported: ({@code xml_parse}) StAX parse into one root per table — no IPC
 * write and no batching; ({@code load_into_datafusion}) zero-copy registration; then a
 * few SQLs valid on the flat schema.
 */
public final class PainXmlBenchmark {

    private PainXmlBenchmark() {}

    private record PainQuery(String name, String sql, String description) {}

    private static final List<PainQuery> QUERIES = List.of(
            new PainQuery("pain_count",
                    "SELECT COUNT(*) AS n FROM pain_transaction",
                    "COUNT(*) over all parsed pain.001 transactions"),
            new PainQuery("pain_sum",
                    "SELECT SUM(instd_amt) AS total FROM pain_transaction",
                    "SUM(instd_amt) over Decimal128(18,5)"),
            new PainQuery("pain_group_ccy",
                    "SELECT ccy, COUNT(*) AS n, SUM(instd_amt) AS total "
                            + "FROM pain_transaction GROUP BY ccy",
                    "GROUP BY currency"),
            new PainQuery("pain_filter_ctry",
                    "SELECT COUNT(*) AS n FROM pain_transaction WHERE cdtr_ctry = 'IT'",
                    "filter on creditor country"));

    private static final Pattern PRODUCE_NANOS = Pattern.compile("\"produceNanos\":(\\d+)");

    /** Same off-heap cap as java-arrow-pain's Type A benchmark (2 GB) for parity. */
    private static final long ALLOCATOR_LIMIT = 2L * 1024 * 1024 * 1024;

    /** Warmup parses of the small fixture before the measured Type A parse. */
    private static final int WARMUP_ITERATIONS = 5;

    /**
     * Runs the pain.001 comparison.
     *
     * @param xmlFile  XML to parse, or {@code null} to generate/reuse Type A (1M)
     * @param out      results base path (writes {@code .json} + {@code .csv})
     * @param config   engine configuration
     * @param monitor  process memory sampler
     */
    public static void run(Path xmlFile, Path out, EngineConfig config, MemoryMonitor monitor)
            throws Exception {
        Path xml = xmlFile != null
                ? xmlFile
                : PainXmlFiles.ensureGenerated(PainXmlFiles.TYPE_A, PainXmlFiles.DEFAULT_DIR);
        long xmlBytes = Files.size(xml);
        System.out.printf("== PAIN.001 (XML -> Arrow -> DataFusion): %s (%.1f MB), parser=jackson-3-streaming ==%n",
                xml, xmlBytes / 1048576.0);

        long rssBefore;
        BufferAllocator allocator = new RootAllocator(ALLOCATOR_LIMIT);
        DataFusionEngine engine = null;
        PainTables tables = null;
        try {
            warmUp(allocator);
            rssBefore = monitor.currentRssBytes();
            long parseStart = System.nanoTime();
            tables = new JacksonPainParser().parse(xml, allocator);
            long parseNanos = System.nanoTime() - parseStart;
            long rssAfterParse = monitor.currentRssBytes();

            long arrowBytes = logicalBytes(tables.message())
                    + logicalBytes(tables.remittance())
                    + logicalBytes(tables.transaction());
            long allocated = allocator.getAllocatedMemory();

            System.out.printf("  %-24s %10.1f ms   (Jackson 3 streaming -> one Arrow root per table; no IPC)%n",
                    "xml_parse", parseNanos / 1_000_000.0);
            System.out.printf("  %-24s %10d msg / %d remittance(s) / %,d transactions%n",
                    "rows", tables.messageRows(), tables.remittanceRows(), tables.transactionRows());
            System.out.printf("  %-24s %10.1f MB logical / %.1f MB allocated%n",
                    "arrow data size", arrowBytes / 1048576.0, allocated / 1048576.0);
            System.out.printf("  %-24s %10.0f MB -> %.0f MB  (peak %.0f MB, java heap %.0f MB)%n",
                    "process memory", rssBefore / 1048576.0, rssAfterParse / 1048576.0,
                    monitor.peakRssBytes() / 1048576.0, monitor.peakJavaHeapBytes() / 1048576.0);

            engine = new DataFusionEngine(config);
            long loadStart = System.nanoTime();
            engine.registerArrow("pain_message", tables.message());
            engine.registerArrow("pain_remittance", tables.remittance());
            engine.registerArrow("pain_transaction", tables.transaction());
            long loadNanos = System.nanoTime() - loadStart;
            long rssAfterLoad = monitor.currentRssBytes();
            System.out.printf("  %-24s %10.1f ms   (zero-copy register of the parsed tables)%n",
                    "load_into_datafusion", loadNanos / 1_000_000.0);

            List<BenchmarkRunner.QueryResultRow> rows =
                    runQueries(engine, monitor, (int) tables.transactionRows());

            String[] versions = versions(engine.versionInfo());
            SyntheticPaymentGenerator.Shape shape =
                    new SyntheticPaymentGenerator.Shape(1, 1, (int) tables.transactionRows());
            BenchmarkRunner.LoadResult load = new BenchmarkRunner.LoadResult(
                    shape, "pain001",
                    tables.messageRows(), tables.remittanceRows(), tables.transactionRows(),
                    15, 15,
                    parseNanos, loadNanos, arrowBytes, allocated, true,
                    rssBefore, rssAfterLoad, monitor.peakRssBytes(), monitor.peakJavaHeapBytes(),
                    versions, Instant.now().toString());

            ResultsWriter.write(out, load, rows);
            System.out.println(">> wrote " + out.toAbsolutePath() + " (+ .csv)");
        } finally {
            if (engine != null) {
                engine.close();
            }
            if (tables != null) {
                tables.close();
            }
            allocator.close();
        }
    }

    /**
     * Parses the small warmup fixture a few times (results closed each time) so the JIT
     * compiler and page cache are hot before the measured Type A parse. Not measured.
     */
    private static void warmUp(BufferAllocator allocator) throws Exception {
        Path warm = PainXmlFiles.ensureGenerated(PainXmlFiles.WARMUP, PainXmlFiles.DEFAULT_DIR);
        JacksonPainParser parser = new JacksonPainParser();
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            try (PainTables t = parser.parse(warm, allocator)) {
                if (t.transactionRows() != PainXmlFiles.WARMUP.totalTransactions()) {
                    throw new IllegalStateException(
                            "warmup parse row mismatch: " + t.transactionRows());
                }
            }
        }
        System.out.printf("  %-24s %10d x small pain.001 (JIT warmup, not measured)%n",
                "warmup", WARMUP_ITERATIONS);
    }

    private static List<BenchmarkRunner.QueryResultRow> runQueries(
            DataFusionEngine engine, MemoryMonitor monitor, int txPerRemittance) {
        String[] v = versions(engine.versionInfo());
        List<BenchmarkRunner.QueryResultRow> rows = new ArrayList<>();
        for (PainQuery q : QUERIES) {
            System.out.printf("%n[%s] %s%n  SQL: %s%n", q.name(), q.description(), q.sql());
            long planStart = System.nanoTime();
            QueryResult result = engine.query(q.sql());
            long planNanos = System.nanoTime() - planStart;

            long consumeStart = System.nanoTime();
            long resultRows = 0;
            long executeNanos;
            long transferNanos;
            try (QueryResult r = result) {
                VectorSchemaRoot batch;
                while ((batch = r.next()) != null) {
                    resultRows += batch.getRowCount();
                    batch.close();
                }
                executeNanos = extractProduceNanos(r.stats());
                transferNanos = System.nanoTime() - consumeStart;
            }
            long planMs = planNanos / 1_000_000L;
            long executeMs = executeNanos / 1_000_000L;
            long transferMs = transferNanos / 1_000_000L;
            System.out.printf(
                    "  result_rows=%,d  plan=%dms  execute=%dms  result_transfer=%dms  peakRSS=%.0fMB%n",
                    resultRows, planMs, executeMs, transferMs, monitor.peakRssBytes() / 1048576.0);
            rows.add(new BenchmarkRunner.QueryResultRow(
                    Instant.now().toString(), v[0], v[1], v[2], v[3],
                    1, 1, txPerRemittance, "pain001", q.name(), q.sql(), q.description(),
                    resultRows, planMs, executeMs, transferMs,
                    monitor.peakRssBytes(), monitor.peakJavaHeapBytes()));
        }
        return rows;
    }

    /** Java/rust/datafusion/arrow versions, parsed from the engine's version string. */
    private static String[] versions(String info) {
        String rust = "unknown";
        String df = "unknown";
        String arrow = "unknown";
        for (String part : info.split(" ")) {
            if (part.startsWith("rust=")) {
                rust = part.substring("rust=".length());
            } else if (part.startsWith("datafusion=")) {
                df = part.substring("datafusion=".length());
            } else if (part.startsWith("arrow=")) {
                arrow = part.substring("arrow=".length());
            }
        }
        return new String[] {System.getProperty("java.version"), rust, df, arrow};
    }

    private static long extractProduceNanos(String stats) {
        if (stats == null) {
            return 0L;
        }
        Matcher m = PRODUCE_NANOS.matcher(stats);
        return m.find() ? Long.parseLong(m.group(1)) : 0L;
    }

    /** Logical bytes: sum of used bytes in each vector's buffers (flat schema). */
    private static long logicalBytes(VectorSchemaRoot root) {
        long bytes = 0;
        for (FieldVector vector : root.getFieldVectors()) {
            for (org.apache.arrow.memory.ArrowBuf buffer : vector.getFieldBuffers()) {
                bytes += buffer.writerIndex();
            }
        }
        return bytes;
    }
}

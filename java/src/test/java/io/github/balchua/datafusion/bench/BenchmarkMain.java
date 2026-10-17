package io.github.balchua.datafusion.bench;

import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.bench.pain.PainXmlBenchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Benchmark CLI, two-phase (spec §30):
 *
 * <pre>
 *   PHASE 1 (LOAD):  generate Arrow data in Java (messages x remittances x transactions),
 *                    register it in DataFusion, report load time + memory.
 *   PHASE 2 (QUERY): run each SQL against the loaded tables, report plan/execute/transfer
 *                    and print the SQL itself.
 * </pre>
 *
 * Examples (from the repo root):
 * 
 * <pre>
 * # 4 messages, 10 remittances each, 2500 transactions each (= 100k transactions)
 * benchmark/run-benchmark.sh --messages 4 --remittances-per-message 10 \\
 *     --transactions-per-remittance 2500 --out bench/results.json
 *
 * # 2M transactions in one RecordBatch, then all 7 queries
 * DFE_JVM_OPTS="-Xmx12g" benchmark/run-benchmark.sh \\
 *     --messages 1 --remittances-per-message 80000 --transactions-per-remittance 25 \\
 *     --out bench/results-2m.json
 * </pre>
 */
public final class BenchmarkMain {

    public static void main(String[] args) throws Exception {
        Parsed p = Parsed.parse(args);
        if (p.help) {
            usage();
            return;
        }
        Files.createDirectories(p.scratchDir);
        Path out = p.out;
        Files.createDirectories(out.getParent() == null ? Path.of(".") : out.getParent());

        try (MemoryMonitor monitor = new MemoryMonitor(100);
                org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator(
                        Long.MAX_VALUE)) {
            if (p.pain) {
                PainXmlBenchmark.run(p.painXml != null ? Path.of(p.painXml) : null,
                        out, p.engineConfig(), monitor);
                return;
            }
            BenchmarkRunner runner = new BenchmarkRunner(
                    allocator, p.engineConfig(), monitor);

            // ---------- PHASE 1: LOAD ----------
            var shape = p.shape();
            System.out.printf(
                    "== LOAD: %d message(s) x %d remittance(s)/message x %d transaction(s)/remittance = %,d transactions ==%n",
                    shape.messages(), shape.remittancesPerMessage(),
                    shape.transactionsPerRemittance(), shape.transactions());
            BenchmarkRunner.LoadedDataset dataset = runner.load(shape, p.profile);
            printLoad(dataset.result());

            // Optional: export the loaded tables so external tools (e.g. DuckDB) can
            // inspect
            // exactly the data that was handed to DataFusion.
            if (p.dumpParquet != null) {
                dumpTables(dataset, p.dumpParquet);
            }

            // ---------- PHASE 2: QUERIES ----------
            if (NarrowPain001.PROFILE.equals(p.profile) && !p.queries.isEmpty()) {
                System.err.println("note: --profile narrow uses a flat pain.001-like schema; "
                        + "the canonical benchmark queries do not apply, running LOAD only");
                p.queries.clear();
            }
            List<BenchmarkRunner.QueryResultRow> rows = new ArrayList<>();
            if (!p.queries.isEmpty()) {
                System.out.println("== QUERIES ==");
                for (BenchmarkQuery query : p.queries) {
                    System.out.printf("%n[%s] %s%n  SQL: %s", query.name, query.description, query.sql);
                    BenchmarkRunner.QueryResultRow row = runner.query(dataset, query);
                    printQuery(row);
                    rows.add(row);
                }
            }
            ResultsWriter.write(out, dataset.result(), rows);
            System.out.println(">> wrote " + out.toAbsolutePath() + " (+ .csv)");
            dataset.close();
        }
    }

    /**
     * Exports each loaded table to Parquet via Rust (queryToParquet). `dest` is a
     * local
     * directory, or an s3:// prefix. Files are named {@code 
     * <table>
     * .parquet}.
     */
    private static void dumpTables(BenchmarkRunner.LoadedDataset dataset, String dest) throws Exception {
        boolean url = dest.contains("://");
        if (!url) {
            Files.createDirectories(Path.of(dest));
        }
        String base = dest.endsWith("/") ? dest.substring(0, dest.length() - 1) : dest;
        System.out.println("== DUMP (Parquet) ==");
        for (String table : new String[] { "payment_message", "remittances", "transactions" }) {
            String target = base + "/" + table + ".parquet";
            long start = System.nanoTime();
            dataset.engine().queryToParquet("SELECT * FROM " + table, target);
            System.out.printf("  %-16s -> %s  (%.0f ms)%n",
                    table, target, (System.nanoTime() - start) / 1_000_000.0);
        }
        System.out.println("  inspect with: duckdb -c \"SELECT * FROM read_parquet('" + base
                + "/transactions.parquet') LIMIT 5;\"");
    }

    private static void printLoad(BenchmarkRunner.LoadResult load) {
        System.out.printf("  %-24s %10.1f ms   (build all Arrow columns: JSON + nested Structs)%n",
                "arrow_build", load.arrowBuildNanos() / 1_000_000.0);
        System.out.printf("  %-24s %10.1f ms%n",
                "load_into_datafusion", load.loadIntoDatafusionNanos() / 1_000_000.0);
        System.out.printf("  %-24s %10d cols / %d leaf fields   (ingest: %s)%n",
                "transactions schema", load.transactionsTopLevelColumns(),
                load.transactionsLeafFields(), load.zeroCopy() ? "zero-copy" : "deep-copy");
        System.out.printf("  %-24s %10.1f MB logical / %.1f MB allocated%n",
                "arrow data size", load.loadedArrowBytes() / 1048576.0,
                load.arrowAllocatedBytes() / 1048576.0);
        System.out.printf("  %-24s %10.0f MB -> %.0f MB  (peak %.0f MB, java heap %.0f MB)%n",
                "process memory", load.rssBeforeBytes() / 1048576.0,
                load.rssAfterBytes() / 1048576.0,
                load.peakRssBytes() / 1048576.0, load.javaHeapBytes() / 1048576.0);
    }

    private static void printQuery(BenchmarkRunner.QueryResultRow row) {
        System.out.printf("  result_rows=%,d  plan=%dms  execute=%dms  result_transfer=%dms  peakRSS=%.0fMB%n",
                row.resultRows(), row.planMs(), row.executeMs(), row.resultTransferMs(),
                row.peakRssBytes() / 1048576.0);
    }

    /** Parsed CLI flags with defaults. */
    private static final class Parsed {
        int messages = 1;
        int remittancesPerMessage = -1; // default derived from rows
        int transactionsPerRemittance = 25;
        String profile = "normal";
        List<BenchmarkQuery> queries = new ArrayList<>();
        Path out = Path.of("bench", "results.json");
        Path scratchDir = Path.of(System.getProperty("java.io.tmpdir"), "dfe-scratch");
        long memoryLimitBytes = 0;
        boolean zeroCopy = true; // zero-copy is the default
        boolean validateIngest = false; // off by default (matches the library default)
        String dumpParquet; // optional: export loaded tables to Parquet for inspection
        String s3Bucket;
        String s3Endpoint;
        String s3Region;
        String s3AccessKey;
        String s3SecretKey;
        boolean s3AllowHttp;
        boolean help;
        boolean pain; // pain.001 XML -> Arrow comparison mode
        String painXml; // optional XML path; null = generate/reuse Type A (1M)

        SyntheticPaymentGenerator.Shape shape() {
            if (remittancesPerMessage <= 0) {
                throw new IllegalArgumentException(
                        "--remittances-per-message is required (together with --messages)");
            }
            return new SyntheticPaymentGenerator.Shape(
                    messages, remittancesPerMessage, transactionsPerRemittance);
        }

        EngineConfig engineConfig() {
            EngineConfig.Builder b = EngineConfig.builder()
                    .memoryLimitBytes(memoryLimitBytes)
                    .scratchDir(scratchDir.toString())
                    .zeroCopyIngest(zeroCopy)
                    .validateOnIngest(validateIngest);
            if (s3Bucket != null) {
                b.s3(s3 -> {
                    if (s3Endpoint != null) {
                        s3.endpoint(s3Endpoint);
                    }
                    if (s3Region != null) {
                        s3.region(s3Region);
                    }
                    if (s3AccessKey != null) {
                        s3.accessKey(s3AccessKey);
                    }
                    if (s3SecretKey != null) {
                        s3.secretKey(s3SecretKey);
                    }
                    s3.allowHttp(s3AllowHttp);
                });
            }
            return b.build();
        }

        static Parsed parse(String[] args) {
            Parsed p = new Parsed();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--messages" -> p.messages = Integer.parseInt(args[++i]);
                    case "--remittances-per-message" -> p.remittancesPerMessage = Integer.parseInt(args[++i]);
                    case "--transactions-per-remittance" -> p.transactionsPerRemittance = Integer.parseInt(args[++i]);
                    case "--profile" -> p.profile = args[++i];
                    case "--zero-copy" -> p.zeroCopy = true;
                    case "--deep-copy" -> p.zeroCopy = false;
                    case "--validate-ingest" -> p.validateIngest = true;
                    case "--no-validate-ingest" -> p.validateIngest = false;
                    case "--dump-parquet" -> p.dumpParquet = args[++i];
                    case "--queries" -> {
                        String value = args[++i].trim();
                        if (value.equalsIgnoreCase("all")) {
                            p.queries.addAll(java.util.List.of(BenchmarkQuery.values()));
                        } else {
                            for (String q : value.split(",")) {
                                String upper = q.trim().toUpperCase();
                                if (!upper.startsWith("Q")) {
                                    upper = "Q" + upper;
                                }
                                BenchmarkQuery match = null;
                                for (BenchmarkQuery candidate : BenchmarkQuery.values()) {
                                    if (candidate.name().equals(upper)) {
                                        match = candidate;
                                        break;
                                    }
                                    if (candidate.name().startsWith(upper + "_")) {
                                        match = candidate;
                                    }
                                }
                                if (match == null) {
                                    throw new IllegalArgumentException("unknown query: " + q);
                                }
                                p.queries.add(match);
                            }
                        }
                    }
                    case "--out" -> p.out = Path.of(args[++i]);
                    case "--scratch" -> p.scratchDir = Path.of(args[++i]);
                    case "--memory-limit-gb" -> p.memoryLimitBytes = Long.parseLong(args[++i]) * 1024 * 1024 * 1024;
                    case "--s3-bucket" -> p.s3Bucket = args[++i];
                    case "--s3-endpoint" -> p.s3Endpoint = args[++i];
                    case "--s3-region" -> p.s3Region = args[++i];
                    case "--s3-access-key" -> p.s3AccessKey = args[++i];
                    case "--s3-secret-key" -> p.s3SecretKey = args[++i];
                    case "--s3-allow-http" -> p.s3AllowHttp = true;
                    case "--pain" -> p.pain = true;
                    case "--pain-xml" -> {
                        p.pain = true;
                        p.painXml = args[++i];
                    }
                    case "--help" -> p.help = true;
                    default -> throw new IllegalArgumentException("unknown arg: " + args[i]);
                }
            }
            return p;
        }
    }

    private static void usage() {
        System.out.println(
                """
                        BenchmarkMain — two phases: LOAD (Arrow -> DataFusion, time + memory), then QUERY (SQL shapes)

                          Dataset shape (hierarchical, pick one):
                            --messages N                        number of payment messages
                            --remittances-per-message N         remittances per message (required)
                            --transactions-per-remittance N     transactions per remittance (default 25)
                            --profile normal|wide               schema profile (default normal)
                            --zero-copy                         zero-copy Arrow ingest (default; no Rust deep copy)
                            --deep-copy                         deep-copy ingest (Rust owns a copy; Java may
                                                                reuse/free buffers immediately)
                            --validate-ingest                   validate Arrow on ingest (default off; on adds an
                                                                O(rows) scan that makes load non-flat)
                            --no-validate-ingest                explicit opt-out (the default)

                          Queries:
                            --queries all|q1,q3,q5             which SQLs to run (default: none = LOAD only)
                                                                q1 projection              SELECT transaction_id FROM transactions
                                                                q2 nested projection        ... debtor.name
                                                                q3 nested filter            WHERE debtor.name = 'BBB'
                                                                q4 deep nested filter       WHERE debtor.account.iban = '…'
                                                                q5 aggregation              GROUP BY currency, SUM(amount)
                                                                q6 join                     message x remittance x transaction
                                                                q7 validation report        per-remittance declared vs actual

                          pain.001 XML comparison (separate run; ignores --messages etc.):
                            --pain                              parse Type A pain.001 XML (1M) -> Arrow -> DataFusion
                            --pain-xml FILE                     use FILE instead of generating/reusing Type A

                          Output / runtime:
                            --out FILE.json                     writes FILE.json and FILE.csv (default bench/results.json)
                            --memory-limit-gb N                 DataFusion memory pool limit (default unlimited)
                            --scratch DIR                       scratch directory
                            --s3-bucket B, --s3-endpoint U, --s3-region R,
                            --s3-access-key K, --s3-secret-key K, --s3-allow-http
                            --help""");
    }
}
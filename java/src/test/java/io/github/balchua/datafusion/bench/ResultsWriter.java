package io.github.balchua.datafusion.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes machine-readable benchmark output: one <code>.json</code> and one
 * <code>.csv</code>
 * side by side (spec §30). Contains a LOAD section (dataset shape, load time,
 * memory) and
 * a QUERY section (one self-describing row per SQL, with the SQL text itself).
 */
public final class ResultsWriter {
    private ResultsWriter() {
    }

    public static void write(Path outputBase,
            BenchmarkRunner.LoadResult load,
            java.util.List<BenchmarkRunner.QueryResultRow> queries) throws IOException {
        writeJson(outputBase, load, queries);
        writeCsv(outputBase, load, queries);
    }

    private static void writeJson(Path outputBase,
            BenchmarkRunner.LoadResult load,
            java.util.List<BenchmarkRunner.QueryResultRow> queries) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        // versions from the first query row (or defaults if no queries ran)
        String javaV = queries.isEmpty() ? System.getProperty("java.version") : queries.get(0).javaVersion();
        String rustV = queries.isEmpty() ? "unknown" : queries.get(0).rustVersion();
        String dfV = queries.isEmpty() ? "unknown" : queries.get(0).dataFusionVersion();
        String arrowV = queries.isEmpty() ? "unknown" : queries.get(0).arrowVersion();
        sb.append("  \"versions\": {\"java\": \"").append(javaV)
                .append("\", \"rust\": \"").append(rustV)
                .append("\", \"datafusion\": \"").append(dfV)
                .append("\", \"arrow\": \"").append(arrowV).append("\"},\n");
        sb.append("  \"load\": ").append(loadJson(load)).append(",\n");
        sb.append("  \"queries\": [\n");
        for (int i = 0; i < queries.size(); i++) {
            sb.append("    ").append(queryJson(queries.get(i)));
            sb.append(i < queries.size() - 1 ? "," : "").append('\n');
        }
        sb.append("  ]\n}\n");
        Files.writeString(withSuffix(outputBase, ".json"), sb.toString(), StandardCharsets.UTF_8);
    }

    private static String loadJson(BenchmarkRunner.LoadResult load) {
        StringBuilder sb = new StringBuilder("{\n");
        sb.append("    \"shape\": {\"messages\": ").append(load.messages())
                .append(", \"remittances_per_message\": ").append(load.shape().remittancesPerMessage())
                .append(", \"transactions_per_remittance\": ").append(load.shape().transactionsPerRemittance())
                .append("},\n");
        sb.append("    \"schema_profile\": \"").append(load.profile()).append("\",\n");
        sb.append("    \"rows\": {\"payment_message\": ").append(load.messages())
                .append(", \"remittances\": ").append(load.remittances())
                .append(", \"transactions\": ").append(load.transactions()).append("},\n");
        sb.append("    \"transactions_top_level_columns\": ").append(load.transactionsTopLevelColumns()).append(",\n");
        sb.append("    \"transactions_leaf_fields\": ").append(load.transactionsLeafFields()).append(",\n");
        sb.append("    \"arrow_build_ms\": ").append(String.format("%.1f", load.arrowBuildNanos() / 1_000_000.0))
                .append(",\n");
        sb.append("    \"load_into_datafusion_ms\": ")
                .append(String.format("%.1f", load.loadIntoDatafusionNanos() / 1_000_000.0)).append(",\n");
        sb.append("    \"ingest_mode\": \"").append(load.zeroCopy() ? "zero-copy" : "deep-copy").append("\",\n");
        sb.append("    \"arrow_data_bytes\": ").append(load.loadedArrowBytes()).append(",\n");
        sb.append("    \"arrow_data_mb\": ").append(String.format("%.1f", load.loadedArrowBytes() / 1048576.0))
                .append(",\n");
        sb.append("    \"arrow_allocated_mb\": ").append(String.format("%.1f", load.arrowAllocatedBytes() / 1048576.0))
                .append(",\n");
        sb.append("    \"rss_before_mb\": ").append(String.format("%.0f", load.rssBeforeBytes() / 1048576.0))
                .append(",\n");
        sb.append("    \"rss_after_mb\": ").append(String.format("%.0f", load.rssAfterBytes() / 1048576.0))
                .append(",\n");
        sb.append("    \"peak_rss_mb\": ").append(String.format("%.0f", load.peakRssBytes() / 1048576.0)).append(",\n");
        sb.append("    \"java_heap_mb\": ").append(String.format("%.0f", load.javaHeapBytes() / 1048576.0))
                .append("\n");
        sb.append("  }");
        return sb.toString();
    }

    private static String queryJson(BenchmarkRunner.QueryResultRow row) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"timestamp\": \"").append(row.timestamp()).append("\", ");
        sb.append("\"shape\": {\"messages\": ").append(row.messages())
                .append(", \"remittances_per_message\": ").append(row.remittancesPerMessage())
                .append(", \"transactions_per_remittance\": ").append(row.transactionsPerRemittance()).append("}, ");
        sb.append("\"schema_profile\": \"").append(row.profile()).append("\", ");
        sb.append("\"query_name\": \"").append(row.queryName()).append("\", ");
        sb.append("\"sql\": \"").append(row.sql().replace("\"", "\\\"")).append("\", ");
        sb.append("\"description\": \"").append(row.queryDescription()).append("\", ");
        sb.append("\"result_rows\": ").append(row.resultRows()).append(", ");
        sb.append("\"plan_ms\": ").append(row.planMs()).append(", ");
        sb.append("\"execute_ms\": ").append(row.executeMs()).append(", ");
        sb.append("\"result_transfer_ms\": ").append(row.resultTransferMs()).append(", ");
        sb.append("\"peak_rss_mb\": ").append(String.format("%.0f", row.peakRssBytes() / 1048576.0)).append(", ");
        sb.append("\"java_heap_mb\": ").append(String.format("%.0f", row.javaHeapBytes() / 1048576.0));
        return sb.append("}").toString();
    }

    /**
     * A single rectangular table: the LOAD columns come first, then the query-only
     * columns.
     * LOAD rows leave the query columns empty; QUERY rows leave the load-only value
     * columns
     * empty. This keeps one header valid for every row (and preserves the original
     * LOAD
     * column positions).
     */
    private static final String CSV_HEADER = "phase,timestamp,java_version,rust_version,datafusion_version,arrow_version,"
            + "messages,remittances_per_message,transactions_per_remittance,schema_profile,"
            + "transactions,stage_name,stage_ms,value_name,value,peak_rss_mb,java_heap_mb,"
            + "query_sql,result_rows,plan_ms,execute_ms,result_transfer_ms";

    private static void writeCsv(Path outputBase,
            BenchmarkRunner.LoadResult load,
            java.util.List<BenchmarkRunner.QueryResultRow> queries) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(CSV_HEADER).append('\n');
        // -- LOAD rows (one per dataset) --
        loadRow(sb, queries, load, "arrow_build",
                load.arrowBuildNanos() / 1_000_000.0, null, null);
        loadRow(sb, queries, load, "load_into_datafusion",
                load.loadIntoDatafusionNanos() / 1_000_000.0, null, null);
        loadRow(sb, queries, load, "schema", 0.0, "transactions_top_level_columns",
                (double) load.transactionsTopLevelColumns());
        loadRow(sb, queries, load, "schema", 0.0, "transactions_leaf_fields",
                (double) load.transactionsLeafFields());
        loadRow(sb, queries, load, "memory", 0.0, "arrow_data_mb",
                load.loadedArrowBytes() / 1048576.0);
        loadRow(sb, queries, load, "memory", 0.0, "arrow_allocated_mb",
                load.arrowAllocatedBytes() / 1048576.0);
        loadRow(sb, queries, load, "memory", 0.0, "rss_after_mb",
                load.rssAfterBytes() / 1048576.0);
        // -- QUERY rows (one per SQL) --
        for (BenchmarkRunner.QueryResultRow row : queries) {
            String[] fields = {
                    "QUERY", row.timestamp(),
                    row.javaVersion(), row.rustVersion(), row.dataFusionVersion(), row.arrowVersion(),
                    String.valueOf(row.messages()),
                    String.valueOf(row.remittancesPerMessage()),
                    String.valueOf(row.transactionsPerRemittance()),
                    row.profile(),
                    "", // transactions: dataset total, not per query
                    row.queryName(), // stage_name
                    "", // stage_ms
                    "", // value_name
                    "", // value
                    String.format("%.0f", row.peakRssBytes() / 1048576.0),
                    String.format("%.0f", row.javaHeapBytes() / 1048576.0),
                    quote(row.sql()),
                    String.valueOf(row.resultRows()),
                    String.valueOf(row.planMs()),
                    String.valueOf(row.executeMs()),
                    String.valueOf(row.resultTransferMs())
            };
            sb.append(String.join(",", fields)).append('\n');
        }
        Files.writeString(withSuffix(outputBase, ".csv"), sb.toString(), StandardCharsets.UTF_8);
    }

    private static void loadRow(StringBuilder sb,
            java.util.List<BenchmarkRunner.QueryResultRow> queries,
            BenchmarkRunner.LoadResult load,
            String stageName, double stageMs,
            String valueName, Double value) {
        String[] versions = versionFields(queries);
        String[] fields = {
                "LOAD", load.timestamp(),
                versions[0], versions[1], versions[2], versions[3],
                String.valueOf(load.messages()),
                String.valueOf(load.shape().remittancesPerMessage()),
                String.valueOf(load.shape().transactionsPerRemittance()),
                load.profile(),
                String.valueOf(load.transactions()),
                stageName,
                String.format("%.3f", stageMs),
                valueName == null ? "" : valueName,
                value == null ? "" : String.format("%.1f", value),
                String.format("%.0f", load.peakRssBytes() / 1048576.0),
                String.format("%.0f", load.javaHeapBytes() / 1048576.0),
                "", "", "", "", "", // query-only columns, empty for LOAD
        };
        sb.append(String.join(",", fields)).append('\n');
    }

    private static String[] versionFields(java.util.List<BenchmarkRunner.QueryResultRow> queries) {
        if (queries.isEmpty()) {
            return new String[] {
                    System.getProperty("java.version"), "unknown", "unknown", "unknown" };
        }
        BenchmarkRunner.QueryResultRow first = queries.get(0);
        return new String[] {
                first.javaVersion(), first.rustVersion(),
                first.dataFusionVersion(), first.arrowVersion() };
    }

    private static String quote(String sql) {
        return '"' + sql.replace("\"", "\"\"") + '"';
    }

    private static Path withSuffix(Path base, String suffix) {
        String name = base.getFileName().toString().replaceAll("\\.(json|csv)$", "");
        return base.resolveSibling(name + suffix);
    }
}

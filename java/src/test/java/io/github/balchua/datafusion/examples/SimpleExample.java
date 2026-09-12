package io.github.balchua.datafusion.examples;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Minimal end-to-end example — no payment fields, just a tiny "people" table:
 *
 * <pre>
 *   id: Int32, name: Utf8, salary: Decimal128(10,2), active: Bool
 *
 *   1. build the table directly as Arrow vectors
 *   2. register it in DataFusion
 *   3. run SQL (projection / filter / aggregation)
 *   4. read the result batches as Arrow, value by value
 *   5. write the result to Parquet from Rust, read it back, query it again
 * </pre>
 *
 * Run it with:
 * <pre>
 * MAVEN_OPTS="--enable-native-access=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED" \
 *   mvn -pl java exec:java -Dexec.classpathScope=test \
 *       -Dexec.mainClass=io.github.balchua.datafusion.examples.SimpleExample
 * </pre>
 */
public final class SimpleExample {

    public static void main(String[] args) throws Exception {
        // The allocator owns all Java-side Arrow memory. With the default zero-copy ingest
        // it must outlive the engine (engine closes first).
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {

            // 1. build a simple table directly as Arrow vectors
            VectorSchemaRoot people = buildPeople(allocator);
            System.out.println("built 'people': " + people.getRowCount() + " rows, "
                    + people.getSchema().getFields().size() + " columns");

            // 2. register it — Java Arrow -> Arrow C Data Interface -> Rust/DataFusion.
            engine.registerArrow("people", people);
            people.close(); // safe: with zero-copy the exporter keeps the buffers alive

            // 3. + 4. run SQL and read the Arrow result values
            query(engine, "SELECT id, name, salary FROM people WHERE active = true ORDER BY id");
            query(engine, "SELECT COUNT(*) AS n, SUM(salary) AS total FROM people");
            query(engine, "SELECT name FROM people WHERE salary > 1500.00 ORDER BY name");

            // 5. write the query result to Parquet in Rust, then query the Parquet table
            Path parquet = Files.createTempFile("people", ".parquet");
            engine.queryToParquet("SELECT * FROM people ORDER BY id", parquet.toString());
            engine.registerParquet("people_parquet", parquet.toString());
            query(engine, "SELECT COUNT(*) AS n FROM people_parquet");
            Files.deleteIfExists(parquet);
        }
    }

    /** Builds a tiny VectorSchemaRoot without any payment specifics. */
    private static VectorSchemaRoot buildPeople(BufferAllocator allocator) {
        Schema schema = new Schema(List.of(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("salary", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null),
                new Field("active", FieldType.nullable(new ArrowType.Bool()), null)
        ));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        IntVector id = (IntVector) root.getVector("id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        DecimalVector salary = (DecimalVector) root.getVector("salary");
        BitVector active = (BitVector) root.getVector("active");

        String[] names = {"alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"};
        for (int i = 0; i < names.length; i++) {
            id.setSafe(i, i);
            name.setSafe(i, names[i].getBytes());
            salary.setSafe(i, BigDecimal.valueOf(100000 + i * 12500, 2).setScale(2, RoundingMode.UNNECESSARY));
            active.setSafe(i, i % 2 == 0 ? 1 : 0);
        }
        root.setRowCount(names.length);
        return root;
    }

    /** Runs SQL and prints every row of every returned batch. */
    private static void query(DataFusionEngine engine, String sql) {
        System.out.println("\nSQL: " + sql);
        try (QueryResult result = engine.query(sql)) {
            VectorSchemaRoot batch;
            while ((batch = result.next()) != null) {     // streaming: zero or more batches
                try (VectorSchemaRoot current = batch) {  // always close a returned root
                    for (int r = 0; r < current.getRowCount(); r++) {
                        StringBuilder line = new StringBuilder("  ");
                        for (Field field : current.getSchema().getFields()) {
                            FieldVector vector = current.getVector(field.getName());
                            Object value = vector.getObject(r);
                            line.append(field.getName()).append('=')
                                    .append(vector.isNull(r) ? "null" : value).append("  ");
                        }
                        System.out.println(line);
                    }
                }
            }
        }
    }
}
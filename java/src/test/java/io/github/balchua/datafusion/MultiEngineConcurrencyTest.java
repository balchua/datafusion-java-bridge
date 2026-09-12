package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Correctness under concurrency across <em>many independent engines</em>.
 *
 * <p>{@link ConcurrencyTest} shares one engine across virtual threads; this test does the
 * opposite and is the harder isolation case: every worker owns its own engine, allocator
 * and Arrow dataset, yet all workers share the process-global engine registry. Each
 * dataset is tagged so its rows are unique; if state ever bled between engines the counts
 * and sums asserted here would change.
 *
 * <p>A {@link CyclicBarrier} holds a whole round at the registration point and releases
 * them together, so create/query/teardown overlap rather than run sequentially.
 */
class MultiEngineConcurrencyTest {

    private static final int WORKERS = 6;
    private static final int ROUNDS = 3;
    private static final int STUDENTS = 60;
    private static final int ENROLLMENTS = 600;

    @Test
    void manyEnginesInParallelKeepResultsIsolatedAndReleaseAllMemory() throws Exception {
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int round = 0; round < ROUNDS; round++) {
                CyclicBarrier start = new CyclicBarrier(WORKERS);
                List<Future<?>> futures = new ArrayList<>();
                for (int w = 0; w < WORKERS; w++) {
                    String tag = "r" + round + "w" + w;
                    futures.add(pool.submit(() -> {
                        runOneEngine(tag, start);
                        return null;
                    }));
                }
                for (Future<?> f : futures) {
                    f.get(120, TimeUnit.SECONDS); // propagate any worker failure
                }
            }
        }
    }

    private static void runOneEngine(String tag, CyclicBarrier start) throws Exception {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults());
        try {
            try (SchoolData.Dataset data = SchoolData.build(allocator, tag, STUDENTS, ENROLLMENTS)) {
                engine.registerArrow(SchoolData.TABLE_STUDENTS, data.students());
                engine.registerArrow(SchoolData.TABLE_CLASSROOMS, data.classrooms());
                engine.registerArrow(SchoolData.TABLE_ENROLLMENTS, data.enrollments());

                // Release every engine of this round at once.
                start.await(30, TimeUnit.SECONDS);

                // Self-tag filters: contamination from another engine would inflate these.
                assertEquals(STUDENTS, scalarLong(engine,
                        "SELECT COUNT(*) AS n FROM students WHERE name LIKE '" + tag + "-%'"));
                assertEquals(ENROLLMENTS, scalarLong(engine,
                        "SELECT COUNT(*) AS n FROM enrollments WHERE enrollment_id LIKE '" + tag + "-%'"));

                // A join that only this engine's data can satisfy, with an exact decimal sum.
                String join = "SELECT COUNT(*) AS n, SUM(e.score) AS total FROM enrollments e "
                        + "JOIN students s ON s.student_id = e.student_id "
                        + "JOIN classrooms c ON c.classroom_id = e.classroom_id "
                        + "WHERE s.student_id = '" + tag + "-S0'";
                assertEquals(SchoolData.studentEnrollmentCount(STUDENTS, ENROLLMENTS, 0),
                        scalarLong(engine, join));
                BigDecimal sum = scalarDecimal(engine, join);
                assertEquals(0, SchoolData.studentScoreSum(STUDENTS, ENROLLMENTS, 0).compareTo(sum),
                        "score sum mismatch for " + tag);

                // MATH = classroom index 0; exercises the third table independently.
                assertEquals(SchoolData.mathEnrollmentCount(ENROLLMENTS), scalarLong(engine,
                        "SELECT COUNT(*) AS n FROM enrollments e "
                                + "JOIN classrooms c ON c.classroom_id = e.classroom_id "
                                + "WHERE c.subject = 'MATH'"));
            }
        } finally {
            // Zero-copy: the engine must release the Rust references before the allocator
            // is closed, after which every Arrow byte must be back.
            engine.close();
            assertEquals(0L, allocator.getAllocatedMemory(),
                    "Arrow memory leaked by engine " + tag);
            allocator.close();
        }
    }

    private static long scalarLong(DataFusionEngine engine, String sql) {
        try (QueryResult result = engine.query(sql)) {
            VectorSchemaRoot batch = result.next();
            try (batch) {
                return ((Number) batch.getVector(0).getObject(0)).longValue();
            }
        }
    }

    private static BigDecimal scalarDecimal(DataFusionEngine engine, String sql) {
        try (QueryResult result = engine.query(sql)) {
            VectorSchemaRoot batch = result.next();
            try (batch) {
                return (BigDecimal) batch.getVector(1).getObject(0);
            }
        }
    }
}

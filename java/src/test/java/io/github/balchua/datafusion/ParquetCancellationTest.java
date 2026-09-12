package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.exception.EngineClosedException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cooperative cancellation of {@code queryToParquet} / {@code registerParquet}.
 *
 * <p>Each test points the engine at a loopback TCP listener that accepts the connection
 * but never answers, so the object-store call blocks deterministically. The engine is
 * configured with {@code closeTimeoutMillis(0)} (wait forever) and the test has a JUnit
 * timeout, so <b>the only way close() can return is cooperative cancellation</b> — the
 * timeout/leak safety net can never make these pass.
 */
class ParquetCancellationTest {

    @TempDir
    Path tempDir;

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void engineCloseCancelsStalledRegistration() throws Exception {
        try (BlackHole server = new BlackHole()) {
            DataFusionEngine engine = engineAgainst(server);
            try {
                AtomicReference<RuntimeException> failure = new AtomicReference<>();
                Thread worker = run(engine, () ->
                        engine.registerParquet("remote", "s3://bucket/x.parquet"), failure);

                assertTrue(server.awaitConnection(10, TimeUnit.SECONDS),
                        "S3 client never connected to the stalled endpoint");
                long closeMs = closeAndTime(engine);
                worker.join(10_000);

                assertFalse(worker.isAlive(), "registerParquet did not return after cancellation");
                assertTrue(closeMs < 5_000, "close took " + closeMs + " ms");
                assertInstanceOf(EngineClosedException.class, failure.get());
            } finally {
                engine.close();
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void engineCloseCancelsStalledUploadAndRemovesScratch() throws Exception {
        try (BlackHole server = new BlackHole()) {
            BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            DataFusionEngine engine = engineAgainst(server);
            try (SchoolData.Dataset data = SchoolData.build(allocator, "up", 10, 20)) {
                engine.registerArrow(SchoolData.TABLE_STUDENTS, data.students());

                AtomicReference<RuntimeException> failure = new AtomicReference<>();
                Thread worker = run(engine, () -> engine.queryToParquet(
                        "SELECT student_id FROM students", "s3://bucket/out.parquet"), failure);

                // Encoding is local and fast; the stall is the multipart upload.
                assertTrue(server.awaitConnection(10, TimeUnit.SECONDS),
                        "upload never connected to the stalled endpoint");
                long closeMs = closeAndTime(engine);
                worker.join(10_000);

                assertFalse(worker.isAlive(), "queryToParquet did not return after cancellation");
                assertTrue(closeMs < 5_000, "close took " + closeMs + " ms");
                assertInstanceOf(EngineClosedException.class, failure.get());
            } finally {
                engine.close();
                allocator.close();
            }
            assertTrue(scratchFiles().findAny().isEmpty(),
                    "cancelled upload left a scratch file behind");
        }
    }

    private DataFusionEngine engineAgainst(BlackHole server) {
        EngineConfig config = EngineConfig.builder()
                .closeTimeoutMillis(0) // wait forever: only cancellation can unblock close
                .scratchDir(tempDir.toString())
                .s3(s3 -> s3.endpoint("http://127.0.0.1:" + server.port())
                        .region("us-east-1")
                        .accessKey("test")
                        .secretKey("test")
                        .allowHttp(true))
                .build();
        return new DataFusionEngine(config);
    }

    private static Thread run(DataFusionEngine engine, Runnable call,
                              AtomicReference<RuntimeException> failure) {
        Thread worker = new Thread(() -> {
            try {
                call.run();
            } catch (RuntimeException t) {
                failure.set(t);
            }
        }, "parquet-under-test");
        worker.start();
        return worker;
    }

    private static long closeAndTime(DataFusionEngine engine) {
        long start = System.nanoTime();
        engine.close();
        return (System.nanoTime() - start) / 1_000_000;
    }

    private Stream<Path> scratchFiles() throws IOException {
        return Files.list(tempDir).filter(p -> p.getFileName().toString().startsWith("df_engine_"));
    }

    /** Accepts one TCP connection, then never sends an HTTP response. */
    private static final class BlackHole implements AutoCloseable {
        private final ServerSocket server;
        private final CountDownLatch connected = new CountDownLatch(1);
        private final Thread thread;

        BlackHole() throws IOException {
            this.server = new ServerSocket(0);
            this.thread = new Thread(this::serve, "black-hole-s3");
            this.thread.setDaemon(true);
            this.thread.start();
        }

        private void serve() {
            try (Socket socket = server.accept()) {
                connected.countDown();
                InputStream in = socket.getInputStream();
                byte[] sink = new byte[8192];
                while (in.read(sink) != -1) {
                    // Read and discard; deliberately never respond.
                }
            } catch (IOException e) {
                // Expected when the test closes the listener/socket; release any waiter.
                connected.countDown();
            }
        }

        int port() {
            return server.getLocalPort();
        }

        boolean awaitConnection(long timeout, TimeUnit unit) throws InterruptedException {
            return connected.await(timeout, unit);
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }
}

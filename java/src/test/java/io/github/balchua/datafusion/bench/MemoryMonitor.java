package io.github.balchua.datafusion.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Samples process memory while the benchmark runs. Peak RSS is reported from
 * the
 * kernel high-water mark ({@code /proc/self/status VmHWM}), which covers JVM,
 * Java
 * Arrow buffers, JNI, Rust/DataFusion allocations and object-store clients —
 * everything
 * in the process (see docs/memory.md). Java heap is sampled from
 * {@link Runtime}.
 */
public final class MemoryMonitor implements AutoCloseable {
    private final Thread sampler;
    private volatile long sampledPeakHeap;

    public MemoryMonitor(long intervalMs) {
        this.sampler = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                long heap = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                if (heap > sampledPeakHeap) {
                    sampledPeakHeap = heap;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(intervalMs));
            }
        }, "dfe-memory-monitor");
        sampler.setDaemon(true);
        sampler.start();
    }

    /** Peak resident set size of the process (kernel high-water mark), in bytes. */
    public long peakRssBytes() {
        return statusKb("VmHWM") * 1024L;
    }

    /** Current RSS of the process, in bytes. */
    public long currentRssBytes() {
        return statusKb("VmRSS") * 1024L;
    }

    /** Peak sampled Java heap usage, in bytes. */
    public long peakJavaHeapBytes() {
        long now = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        return Math.max(sampledPeakHeap, now);
    }

    /** Field value in kB from /proc/self/status, or 0 when unavailable. */
    private static long statusKb(String field) {
        try {
            String content = Files.readString(Path.of("/proc/self/status"), java.nio.charset.StandardCharsets.US_ASCII);
            for (String line : content.split("\n")) {
                if (line.startsWith(field + ":")) {
                    String num = line.substring(field.length() + 1).trim().split(" ")[0];
                    return Long.parseLong(num);
                }
            }
        } catch (IOException | NumberFormatException e) {
            return 0; // /proc not readable or the value is not numeric
        }
        return 0;
    }

    @Override
    public void close() {
        sampler.interrupt();
    }
}
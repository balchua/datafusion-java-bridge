package io.github.balchua.datafusion.bench;

import java.util.LinkedHashMap;
import java.util.function.Supplier;

/** Accumulates named stage timings (nanoseconds) in encounter order. */
public final class StageTimer {
    private final LinkedHashMap<String, Long> nanos = new LinkedHashMap<>();

    /** Times a body and accumulates it under one stage name. */
    public void stage(String name, Runnable body) {
        long start = System.nanoTime();
        body.run();
        add(name, System.nanoTime() - start);
    }

    /** Times a value-producing body. */
    public <T> T stageValue(String name, Supplier<T> body) {
        long start = System.nanoTime();
        T value = body.get();
        add(name, System.nanoTime() - start);
        return value;
    }

    /** Adds an externally measured duration (e.g. returned by the native layer). */
    public void add(String name, long durationNanos) {
        nanos.put(name, nanos.getOrDefault(name, 0L) + durationNanos);
    }

    public long get(String name) {
        return nanos.getOrDefault(name, 0L);
    }

    /** Copy of all stage durations (nanoseconds). */
    public LinkedHashMap<String, Long> stages() {
        return new LinkedHashMap<>(nanos);
    }
}
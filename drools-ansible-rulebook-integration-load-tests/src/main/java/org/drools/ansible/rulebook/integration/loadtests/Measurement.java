package org.drools.ansible.rulebook.integration.loadtests;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public final class Measurement {

    private Measurement() {}

    public static TimedResult timeWork(Supplier<List<Map>> work) {
        Instant start = Instant.now();
        List<Map> matches = work.get();
        long durationMs = Duration.between(start, Instant.now()).toMillis();
        return new TimedResult(matches, durationMs);
    }

    public static long captureUsedMemoryAfterGc() {
        System.gc();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        System.gc();
        Runtime r = Runtime.getRuntime();
        return r.totalMemory() - r.freeMemory();
    }

    public static final class TimedResult {
        public final List<Map> matches;
        public final long durationMs;

        public TimedResult(List<Map> matches, long durationMs) {
            this.matches = matches;
            this.durationMs = durationMs;
        }
    }
}

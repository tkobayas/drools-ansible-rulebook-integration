package org.drools.ansible.rulebook.integration.loadtests.analyze;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * See spec section 5.8. Groups test results four ways
 * (match|unmatch) x (noHA|HA-PG), sorts each group by event count, applies the
 * same absolute-increase / consecutive-acceleration / total-increase thresholds
 * used in the main module, and flags a leak if any group trips a threshold.
 */
public class MemoryLeakAnalyzer {

    private static final double INCREASE_GROWTH_THRESHOLD = 3.0;
    private static final long ABSOLUTE_INCREASE_THRESHOLD = 50_000_000;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java MemoryLeakAnalyzer <result_file>");
            System.exit(1);
        }
        try {
            AnalyzeResult r = new MemoryLeakAnalyzer().analyzeFile(args[0]);
            if (r.hasLeak || r.exceptionFound) {
                System.err.println("\n❌ MEMORY LEAK DETECTED OR EXCEPTION FOUND!");
                System.err.println("  Review the result file for details.\n");
                System.exit(1);
            }
            System.out.println("\n✅ No memory leak detected.");
            System.exit(0);
        } catch (Exception e) {
            System.err.println("Error analyzing results: " + e.getMessage());
            e.printStackTrace();
            System.exit(2);
        }
    }

    public AnalyzeResult analyzeFile(String filename) throws IOException {
        ParseResult pr = parseResultFile(filename);
        boolean hasLeak = analyzeResults(pr.results);
        return new AnalyzeResult(hasLeak, pr.exceptionFound);
    }

    private ParseResult parseResultFile(String filename) throws IOException {
        List<TestResult> results = new ArrayList<>();
        boolean exceptionFound = false;

        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                if (line.contains("Exception") || line.contains("exception")) {
                    exceptionFound = true;
                }
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    try {
                        String testName = parts[0].trim();
                        long mem = Long.parseLong(parts[1].trim());
                        long dur = Long.parseLong(parts[2].trim());
                        results.add(new TestResult(testName, mem, dur));
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }

        if (exceptionFound) {
            System.err.println("\n⚠️  EXCEPTION FOUND IN RESULTS (potentially caused by a memory leak)\n");
        }

        if (results.isEmpty()) {
            throw new IOException("No valid test results found in file. Please check the result file format.");
        }
        return new ParseResult(results, exceptionFound);
    }

    private boolean analyzeResults(List<TestResult> results) {
        System.out.println("Memory Leak Analysis Report");
        System.out.println("===========================\n");

        Map<String, List<TestResult>> groups = new LinkedHashMap<>();
        groups.put("match/noHA", new ArrayList<>());
        groups.put("match/HA-PG", new ArrayList<>());
        groups.put("unmatch/noHA", new ArrayList<>());
        groups.put("unmatch/HA-PG", new ArrayList<>());

        for (TestResult r : results) {
            boolean unmatch = r.testName.contains("unmatch");
            boolean haPg = r.testName.contains(" (HA-PG)");
            String key = (unmatch ? "unmatch" : "match") + "/" + (haPg ? "HA-PG" : "noHA");
            groups.get(key).add(r);
        }

        boolean hasLeak = false;
        for (Map.Entry<String, List<TestResult>> entry : groups.entrySet()) {
            List<TestResult> tests = entry.getValue();
            if (tests.isEmpty()) continue;
            System.out.println(entry.getKey() + ":");
            hasLeak |= analyzeTestGroup(tests);
            System.out.println();
        }
        return hasLeak;
    }

    private boolean analyzeTestGroup(List<TestResult> tests) {
        if (tests.isEmpty()) return false;

        tests.sort((a, b) -> Integer.compare(extractEventCount(a.testName), extractEventCount(b.testName)));

        System.out.println("Test Name                                 Memory (bytes)    Duration (ms)");
        System.out.println("------------------------------------------------------------------------");
        for (TestResult t : tests) {
            System.out.printf("%-41s %,13d    %,12d%n", t.testName, t.memoryUsage, t.duration);
        }

        System.out.println("\nMemory Increase Analysis:");
        boolean hasLeak = false;
        Long previousIncrease = null;
        int consecutive = 0;

        for (int i = 1; i < tests.size(); i++) {
            TestResult prev = tests.get(i - 1);
            TestResult curr = tests.get(i);
            long increase = curr.memoryUsage - prev.memoryUsage;

            System.out.printf("  %s → %s:%n",
                    formatEventCount(extractEventCount(prev.testName)),
                    formatEventCount(extractEventCount(curr.testName)));
            System.out.printf("    Memory increase: %,d bytes", increase);

            if (curr.memoryUsage == 0) {
                System.out.printf(" ⚠️  TEST FAILED (likely due to memory threshold)!%n");
                hasLeak = true;
            } else if (Math.abs(increase) > ABSOLUTE_INCREASE_THRESHOLD) {
                System.out.printf(" ⚠️  LARGE INCREASE!%n");
                hasLeak = true;
            } else if (previousIncrease != null && increase > 0 && previousIncrease > 0) {
                double ratio = (double) increase / previousIncrease;
                System.out.printf(" (%.2fx previous increase)", ratio);
                if (ratio > INCREASE_GROWTH_THRESHOLD) {
                    consecutive++;
                    if (consecutive >= 2) {
                        System.out.printf(" ⚠️  CONSECUTIVE ACCELERATING GROWTH!%n");
                        hasLeak = true;
                    } else {
                        System.out.printf(" ⚠ (noted, but not a leak if it's not consecutive)%n");
                    }
                } else {
                    consecutive = 0;
                    System.out.printf(" ✓%n");
                }
            } else {
                consecutive = 0;
                System.out.printf(" ✓%n");
            }
            previousIncrease = increase;
        }

        if (tests.size() >= 2) {
            long total = tests.get(tests.size() - 1).memoryUsage - tests.get(0).memoryUsage;
            System.out.printf("\nTotal memory increase (first → last): %,d bytes", total);
            if (total > ABSOLUTE_INCREASE_THRESHOLD * 3) {
                System.out.printf(" ⚠️  EXCESSIVE TOTAL INCREASE!%n");
                hasLeak = true;
            } else {
                System.out.printf(" ✓%n");
            }
        }
        return hasLeak;
    }

    private int extractEventCount(String name) {
        if (name.contains("1m_")) return 1_000_000;
        if (name.contains("100k_")) return 100_000;
        if (name.contains("10k_")) return 10_000;
        if (name.contains("1k_")) return 1_000;
        return 0;
    }

    private String formatEventCount(int c) {
        if (c >= 1_000_000) return (c / 1_000_000) + "M";
        if (c >= 1_000) return (c / 1_000) + "k";
        return String.valueOf(c);
    }

    public static final class AnalyzeResult {
        public final boolean hasLeak;
        public final boolean exceptionFound;

        public AnalyzeResult(boolean hasLeak, boolean exceptionFound) {
            this.hasLeak = hasLeak;
            this.exceptionFound = exceptionFound;
        }
    }

    private static final class TestResult {
        final String testName;
        final long memoryUsage;
        final long duration;

        TestResult(String testName, long memoryUsage, long duration) {
            this.testName = testName;
            this.memoryUsage = memoryUsage;
            this.duration = duration;
        }
    }

    private static final class ParseResult {
        final List<TestResult> results;
        final boolean exceptionFound;

        ParseResult(List<TestResult> results, boolean exceptionFound) {
            this.results = results;
            this.exceptionFound = exceptionFound;
        }
    }
}

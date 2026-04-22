package org.drools.ansible.rulebook.integration.loadtests.analyze;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class MemoryLeakAnalyzerTest {

    @Test
    void cleanAcrossAllFourGroups_reportsNoLeak(@TempDir Path tmp) throws Exception {
        Path f = tmp.resolve("result.txt");
        Files.writeString(f, String.join("\n",
                // match / noHA
                "24kb_1k_events.json, 5200000, 100",
                "24kb_10k_events.json, 5210000, 300",
                "24kb_100k_events.json, 5225000, 1800",
                "24kb_1m_events.json, 5280000, 17000",
                // match / HA-PG
                "24kb_1k_events.json (HA-PG), 6100000, 180",
                "24kb_10k_events.json (HA-PG), 6130000, 600",
                "24kb_100k_events.json (HA-PG), 6175000, 3500",
                "24kb_1m_events.json (HA-PG), 6240000, 32000",
                // unmatch / noHA
                "24kb_1k_events_unmatch.json, 5100000, 80",
                "24kb_10k_events_unmatch.json, 5115000, 250",
                "24kb_100k_events_unmatch.json, 5135000, 1500",
                "24kb_1m_events_unmatch.json, 5175000, 14000",
                // unmatch / HA-PG
                "24kb_1k_events_unmatch.json (HA-PG), 6000000, 140",
                "24kb_10k_events_unmatch.json (HA-PG), 6020000, 500",
                "24kb_100k_events_unmatch.json (HA-PG), 6050000, 2800",
                "24kb_1m_events_unmatch.json (HA-PG), 6100000, 26000"
        ));

        MemoryLeakAnalyzer.AnalyzeResult r = new MemoryLeakAnalyzer().analyzeFile(f.toString());

        assertThat(r.exceptionFound).isFalse();
        assertThat(r.hasLeak).isFalse();
    }

    @Test
    void hugeAbsoluteSpike_reportsLeak(@TempDir Path tmp) throws Exception {
        Path f = tmp.resolve("result.txt");
        Files.writeString(f, String.join("\n",
                "24kb_1k_events.json, 5000000, 100",
                "24kb_10k_events.json, 5010000, 300",
                "24kb_100k_events.json, 5020000, 1800",
                "24kb_1m_events.json, 300000000, 17000" // 295MB jump
        ));

        MemoryLeakAnalyzer.AnalyzeResult r = new MemoryLeakAnalyzer().analyzeFile(f.toString());

        assertThat(r.hasLeak).isTrue();
    }

    @Test
    void exceptionSubstringInResultFile_isFlagged(@TempDir Path tmp) throws Exception {
        Path f = tmp.resolve("result.txt");
        Files.writeString(f, String.join("\n",
                "24kb_1k_events.json, 5200000, 100",
                "RuntimeException at line 42",
                "24kb_10k_events.json, 5210000, 300"
        ));

        MemoryLeakAnalyzer.AnalyzeResult r = new MemoryLeakAnalyzer().analyzeFile(f.toString());

        assertThat(r.exceptionFound).isTrue();
    }
}

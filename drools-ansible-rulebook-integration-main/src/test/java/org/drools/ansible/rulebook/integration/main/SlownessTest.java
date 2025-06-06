package org.drools.ansible.rulebook.integration.main;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.rulesengine.SlowAutomaticPseudoClock;
import org.drools.ansible.rulebook.integration.main.Main.ExecuteResult;
import org.drools.ansible.rulebook.integration.main.utils.StringPrintStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.rulesengine.SessionStatsCollector.DELAY_WARNING_THRESHOLD_PROPERTY;

public class SlownessTest {

    static PrintStream originalOut = System.out;

    static StringPrintStream stringPrintStream = new StringPrintStream(System.out);

    @BeforeAll
    static public void beforeClass() throws Exception {
        System.setOut(stringPrintStream);
    }

    @AfterAll
    static public void afterClass() {
        System.setOut(originalOut);
    }

    @Test
    void testOnceAfter() {
        try {
            // <drools.delay.warning.threshold>2</drools.delay.warning.threshold> configured in pom.xml.
            // But explicitly set it here in case of running the test from IDE.
            System.setProperty(DELAY_WARNING_THRESHOLD_PROPERTY, "2");

            SlowAutomaticPseudoClock.enable(2000, 4000); // introduces 4 seconds slowness
            ExecuteResult result = Main.execute("slowness-test.json");

            // a warning is logged. e.g.
            // WARN org.drools.ansible.rulebook.integration.api.rulesengine.SessionStatsCollector - r1 is fired with a delay of 3016 ms
            assertThat(stringPrintStream.getStringList()).anyMatch(s -> s.contains("r1 is fired with a delay of"));

            List<Map> returnedMatches = result.getReturnedMatches();
            assertThat(returnedMatches).hasSize(1);
            assertThat(returnedMatches.get(0)).containsOnlyKeys("r1");
        } finally {
            SlowAutomaticPseudoClock.resetAndDisable();
            System.clearProperty(DELAY_WARNING_THRESHOLD_PROPERTY);
        }
    }
}

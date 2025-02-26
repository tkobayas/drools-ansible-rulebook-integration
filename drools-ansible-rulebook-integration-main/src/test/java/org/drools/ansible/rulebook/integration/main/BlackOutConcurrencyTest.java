package org.drools.ansible.rulebook.integration.main;

import org.drools.ansible.rulebook.integration.api.rulesengine.MemoryThresholdReachedException;
import org.drools.ansible.rulebook.integration.main.Main.ExecuteResult;
import org.junit.Ignore;
import org.junit.Test;

import static org.drools.ansible.rulebook.integration.api.rulesengine.RuleEngineTestUtils.disableEventStructureSuggestion;
import static org.drools.ansible.rulebook.integration.api.rulesengine.RuleEngineTestUtils.enableEventStructureSuggestion;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BlackOutConcurrencyTest {

    // test blackout with multi-thread client
    @Test
    public void testManyEvents() {
        checkDuration("100k_event_blackout_ast.json", 10_000);
    }

    private static void checkDuration(String jsonFile, int expectedMaxDuration) {
        ExecuteResult result = Main.execute(jsonFile);
        long duration = result.getDuration();
        System.out.println("Executed in " + duration + " msecs");
        assertTrue("There is a performance issue, this test took too long: " + duration + " msecs", duration < expectedMaxDuration);
    }
}

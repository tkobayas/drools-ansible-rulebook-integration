package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_HA_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_PG_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createMatchingEvent;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.dropTables;

/**
 * Action and MatchingEvent related tests for HAStateManager
 */
class HAStateManagerActionTest {

    private HAStateManager stateManager;

    private static final String HA_UUID = "test-ha-1";
    private static final String LEADER_ID = "test-leader-1";

    @BeforeEach
    void setUp() {
        stateManager = HAStateManagerFactory.create();
        stateManager.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG);
    }

    @AfterEach
    void tearDown() {
        if (stateManager != null) {
            stateManager.shutdown();
        }

        dropTables();
    }

    @Test
    void testActionManagement() {
        stateManager.enableLeader(LEADER_ID);

        // First create a matching event
        MatchingEvent me = createMatchingEvent(HA_UUID, "testRuleset", "testRule",
                                               Map.of("fact", "value"));
        String meUuid = stateManager.addMatchingEvent(me);

        // Add an action
        String actionData = "{\"name\":\"send_alert\",\"status\":\"running\",\"start_time\":\"2024-01-01T10:00:00Z\"}";
        stateManager.addActionInfo(meUuid, 0, actionData);

        // Verify action exists
        assertThat(stateManager.actionInfoExists(meUuid, 0)).isTrue();
        assertThat(stateManager.actionInfoExists(meUuid, 1)).isFalse();

        // Get action and verify
        String retrieved = stateManager.getActionInfo(meUuid, 0);
        assertThat(retrieved).isEqualTo(actionData);

        // Update action
        String updatedData = "{\"name\":\"send_alert\",\"status\":\"success\",\"end_time\":\"2024-01-01T10:01:00Z\"}";
        stateManager.updateActionInfo(meUuid, 0, updatedData);

        // Verify update
        retrieved = stateManager.getActionInfo(meUuid, 0);
        assertThat(retrieved).isEqualTo(updatedData);

        // Delete action
        stateManager.deleteActionInfo(meUuid);

        // Verify action is gone
        assertThat(stateManager.actionInfoExists(meUuid, 0)).isFalse();

        // Should not appear in pending events
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents();
        assertThat(pending).isEmpty();
    }

    @Test
    void testPendingMatchingEventsRecovery() {
        stateManager.enableLeader(LEADER_ID);

        // Create multiple matching events with different states
        MatchingEvent matchingEvent1 = createMatchingEvent(HA_UUID, "ruleset1", "rule1",
                                                           Map.of("event", "1"));
        String meUuid1 = stateManager.addMatchingEvent(matchingEvent1);

        MatchingEvent matchingEvent2 = createMatchingEvent(HA_UUID, "ruleset1", "rule2",
                                                           Map.of("event", "2"));
        String meUuid2 = stateManager.addMatchingEvent(matchingEvent2);

        // Add action for first ME (in progress)
        stateManager.addActionInfo(meUuid1, 0, "{\"status\":\"running\"}");

        // Get pending events (should include both)
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents();
        assertThat(pending).hasSize(2);
        assertThat(pending.stream().map(MatchingEvent::getMeUuid))
                .containsExactlyInAnyOrder(meUuid1, meUuid2);
    }
}
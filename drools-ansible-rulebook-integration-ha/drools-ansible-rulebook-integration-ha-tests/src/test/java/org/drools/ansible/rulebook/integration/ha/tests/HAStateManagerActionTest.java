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
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createMatchingEvent;

/**
 * Action and MatchingEvent related tests for HAStateManager
 */
class HAStateManagerActionTest {

    private HAStateManager stateManager;
    private static final String SESSION_ID = "test-session-1";
    private static final String LEADER_ID = "test-leader-1";

    @BeforeEach
    void setUp() {
        // Use new initializeHA API
        String uuid = "test-ha-" + System.nanoTime();
        Map<String, Object> postgresParams = new HashMap<>();
        // Use H2 for testing (falls back when postgres params are empty)

        Map<String, Object> config = new HashMap<>();
        config.put("write_after", 1);

        stateManager = HAStateManagerFactory.create();
        stateManager.initializeHA(uuid, postgresParams, config);
    }

    @AfterEach
    void tearDown() {
        if (stateManager != null) {
            stateManager.shutdown();
        }
    }

    @Test
    void testActionManagement() {
        stateManager.enableLeader(LEADER_ID);

        // First create a matching event
        MatchingEvent me = createMatchingEvent(SESSION_ID, "testRuleset", "testRule",
                                               Map.of("fact", "value"));
        String meUuid = stateManager.addMatchingEvent(me);

        // Add an action
        String actionData = "{\"name\":\"send_alert\",\"status\":\"running\",\"start_time\":\"2024-01-01T10:00:00Z\"}";
        stateManager.addActionInfo(SESSION_ID, meUuid, 0, actionData);

        // Verify action exists
        assertThat(stateManager.actionInfoExists(SESSION_ID, meUuid, 0)).isTrue();
        assertThat(stateManager.actionInfoExists(SESSION_ID, meUuid, 1)).isFalse();

        // Get action and verify
        String retrieved = stateManager.getActionInfo(SESSION_ID, meUuid, 0);
        assertThat(retrieved).isEqualTo(actionData);

        // Update action
        String updatedData = "{\"name\":\"send_alert\",\"status\":\"success\",\"end_time\":\"2024-01-01T10:01:00Z\"}";
        stateManager.updateActionInfo(SESSION_ID, meUuid, 0, updatedData);

        // Verify update
        retrieved = stateManager.getActionInfo(SESSION_ID, meUuid, 0);
        assertThat(retrieved).isEqualTo(updatedData);

        // Delete action
        stateManager.deleteActionInfo(SESSION_ID, meUuid);

        // Verify action is gone
        assertThat(stateManager.actionInfoExists(SESSION_ID, meUuid, 0)).isFalse();

        // Should not appear in pending events
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents(SESSION_ID);
        assertThat(pending).isEmpty();
    }

    @Test
    void testPendingMatchingEventsRecovery() {
        stateManager.enableLeader(LEADER_ID);

        // Create multiple matching events with different states
        MatchingEvent matchingEvent1 = createMatchingEvent(SESSION_ID, "ruleset1", "rule1",
                                                           Map.of("event", "1"));
        String meUuid1 = stateManager.addMatchingEvent(matchingEvent1);

        MatchingEvent matchingEvent2 = createMatchingEvent(SESSION_ID, "ruleset1", "rule2",
                                                           Map.of("event", "2"));
        String meUuid2 = stateManager.addMatchingEvent(matchingEvent2);

        // Add action for first ME (in progress)
        stateManager.addActionInfo(SESSION_ID, meUuid1, 0, "{\"status\":\"running\"}");

        // Get pending events (should include both)
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents("ruleset1");
        assertThat(pending).hasSize(2);
        assertThat(pending.stream().map(MatchingEvent::getMeUuid))
                .containsExactlyInAnyOrder(meUuid1, meUuid2);
    }
}
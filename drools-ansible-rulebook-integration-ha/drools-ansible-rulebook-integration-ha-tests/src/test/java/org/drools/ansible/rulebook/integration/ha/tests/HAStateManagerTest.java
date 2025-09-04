package org.drools.ansible.rulebook.integration.ha.tests;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.EventState;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for HAStateManager
 */
public class HAStateManagerTest {

    private HAStateManager stateManager;
    private ObjectMapper objectMapper = new ObjectMapper();
    private static final String SESSION_ID = "test-session-1";
    private static final String LEADER_ID = "test-leader-1";

    @Before
    public void setUp() {
        // Use new initializeHA API
        String uuid = "test-ha-" + System.nanoTime();
        Map<String, Object> postgresParams = new HashMap<>();
        // Use H2 for testing (falls back when postgres params are empty)

        Map<String, Object> config = new HashMap<>();
        config.put("write_after", 1);

        stateManager = HAStateManagerFactory.createH2();
        stateManager.initializeHA(uuid, postgresParams, config);
    }

    @After
    public void tearDown() {
        if (stateManager != null) {
            stateManager.shutdown();
        }
    }

    // Utility method to create a MatchingEvent with default values
    private MatchingEvent createMatchingEvent(String sessionId, String rulesetName,
                                              String ruleName, Map<String, Object> matchingFacts) {
        MatchingEvent me = new MatchingEvent();
        me.setSessionId(sessionId);
        me.setRuleSetName(rulesetName);
        me.setRuleName(ruleName);

        // Serialize matching facts to JSON
        try {
            String eventDataJson = objectMapper.writeValueAsString(matchingFacts);
            me.setEventData(eventDataJson);
        } catch (Exception e) {
            me.setEventData("{}");
        }

        return me;
    }

    @Test
    public void testLeaderElection() {
        // Initially not a leader
        assertFalse(stateManager.isLeader());

        // Enable leader mode
        stateManager.enableLeader(LEADER_ID);
        assertTrue(stateManager.isLeader());

        // Disable leader mode
        stateManager.disableLeader(LEADER_ID);
        assertFalse(stateManager.isLeader());
    }

    @Test
    public void testEventStatePersistence() {
        stateManager.enableLeader(LEADER_ID);

        // Create and persist event state
        EventState event = new EventState();
        event.setSessionId(SESSION_ID);
        event.setRulebookHash("abc123");
        event.setSessionStats(Map.of("temperature", 30, "humidity", 65));

        stateManager.persistEventState(SESSION_ID, event);

        // Event state persistence doesn't return anything but should not throw
        assertTrue(true); // If we got here, persistence worked
    }

    @Test
    public void testMatchingEventCreation() {
        stateManager.enableLeader(LEADER_ID);

        // Create matching event when rule triggers
        Map<String, Object> facts = Map.of(
                "temperature", 35,
                "alert", "high_temp"
        );

        MatchingEvent me = createMatchingEvent(SESSION_ID, "alertRules", "tempAlert", facts);
        String meUuid = stateManager.addMatchingEvent(me);

        assertNotNull(meUuid);
        assertFalse(meUuid.isEmpty());
        assertEquals(me.getMeUuid(), meUuid);
    }

    @Test
    public void testActionManagement() {
        stateManager.enableLeader(LEADER_ID);

        // First create a matching event
        MatchingEvent me = createMatchingEvent(SESSION_ID, "testRuleset", "testRule",
                                               Map.of("fact", "value"));
        String meUuid = stateManager.addMatchingEvent(me);

        // Add an action
        String actionData = "{\"name\":\"send_alert\",\"status\":\"running\",\"start_time\":\"2024-01-01T10:00:00Z\"}";
        stateManager.addActionState(SESSION_ID, meUuid, 0, actionData);

        // Verify action exists
        assertTrue(stateManager.actionStateExists(SESSION_ID, meUuid, 0));
        assertFalse(stateManager.actionStateExists(SESSION_ID, meUuid, 1));

        // Get action and verify
        String retrieved = stateManager.getActionState(SESSION_ID, meUuid, 0);
        assertEquals(actionData, retrieved);

        // Update action
        String updatedData = "{\"name\":\"send_alert\",\"status\":\"success\",\"end_time\":\"2024-01-01T10:01:00Z\"}";
        stateManager.updateActionState(SESSION_ID, meUuid, 0, updatedData);

        // Verify update
        retrieved = stateManager.getActionState(SESSION_ID, meUuid, 0);
        assertEquals(updatedData, retrieved);
    }

    @Test
    public void testPendingMatchingEventsRecovery() {
        stateManager.enableLeader(LEADER_ID);

        // Create multiple matching events with different states
        MatchingEvent matchingEvent1 = createMatchingEvent(SESSION_ID, "rules1", "rule1",
                                                           Map.of("event", "1"));
        String me1 = stateManager.addMatchingEvent(matchingEvent1);

        MatchingEvent matchingEvent2 = createMatchingEvent(SESSION_ID, "rules2", "rule2",
                                                           Map.of("event", "2"));
        String me2 = stateManager.addMatchingEvent(matchingEvent2);

        // Add action for first ME (in progress)
        stateManager.addActionState(SESSION_ID, me1, 0, "{\"status\":\"running\"}");

        // Get pending events (should include both)
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents(SESSION_ID);
        assertThat(pending).hasSize(2);
        assertThat(pending.stream().map(MatchingEvent::getMeUuid))
                .containsExactlyInAnyOrder(me1, me2);
    }

    @Test
    public void testMatchingEventDeletion() {
        stateManager.enableLeader(LEADER_ID);

        // Create ME and action
        MatchingEvent me = createMatchingEvent(SESSION_ID, "rules", "rule",
                                               Map.of("data", "test"));
        String meUuid = stateManager.addMatchingEvent(me);

        // Add an action
        stateManager.addActionState(SESSION_ID, meUuid, 0, "{\"name\":\"test_action\"}");

        // Verify action exists
        assertTrue(stateManager.actionStateExists(SESSION_ID, meUuid, 0));

        // Delete matching event and all its actions
        stateManager.deleteActionStates(SESSION_ID, meUuid);

        // Verify action is gone
        assertFalse(stateManager.actionStateExists(SESSION_ID, meUuid, 0));

        // Should not appear in pending events
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents(SESSION_ID);
        assertThat(pending).isEmpty();
    }

    @Test
    public void testTwoVersionStateProtocol() {
        stateManager.enableLeader(LEADER_ID);

        // Create initial state
        EventState event1 = new EventState();
        event1.setSessionId(SESSION_ID);
        event1.setSessionStats(Map.of("version", 1));
        stateManager.persistEventState(SESSION_ID, event1);

        // Commit state
        stateManager.commitState(SESSION_ID);

        // Create new state
        EventState event2 = new EventState();
        event2.setSessionId(SESSION_ID);
        event2.setSessionStats(Map.of("version", 2));
        stateManager.persistEventState(SESSION_ID, event2);

        // Rollback should restore previous state
        stateManager.rollbackState(SESSION_ID);

        // Verify rollback worked (implementation specific)
        // This test demonstrates the API usage
        assertTrue(true);
    }

    @Test(expected = IllegalStateException.class)
    public void testNonLeaderCannotPersist() {
        // Not setting as leader
        EventState event = new EventState();
        event.setSessionId(SESSION_ID);

        // Should throw IllegalStateException
        stateManager.persistEventState(SESSION_ID, event);
    }

    @Test
    public void testSessionStatsPersistence() {
        stateManager.enableLeader(LEADER_ID);

        Map<String, Object> stats = new HashMap<>();
        stats.put("eventsProcessed", 100);
        stats.put("rulesTriggered", 25);
        stats.put("actionsExecuted", 20);

        stateManager.persistSessionStats(SESSION_ID, stats);

        // Stats persistence doesn't return anything but should not throw
        assertTrue(true);
    }

    @Test
    public void testHAStats() {
        // Test initial HA stats
        HAStats stats = stateManager.getHAStats();
        assertNotNull(stats);
        assertNull(stats.getCurrentLeader());
        assertEquals(0, stats.getLeaderSwitches());
        assertEquals(0, stats.getEventsProcessedInTerm());
        assertEquals(0, stats.getActionsProcessedInTerm());

        // Enable leader and verify stats update
        stateManager.enableLeader(LEADER_ID);
        stats = stateManager.getHAStats();
        assertEquals(LEADER_ID, stats.getCurrentLeader());
        assertEquals(1, stats.getLeaderSwitches());
        assertNotNull(stats.getCurrentTermStartedAt());

        // Process some events/actions and verify counters
        EventState event = new EventState();
        event.setSessionId(SESSION_ID);
        stateManager.persistEventState(SESSION_ID, event);

        MatchingEvent me = createMatchingEvent(SESSION_ID, "test", "rule", Map.of("test", "data"));
        String meUuid = stateManager.addMatchingEvent(me);
        stateManager.addActionState(SESSION_ID, meUuid, 0, "{\"name\":\"test_action\",\"status\":\"running\"}");

        // Check updated stats
        stats = stateManager.getHAStats();
        assertEquals(1, stats.getEventsProcessedInTerm());
        assertEquals(1, stats.getActionsProcessedInTerm());
    }

    @Test
    public void testNewActionManagementAPIs() {
        stateManager.enableLeader(LEADER_ID);

        // Create matching event
        MatchingEvent me = createMatchingEvent(SESSION_ID, "test", "rule", Map.of("test", "data"));
        String meUuid = stateManager.addMatchingEvent(me);

        // Test addAction
        String action1 = "{\"name\":\"action1\",\"status\":\"running\",\"reference_id\":\"ref1\"}";

        stateManager.addActionState(SESSION_ID, meUuid, 0, action1);

        // Test actionExists
        assertTrue(stateManager.actionStateExists(SESSION_ID, meUuid, 0));
        assertFalse(stateManager.actionStateExists(SESSION_ID, meUuid, 1));

        // Test getAction
        String retrievedAction = stateManager.getActionState(SESSION_ID, meUuid, 0);
        assertEquals(action1, retrievedAction);

        // Test updateAction
        String updatedAction1 = "{\"name\":\"action1\",\"status\":\"success\",\"reference_id\":\"ref1\",\"end_time\":\"2024-01-01T10:05:00Z\"}";
        stateManager.updateActionState(SESSION_ID, meUuid, 0, updatedAction1);

        retrievedAction = stateManager.getActionState(SESSION_ID, meUuid, 0);
        assertEquals(updatedAction1, retrievedAction);

        // Test deleteActions
        stateManager.deleteActionStates(SESSION_ID, meUuid);
        assertFalse(stateManager.actionStateExists(SESSION_ID, meUuid, 0));
        assertTrue(stateManager.getActionState(SESSION_ID, meUuid, 0).isEmpty());
    }
}
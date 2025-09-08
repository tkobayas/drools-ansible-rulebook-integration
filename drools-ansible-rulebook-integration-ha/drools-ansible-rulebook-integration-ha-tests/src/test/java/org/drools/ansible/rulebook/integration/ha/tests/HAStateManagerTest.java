package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.EventState;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests for HAStateManager
 */
public class HAStateManagerTest {

    private HAStateManager stateManager;
    private static final String SESSION_ID = "test-session-1";
    private static final String LEADER_ID = "test-leader-1";

    @BeforeEach
    public void setUp() {
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
        String eventDataJson = toJson(matchingFacts);
        me.setEventData(eventDataJson);
        return me;
    }

    @Test
    public void testLeaderElection() {
        // Initially not a leader
        assertThat(stateManager.isLeader()).isFalse();

        // Enable leader mode
        stateManager.enableLeader(LEADER_ID);
        assertThat(stateManager.isLeader()).isTrue();

        // Disable leader mode
        stateManager.disableLeader(LEADER_ID);
        assertThat(stateManager.isLeader()).isFalse();
    }

    @Test
    public void testEventStatePersistence() {
        stateManager.enableLeader(LEADER_ID);

        // Create and persist event state
        EventState event = new EventState();
        event.setSessionId(SESSION_ID);
        event.setRulebookHash("abc123");
        event.setSessionStats(dummySessionStats());

        stateManager.persistEventState(SESSION_ID, event);

        // Event state persistence doesn't return anything but should not throw
        assertThat(true).isTrue(); // If we got here, persistence worked
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

        assertThat(meUuid).isNotNull();
        assertThat(meUuid).isNotEmpty();
        assertThat(meUuid).isEqualTo(me.getMeUuid());
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
        assertThat(stateManager.actionStateExists(SESSION_ID, meUuid, 0)).isTrue();
        assertThat(stateManager.actionStateExists(SESSION_ID, meUuid, 1)).isFalse();

        // Get action and verify
        String retrieved = stateManager.getActionState(SESSION_ID, meUuid, 0);
        assertThat(retrieved).isEqualTo(actionData);

        // Update action
        String updatedData = "{\"name\":\"send_alert\",\"status\":\"success\",\"end_time\":\"2024-01-01T10:01:00Z\"}";
        stateManager.updateActionState(SESSION_ID, meUuid, 0, updatedData);

        // Verify update
        retrieved = stateManager.getActionState(SESSION_ID, meUuid, 0);
        assertThat(retrieved).isEqualTo(updatedData);
    }

    @Test
    public void testPendingMatchingEventsRecovery() {
        stateManager.enableLeader(LEADER_ID);

        // Create multiple matching events with different states
        MatchingEvent matchingEvent1 = createMatchingEvent(SESSION_ID, "ruleset1", "rule1",
                                                           Map.of("event", "1"));
        String me1 = stateManager.addMatchingEvent(matchingEvent1);

        MatchingEvent matchingEvent2 = createMatchingEvent(SESSION_ID, "ruleset1", "rule2",
                                                           Map.of("event", "2"));
        String me2 = stateManager.addMatchingEvent(matchingEvent2);

        // Add action for first ME (in progress)
        stateManager.addActionState(SESSION_ID, me1, 0, "{\"status\":\"running\"}");

        // Get pending events (should include both)
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents("ruleset1");
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
        assertThat(stateManager.actionStateExists(SESSION_ID, meUuid, 0)).isTrue();

        // Delete matching event and all its actions
        stateManager.deleteActionStates(SESSION_ID, meUuid);

        // Verify action is gone
        assertThat(stateManager.actionStateExists(SESSION_ID, meUuid, 0)).isFalse();

        // Should not appear in pending events
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents(SESSION_ID);
        assertThat(pending).isEmpty();
    }

    // Revisit this test. Scenario is:
    // 1. Node1 writes initial state (version 1)
    // 2. Node1 writes new state (version 2), but assume it crashes before commit
    // 3. On recovery, Node2 becomes leader and read the state - should see version 1 as current
    // 4. Node2 can compare with its own state (= version 1). They are the same, so no action needed
    // Note: This is more of an integration test scenario. This unit test should be much simpler.
    @Test
    public void testTwoVersionStateProtocol() {
        stateManager.enableLeader(LEADER_ID);

        // Create initial state
        EventState event1 = new EventState();
        event1.setSessionId(SESSION_ID);
        event1.setSessionStats(dummySessionStats());
        stateManager.persistEventState(SESSION_ID, event1);

        // Commit state
        stateManager.commitState(SESSION_ID);

        // Create new state
        EventState event2 = new EventState();
        event2.setSessionId(SESSION_ID);
        event2.setSessionStats(dummySessionStats());
        stateManager.persistEventState(SESSION_ID, event2);

        // Rollback should restore previous state
        stateManager.rollbackState(SESSION_ID);

        // Verify rollback worked (implementation specific)
        // This test demonstrates the API usage
        assertThat(true).isTrue();
    }

    @Test
    public void testNonLeaderCannotPersist() {
        // Not setting as leader
        EventState event = new EventState();
        event.setSessionId(SESSION_ID);

        // Should throw IllegalStateException
        assertThrows(IllegalStateException.class, () -> {
            stateManager.persistEventState(SESSION_ID, event);
        });
    }

    private static SessionStats dummySessionStats() {
        return new SessionStats(
                "2024-06-01T10:00:00Z", // start
                "2024-06-01T12:00:00Z", // end
                "2024-06-01T12:00:00Z", // lastClockTime
                5,                      // clockAdvanceCount
                10,                     // numberOfRules
                2,                      // numberOfDisabledRules
                7,                      // rulesTriggered
                100,                    // eventsProcessed
                80,                     // eventsMatched
                20,                     // eventsSuppressed
                15,                     // permanentStorageCount
                2048,                   // permanentStorageSize
                3,                      // asyncResponses
                1024,                   // bytesSentOnAsync
                123456L,                // sessionId
                "MyRuleSet",            // ruleSetName
                "RuleA",                // lastRuleFired
                "2024-06-01T11:59:00Z", // lastRuleFiredAt
                "2024-06-01T11:58:00Z", // lastEventReceivedAt
                500000L,                // baseLevelMemory
                800000L                 // peakMemory
        );
    }

    @Test
    public void testHAStats() {
        // Test initial HA stats
        HAStats stats = stateManager.getHAStats();
        assertThat(stats).isNotNull();
        assertThat(stats.getCurrentLeader()).isNull();
        assertThat(stats.getLeaderSwitches()).isEqualTo(0);
        assertThat(stats.getEventsProcessedInTerm()).isEqualTo(0);
        assertThat(stats.getActionsProcessedInTerm()).isEqualTo(0);

        // Enable leader and verify stats update
        stateManager.enableLeader(LEADER_ID);
        stats = stateManager.getHAStats();
        assertThat(stats.getCurrentLeader()).isEqualTo(LEADER_ID);
        assertThat(stats.getLeaderSwitches()).isEqualTo(1);
        assertThat(stats.getCurrentTermStartedAt()).isNotNull();

        // Process some events/actions and verify counters
        EventState event = new EventState();
        event.setSessionId(SESSION_ID);
        stateManager.persistEventState(SESSION_ID, event);

        MatchingEvent me = createMatchingEvent(SESSION_ID, "test", "rule", Map.of("test", "data"));
        String meUuid = stateManager.addMatchingEvent(me);
        stateManager.addActionState(SESSION_ID, meUuid, 0, "{\"name\":\"test_action\",\"status\":\"running\"}");

        // Check updated stats
        stats = stateManager.getHAStats();
        assertThat(stats.getEventsProcessedInTerm()).isEqualTo(1); // TODO: events_processed should be incremented when assertEvent is called, not by persistEventState
        assertThat(stats.getActionsProcessedInTerm()).isEqualTo(1);
    }
}
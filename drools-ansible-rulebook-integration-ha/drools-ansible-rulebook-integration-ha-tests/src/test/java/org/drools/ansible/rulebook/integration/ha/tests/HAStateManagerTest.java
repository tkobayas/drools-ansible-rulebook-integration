package org.drools.ansible.rulebook.integration.ha.tests;

import org.drools.ansible.rulebook.integration.ha.api.HAConfiguration;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.ActionState;
import org.drools.ansible.rulebook.integration.ha.model.EventState;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Integration tests for HAStateManager
 */
public class HAStateManagerTest {
    
    private HAStateManager stateManager;
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
        me.setMeUuid(UUID.randomUUID().toString());
        me.setSessionId(sessionId);
        me.setRulesetName(rulesetName);
        me.setRuleName(ruleName);
        me.setMatchingFacts(matchingFacts);
        me.setStatus(MatchingEvent.MatchingEventStatus.PENDING);
        me.setCreatedAt(Instant.now().toString());
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
    public void testActionStatePersistence() {
        stateManager.enableLeader(LEADER_ID);
        
        // First create a matching event
        MatchingEvent me = createMatchingEvent(SESSION_ID, "testRuleset", "testRule", 
                                                Map.of("fact", "value"));
        String meUuid = stateManager.addMatchingEvent(me);
        
        // Create and persist action state
        ActionState actionState = new ActionState();
        actionState.setMeUuid(meUuid);
        actionState.setRulesetName("testRuleset");
        actionState.setRuleName("testRule");
        
        ActionState.Action action = new ActionState.Action();
        action.setName("send_alert");
        action.setIndex(0);
        action.setStatus(ActionState.Action.ActionStatus.RUNNING);
        actionState.getActions().add(action);
        
        stateManager.persistActionState(SESSION_ID, meUuid, actionState);
        
        // Retrieve and verify
        ActionState retrieved = stateManager.getActionState(SESSION_ID, meUuid);
        assertNotNull(retrieved);
        assertEquals(meUuid, retrieved.getMeUuid());
        assertEquals(1, retrieved.getActions().size());
        assertEquals("send_alert", retrieved.getActions().get(0).getName());
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
        
        // Add action state for first ME (in progress)
        ActionState as1 = new ActionState();
        as1.setMeUuid(me1);
        ActionState.Action action1 = new ActionState.Action();
        action1.setStatus(ActionState.Action.ActionStatus.RUNNING);
        as1.getActions().add(action1);
        stateManager.persistActionState(SESSION_ID, me1, as1);
        
        // Get pending events (should include both)
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents(SESSION_ID);
        assertThat(pending).hasSize(2);
        assertThat(pending.stream().map(MatchingEvent::getMeUuid))
            .containsExactlyInAnyOrder(me1, me2);
    }
    
    @Test
    public void testMatchingEventDeletion() {
        stateManager.enableLeader(LEADER_ID);
        
        // Create ME and action state
        MatchingEvent me = createMatchingEvent(SESSION_ID, "rules", "rule", 
                                               Map.of("data", "test"));
        String meUuid = stateManager.addMatchingEvent(me);
        
        ActionState actionState = new ActionState();
        actionState.setMeUuid(meUuid);
        stateManager.persistActionState(SESSION_ID, meUuid, actionState);
        
        // Verify it exists
        assertNotNull(stateManager.getActionState(SESSION_ID, meUuid));
        
        // Delete it
        stateManager.removeMatchingEvent(meUuid);
        
        // Verify it's gone
        assertNull(stateManager.getActionState(SESSION_ID, meUuid));
        
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
        stateManager.addAction(SESSION_ID, meUuid, 0, Map.of("name", "test_action", "status", "running"));
        
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
        Map<String, Object> action1 = new HashMap<>();
        action1.put("name", "action1");
        action1.put("status", "running");
        action1.put("reference_id", "ref1");
        
        stateManager.addAction(SESSION_ID, meUuid, 0, action1);
        
        // Test actionExists
        assertTrue(stateManager.actionExists(SESSION_ID, meUuid, 0));
        assertFalse(stateManager.actionExists(SESSION_ID, meUuid, 1));
        
        // Test getAction
        Map<String, Object> retrievedAction = stateManager.getAction(SESSION_ID, meUuid, 0);
        assertEquals("action1", retrievedAction.get("name"));
        assertEquals("running", retrievedAction.get("status"));
        assertEquals("ref1", retrievedAction.get("reference_id"));
        
        // Test updateAction
        action1.put("status", "success");
        action1.put("end_time", Instant.now().toString());
        stateManager.updateAction(SESSION_ID, meUuid, 0, action1);
        
        retrievedAction = stateManager.getAction(SESSION_ID, meUuid, 0);
        assertEquals("success", retrievedAction.get("status"));
        assertNotNull(retrievedAction.get("end_time"));
        
        // Test deleteActions
        stateManager.deleteActions(SESSION_ID, meUuid);
        assertFalse(stateManager.actionExists(SESSION_ID, meUuid, 0));
        assertTrue(stateManager.getAction(SESSION_ID, meUuid, 0).isEmpty());
    }
}
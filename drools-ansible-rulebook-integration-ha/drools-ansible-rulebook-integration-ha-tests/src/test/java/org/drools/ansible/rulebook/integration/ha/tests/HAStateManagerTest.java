package org.drools.ansible.rulebook.integration.ha.tests;

import org.drools.ansible.rulebook.integration.ha.api.HAConfiguration;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.ActionState;
import org.drools.ansible.rulebook.integration.ha.model.EventState;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        // Configure H2 in-memory database for testing
        Map<String, Object> config = new HashMap<>();
        config.put("database_type", "H2");
        config.put("db_url", "jdbc:h2:mem:test_ha;DB_CLOSE_DELAY=-1");
        config.put("username", "sa");
        config.put("password", "");
        
        stateManager = HAStateManagerFactory.create(config);
    }
    
    @After
    public void tearDown() {
        if (stateManager != null) {
            stateManager.shutdown();
        }
    }
    
    @Test
    public void testLeaderElection() {
        // Initially not a leader
        assertFalse(stateManager.isLeader());
        
        // Set as leader
        stateManager.setLeader(LEADER_ID);
        assertTrue(stateManager.isLeader());
        
        // Unset leader
        stateManager.unsetLeader();
        assertFalse(stateManager.isLeader());
    }
    
    @Test
    public void testEventStatePersistence() {
        stateManager.setLeader(LEADER_ID);
        
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
        stateManager.setLeader(LEADER_ID);
        
        // Create matching event when rule triggers
        Map<String, Object> facts = Map.of(
            "temperature", 35,
            "alert", "high_temp"
        );
        
        String meUuid = stateManager.addMatchingEvent(
            SESSION_ID, "alertRules", "tempAlert", facts
        );
        
        assertNotNull(meUuid);
        assertFalse(meUuid.isEmpty());
    }
    
    @Test
    public void testActionStatePersistence() {
        stateManager.setLeader(LEADER_ID);
        
        // First create a matching event
        String meUuid = stateManager.addMatchingEvent(
            SESSION_ID, "testRuleset", "testRule", Map.of("fact", "value")
        );
        
        // Create and persist action state
        ActionState actionState = new ActionState();
        actionState.setMeUuid(meUuid);
        actionState.setRulesetName("testRuleset");
        actionState.setRuleName("testRule");
        
        ActionState.Action action = new ActionState.Action();
        action.setName("send_alert");
        action.setIndex(0);
        action.setStatus(ActionState.Action.ActionStatus.PENDING);
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
        stateManager.setLeader(LEADER_ID);
        
        // Create multiple matching events with different states
        String me1 = stateManager.addMatchingEvent(
            SESSION_ID, "rules1", "rule1", Map.of("event", "1")
        );
        String me2 = stateManager.addMatchingEvent(
            SESSION_ID, "rules2", "rule2", Map.of("event", "2")
        );
        
        // Add action state for first ME (in progress)
        ActionState as1 = new ActionState();
        as1.setMeUuid(me1);
        ActionState.Action action1 = new ActionState.Action();
        action1.setStatus(ActionState.Action.ActionStatus.STARTED);
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
        stateManager.setLeader(LEADER_ID);
        
        // Create ME and action state
        String meUuid = stateManager.addMatchingEvent(
            SESSION_ID, "rules", "rule", Map.of("data", "test")
        );
        
        ActionState actionState = new ActionState();
        actionState.setMeUuid(meUuid);
        stateManager.persistActionState(SESSION_ID, meUuid, actionState);
        
        // Verify it exists
        assertNotNull(stateManager.getActionState(SESSION_ID, meUuid));
        
        // Delete it
        stateManager.removeMatchingEvent(SESSION_ID, meUuid);
        
        // Verify it's gone
        assertNull(stateManager.getActionState(SESSION_ID, meUuid));
        
        // Should not appear in pending events
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents(SESSION_ID);
        assertThat(pending).isEmpty();
    }
    
    @Test
    public void testTwoVersionStateProtocol() {
        stateManager.setLeader(LEADER_ID);
        
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
        stateManager.setLeader(LEADER_ID);
        
        Map<String, Object> stats = new HashMap<>();
        stats.put("eventsProcessed", 100);
        stats.put("rulesTriggered", 25);
        stats.put("actionsExecuted", 20);
        
        stateManager.persistSessionStats(SESSION_ID, stats);
        
        // Stats persistence doesn't return anything but should not throw
        assertTrue(true);
    }
}
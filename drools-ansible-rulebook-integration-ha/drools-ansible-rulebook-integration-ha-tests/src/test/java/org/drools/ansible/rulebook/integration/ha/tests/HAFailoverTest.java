package org.drools.ansible.rulebook.integration.ha.tests;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.ActionState;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Tests for HA failover scenarios
 */
public class HAFailoverTest {
    
    private static final String SESSION_ID = "failover-session";
    
    @Test
    public void testLeaderFailoverWithPendingActions() {
        // Shared database URL for simulating multiple nodes
        String dbUrl = "jdbc:h2:mem:failover_test;DB_CLOSE_DELAY=-1";
        
        // Node 1 becomes leader
        HAStateManager node1 = createNode(dbUrl);
        node1.setLeader("node-1");
        
        // Create matching events with actions in progress
        String me1 = node1.addMatchingEvent(SESSION_ID, "rules", "alert", 
                                               Map.of("alert", "critical"));
        
        // Start action execution
        ActionState actionState = new ActionState();
        actionState.setMeUuid(me1);
        ActionState.Action action = new ActionState.Action();
        action.setName("send_notification");
        action.setStatus(ActionState.Action.ActionStatus.STARTED);
        action.setReferenceId("job-123");
        actionState.getActions().add(action);
        
        node1.persistActionState(SESSION_ID, me1, actionState);
        
        // Simulate node 1 failure
        node1.unsetLeader();
        node1.shutdown();
        
        // Node 2 takes over
        HAStateManager node2 = createNode(dbUrl);
        node2.setLeader("node-2");
        
        // Node 2 should see pending actions
        List<MatchingEvent> pending = node2.getPendingMatchingEvents(SESSION_ID);
        assertThat(pending).hasSize(1);
        
        MatchingEvent recovered = pending.get(0);
        assertEquals(me1, recovered.getMeUuid());
        
        // Check action state was preserved
        ActionState recoveredState = node2.getActionState(SESSION_ID, me1);
        assertNotNull(recoveredState);
        assertEquals(1, recoveredState.getActions().size());
        assertEquals("job-123", recoveredState.getActions().get(0).getReferenceId());
        assertEquals(ActionState.Action.ActionStatus.STARTED, 
                    recoveredState.getActions().get(0).getStatus());
        
        // Node 2 can complete the action
        recoveredState.getActions().get(0).setStatus(ActionState.Action.ActionStatus.COMPLETED);
        node2.persistActionState(SESSION_ID, me1, recoveredState);
        
        // Clean up
        node2.removeMatchingEvent(SESSION_ID, me1);
        node2.shutdown();
    }
    
    @Test
    public void testMultipleFailovers() {
        String dbUrl = "jdbc:h2:mem:multi_failover;DB_CLOSE_DELAY=-1";
        
        // Node 1 starts work
        HAStateManager node1 = createNode(dbUrl);
        node1.setLeader("node-1");
        String me1 = node1.addMatchingEvent(SESSION_ID, "rules", "rule1", Map.of("data", "1"));
        node1.unsetLeader();
        
        // Node 2 takes over
        HAStateManager node2 = createNode(dbUrl);
        node2.setLeader("node-2");
        List<MatchingEvent> pending2 = node2.getPendingMatchingEvents(SESSION_ID);
        assertThat(pending2).hasSize(1);
        
        // Node 2 creates more work
        String me2 = node2.addMatchingEvent(SESSION_ID, "rules", "rule2", Map.of("data", "2"));
        node2.unsetLeader();
        
        // Node 3 takes over
        HAStateManager node3 = createNode(dbUrl);
        node3.setLeader("node-3");
        List<MatchingEvent> pending3 = node3.getPendingMatchingEvents(SESSION_ID);
        
        // Should see both MEs
        assertThat(pending3).hasSize(2);
        assertThat(pending3.stream().map(MatchingEvent::getMeUuid))
            .containsExactlyInAnyOrder(me1, me2);
        
        // Clean up
        node1.shutdown();
        node2.shutdown();
        node3.shutdown();
    }
    
    @Test
    public void testSplitBrainRecovery() {
        String dbUrl = "jdbc:h2:mem:split_brain;DB_CLOSE_DELAY=-1";
        
        // Both nodes think they're leader (split brain)
        HAStateManager node1 = createNode(dbUrl);
        HAStateManager node2 = createNode(dbUrl);
        
        node1.setLeader("node-1");
        node2.setLeader("node-2");
        
        // Both try to create MEs
        String me1 = node1.addMatchingEvent(SESSION_ID, "rules", "rule1", Map.of("from", "node1"));
        String me2 = node2.addMatchingEvent(SESSION_ID, "rules", "rule2", Map.of("from", "node2"));
        
        // Resolve split brain - node2 wins
        node1.unsetLeader();
        
        // Node2 should see both MEs
        List<MatchingEvent> allEvents = node2.getPendingMatchingEvents(SESSION_ID);
        assertThat(allEvents).hasSize(2);
        
        // Clean up
        node1.shutdown();
        node2.shutdown();
    }
    
    @Test
    public void testActionRetryAfterFailure() {
        String dbUrl = "jdbc:h2:mem:action_retry;DB_CLOSE_DELAY=-1";
        
        HAStateManager node1 = createNode(dbUrl);
        node1.setLeader("node-1");
        
        // Create ME with failed action
        String meUuid = node1.addMatchingEvent(SESSION_ID, "rules", "retry_rule", 
                                                  Map.of("retry", true));
        
        ActionState failedAction = new ActionState();
        failedAction.setMeUuid(meUuid);
        ActionState.Action action = new ActionState.Action();
        action.setName("flaky_action");
        action.setStatus(ActionState.Action.ActionStatus.FAILED);
        action.setReferenceId("failed-job-1");
        failedAction.getActions().add(action);
        
        node1.persistActionState(SESSION_ID, meUuid, failedAction);
        node1.unsetLeader();
        
        // New leader retries
        HAStateManager node2 = createNode(dbUrl);
        node2.setLeader("node-2");
        
        ActionState toRetry = node2.getActionState(SESSION_ID, meUuid);
        assertEquals(ActionState.Action.ActionStatus.FAILED, 
                    toRetry.getActions().get(0).getStatus());
        
        // Retry the action
        toRetry.getActions().get(0).setStatus(ActionState.Action.ActionStatus.STARTED);
        toRetry.getActions().get(0).setReferenceId("retry-job-2");
        node2.persistActionState(SESSION_ID, meUuid, toRetry);
        
        // Eventually succeed
        toRetry.getActions().get(0).setStatus(ActionState.Action.ActionStatus.COMPLETED);
        node2.persistActionState(SESSION_ID, meUuid, toRetry);
        
        // Clean up
        node2.removeMatchingEvent(SESSION_ID, meUuid);
        node1.shutdown();
        node2.shutdown();
    }
    
    private HAStateManager createNode(String dbUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put("database_type", "H2");
        config.put("db_url", dbUrl);
        config.put("username", "sa");
        config.put("password", "");
        return HAStateManagerFactory.create(config);
    }
}
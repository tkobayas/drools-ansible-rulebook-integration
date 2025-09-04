package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for HA failover scenarios
 */
public class HAFailoverTest {

    private static final String SESSION_ID = "failover-session";

    // Utility method to create a MatchingEvent with default values
    private MatchingEvent createMatchingEvent(String sessionId, String rulesetName,
                                              String ruleName, Map<String, Object> matchingFacts) {
        MatchingEvent me = new MatchingEvent();
        me.setSessionId(sessionId);
        me.setRuleSetName(rulesetName);
        me.setRuleName(ruleName);

        // Serialize matching facts to JSON
        try {
            String eventDataJson = new ObjectMapper().writeValueAsString(matchingFacts);
            me.setEventData(eventDataJson);
        } catch (Exception e) {
            me.setEventData("{}");
        }

        return me;
    }

    @Test
    public void testLeaderFailoverWithPendingActions() {
        // Shared database URL for simulating multiple nodes
        String dbUrl = "jdbc:h2:mem:failover_test;DB_CLOSE_DELAY=-1";

        // Node 1 becomes leader
        HAStateManager node1 = createNode(dbUrl);
        node1.enableLeader("node-1");

        // Create matching events with actions in progress
        MatchingEvent me = createMatchingEvent(SESSION_ID, "rules", "alert",
                                               Map.of("alert", "critical"));
        String me1 = node1.addMatchingEvent(me);

        // Start action execution
        Map<String, Object> actionData = Map.of(
                "name", "send_notification",
                "status", "running",
                "reference_id", "job-123"
        );

        node1.addAction(SESSION_ID, me1, 0, actionData);

        // Simulate node 1 failure
        node1.disableLeader("node-1");
        node1.shutdown();

        // Node 2 takes over
        HAStateManager node2 = createNode(dbUrl);
        node2.enableLeader("node-2");

        // Node 2 should see pending actions
        List<MatchingEvent> pending = node2.getPendingMatchingEvents(SESSION_ID);
        assertThat(pending).hasSize(1);

        MatchingEvent recovered = pending.get(0);
        assertEquals(me1, recovered.getMeUuid());

        // Check action was preserved
        assertTrue(node2.actionExists(SESSION_ID, me1, 0));
        Map<String, Object> recoveredAction = node2.getAction(SESSION_ID, me1, 0);
        assertEquals("job-123", recoveredAction.get("reference_id"));
        assertEquals("running", recoveredAction.get("status"));

        // Node 2 can complete the action
        Map<String, Object> completedAction = Map.of(
                "name", "send_notification",
                "status", "success",
                "reference_id", "job-123"
        );
        node2.updateAction(SESSION_ID, me1, 0, completedAction);

        // Clean up
        node2.deleteActions(SESSION_ID, me1);
        node2.shutdown();
    }

    @Test
    public void testMultipleFailovers() {
        String dbUrl = "jdbc:h2:mem:multi_failover;DB_CLOSE_DELAY=-1";

        // Node 1 starts work
        HAStateManager node1 = createNode(dbUrl);
        node1.enableLeader("node-1");

        MatchingEvent matchingEvent1 = createMatchingEvent(SESSION_ID, "rules", "rule1",
                                                           Map.of("data", "1"));
        String me1 = node1.addMatchingEvent(matchingEvent1);

        node1.disableLeader("node-1");

        // Node 2 takes over
        HAStateManager node2 = createNode(dbUrl);
        node2.enableLeader("node-2");
        List<MatchingEvent> pending2 = node2.getPendingMatchingEvents(SESSION_ID);
        assertThat(pending2).hasSize(1);

        // Node 2 creates more work
        MatchingEvent matchingEvent2 = createMatchingEvent(SESSION_ID, "rules", "rule2",
                                                           Map.of("data", "2"));
        String me2 = node2.addMatchingEvent(matchingEvent2);
        node2.disableLeader("node-1");

        // Node 3 takes over
        HAStateManager node3 = createNode(dbUrl);
        node3.enableLeader("node-3");
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

        node1.enableLeader("node-1");
        node2.enableLeader("node-2");

        // Both try to create MEs
        MatchingEvent matchingEvent1 = createMatchingEvent(SESSION_ID, "rules", "rule1",
                                                           Map.of("from", "node1"));
        String me1 = node1.addMatchingEvent(matchingEvent1);

        MatchingEvent matchingEvent2 = createMatchingEvent(SESSION_ID, "rules", "rule2",
                                                           Map.of("from", "node2"));
        String me2 = node2.addMatchingEvent(matchingEvent2);

        // Resolve split brain - node2 wins
        node1.disableLeader("node-1");

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
        node1.enableLeader("node-1");

        // Create ME with failed action
        MatchingEvent me = createMatchingEvent(SESSION_ID, "rules", "retry_rule",
                                               Map.of("retry", true));
        String meUuid = node1.addMatchingEvent(me);

        Map<String, Object> failedActionData = Map.of(
                "name", "flaky_action",
                "status", "failed",
                "reference_id", "failed-job-1"
        );

        node1.addAction(SESSION_ID, meUuid, 0, failedActionData);
        node1.disableLeader("node-1");

        // New leader retries
        HAStateManager node2 = createNode(dbUrl);
        node2.enableLeader("node-2");

        Map<String, Object> failedAction = node2.getAction(SESSION_ID, meUuid, 0);
        assertEquals("failed", failedAction.get("status"));

        // Retry the action
        Map<String, Object> retryAction = Map.of(
                "name", "flaky_action",
                "status", "running",
                "reference_id", "retry-job-2"
        );
        node2.updateAction(SESSION_ID, meUuid, 0, retryAction);

        // Eventually succeed
        Map<String, Object> successAction = Map.of(
                "name", "flaky_action",
                "status", "success",
                "reference_id", "retry-job-2"
        );
        node2.updateAction(SESSION_ID, meUuid, 0, successAction);

        // Clean up
        node2.deleteActions(SESSION_ID, meUuid);
        node1.shutdown();
        node2.shutdown();
    }

    private HAStateManager createNode(String dbUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put("database_type", "H2");
        config.put("db_url", dbUrl);
        config.put("username", "sa");
        config.put("password", "");
        config.put("write_after", 1);
        HAStateManager manager = HAStateManagerFactory.createH2();
        manager.initializeHA("test-failover-" + System.nanoTime(), new HashMap<>(), config);
        return manager;
    }
}
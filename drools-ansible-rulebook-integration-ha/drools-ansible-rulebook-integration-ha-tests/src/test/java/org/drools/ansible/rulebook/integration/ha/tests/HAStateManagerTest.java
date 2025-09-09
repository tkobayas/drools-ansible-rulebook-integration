package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.HashMap;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createMatchingEvent;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * General function tests for HAStateManager
 *
 * For Action and MatchingEvent related tests see HAStateManagerActionTest
 * For Session related tests see HAStateManagerSessionTest
 */
class HAStateManagerTest {

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
    void testLeaderElection() {
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
    void testNonLeaderCannotPersist() {
        // Not setting as leader
        SessionState sessionState = new SessionState();
        sessionState.setSessionId(SESSION_ID);

        // Should throw IllegalStateException
        assertThrows(IllegalStateException.class, () -> {
            stateManager.persistSessionState(SESSION_ID, sessionState);
        });
    }

    @Test
    void testHAStats() {
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
        SessionState sessionState = new SessionState();
        sessionState.setSessionId(SESSION_ID);
        stateManager.persistSessionState(SESSION_ID, sessionState);

        MatchingEvent me = createMatchingEvent(SESSION_ID, "test", "rule", Map.of("test", "data"));
        String meUuid = stateManager.addMatchingEvent(me);
        stateManager.addActionInfo(SESSION_ID, meUuid, 0, "{\"name\":\"test_action\",\"status\":\"running\"}");

        // Check updated stats
        stats = stateManager.getHAStats();
        assertThat(stats.getEventsProcessedInTerm()).isEqualTo(1); // TODO: events_processed should be incremented when assertEvent is called, not by persistSessionState
        assertThat(stats.getActionsProcessedInTerm()).isEqualTo(1);
    }
}
package org.drools.ansible.rulebook.integration.ha.tests;

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
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_HA_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_PG_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createMatchingEvent;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.dropTables;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * General function tests for HAStateManager
 *
 * For Action and MatchingEvent related tests see HAStateManagerActionTest
 * For Session related tests see HAStateManagerSessionTest
 */
class HAStateManagerTest {

    private HAStateManager stateManager;
    private static final String HA_UUID = "test-ha-1";
    private static final String LEADER_ID = "test-leader-1";
    private static final String RULE_SET_NAME = "testRuleset";

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
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);

        // Should throw IllegalStateException
        assertThrows(IllegalStateException.class, () -> {
            stateManager.persistSessionState(sessionState);
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
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        stateManager.persistSessionState(sessionState);

        MatchingEvent me = createMatchingEvent(HA_UUID, "test", "rule", Map.of("test", "data"));
        String meUuid = stateManager.addMatchingEvent(me);
        stateManager.addActionInfo(meUuid, 0, "{\"name\":\"test_action\",\"status\":\"running\"}");

        // Check updated stats
        stats = stateManager.getHAStats();
        assertThat(stats.getEventsProcessedInTerm()).isEqualTo(0); // Events processed not incremented in this test
        assertThat(stats.getActionsProcessedInTerm()).isEqualTo(1);
    }
}

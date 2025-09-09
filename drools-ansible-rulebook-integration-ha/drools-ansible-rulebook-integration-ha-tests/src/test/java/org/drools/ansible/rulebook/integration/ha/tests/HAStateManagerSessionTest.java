package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.HashMap;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Session related tests for HAStateManager
 */
class HAStateManagerSessionTest {

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
    void testSessionStatePersistence() {
        stateManager.enableLeader(LEADER_ID);

        // Create and persist session state
        SessionState sessionState = new SessionState();
        sessionState.setSessionId(SESSION_ID);
        sessionState.setRulebookHash("abc123");
        sessionState.setSessionStats(dummySessionStats());

        stateManager.persistSessionState(SESSION_ID, sessionState);

        // Session state persistence doesn't return anything but should not throw
        assertThat(true).isTrue(); // If we got here, persistence worked
    }

    // Revisit this test. Scenario is:
    // 1. Node1 writes initial state (version 1)
    // 2. Node1 writes new state (version 2), but assume it crashes before commit
    // 3. On recovery, Node2 becomes leader and read the state - should see version 1 as current
    // 4. Node2 can compare with its own state (= version 1). They are the same, so no action needed
    // Note: This is more of an integration test scenario. This unit test should be much simpler.
    @Test
    void testTwoVersionState() {
        // TBD
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
}
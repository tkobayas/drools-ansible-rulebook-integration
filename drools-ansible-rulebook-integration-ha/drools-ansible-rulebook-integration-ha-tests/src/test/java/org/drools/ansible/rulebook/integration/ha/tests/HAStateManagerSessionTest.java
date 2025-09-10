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
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_HA_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_PG_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.dropTables;

/**
 * Session related tests for HAStateManager
 */
class HAStateManagerSessionTest {

    private HAStateManager stateManager;
    private static final String HA_UUID = "test-ha-1";
    private static final String LEADER_ID = "test-leader-1";

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
    void testSessionStatePersistence() {
        stateManager.enableLeader(LEADER_ID);

        // Create and persist session state
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRulebookHash("abc123");
        sessionState.setLeaderId(LEADER_ID);
        Map<String, Object> partialEvents = Map.of("event1", Map.of("data", "value1"));
        sessionState.setPartialEvents(partialEvents);
        sessionState.setClockTimeMillis(1625079600000L);
        SessionStats stats = dummySessionStats();
        sessionState.setSessionStats(stats);
        sessionState.setVersion(1);
        sessionState.setCurrent(true);
        sessionState.setCreatedAt("2024-06-01T10:00:00Z");

        stateManager.persistSessionState(sessionState);

        // Retrieve the persisted session state
        SessionState retrievedState = stateManager.getSessionState();

        // Verify the retrieved state matches what was persisted
        assertThat(retrievedState).isNotNull();
        assertThat(retrievedState.getHaUuid()).isEqualTo(HA_UUID);
        assertThat(retrievedState.getRulebookHash()).isEqualTo("abc123");
        assertThat(retrievedState.getLeaderId()).isEqualTo(LEADER_ID);
        assertThat(retrievedState.getPartialEvents()).isEqualTo(partialEvents);
        assertThat(retrievedState.getClockTimeMillis()).isEqualTo(1625079600000L);
        assertThat(retrievedState.getSessionStats()).usingRecursiveComparison().isEqualTo(stats);
        assertThat(retrievedState.getVersion()).isEqualTo(1);
        assertThat(retrievedState.isCurrent()).isTrue();
        assertThat(retrievedState.getCreatedAt()).isEqualTo("2024-06-01T10:00:00Z");
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
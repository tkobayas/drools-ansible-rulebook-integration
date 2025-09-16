package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

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

    public static final String ALL_CONDITION_RULE =
            """
            {
                "name": "Test Ruleset",
                "rules": [
                        {
                            "Rule": {
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "i"
                                                },
                                                "rhs": {
                                                    "Integer": 1
                                                }
                                            }
                                        },
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "j"
                                                },
                                                "rhs": {
                                                    "Integer": 2
                                                }
                                            }
                                        }
                                    ]
                                },
                                "enabled": true,
                                "name": null
                            }
                        }
                    ]
            }
            """;

    @Test
    void testSessionRecoveryWithPartialMatch() {
        stateManager.enableLeader(LEADER_ID);

        RulesExecutor rulesExecutor1 = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK), ALL_CONDITION_RULE);

        String eventJson = "{\"i\":1}";
        long insertedAt = System.currentTimeMillis();

        List<EventRecord> partialEvents = List.of(new EventRecord(eventJson, insertedAt));

        List<Match> matchList = rulesExecutor1.processEvents(eventJson).join(); // partial match
        assertThat(matchList).isEmpty();

        // Create and persist session state
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRulebookHash("abc123");
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setPartialEvents(partialEvents);
        sessionState.setPersistedTime(1625079600000L);
        sessionState.setVersion(1);
        sessionState.setCurrent(true);
        sessionState.setCreatedTime(1717232400000L); // 2024-06-01T10:00:00Z

        stateManager.persistSessionState(sessionState);

        //--------

        // Simulate that a node crashes
        rulesExecutor1 = null;

        // On recovery, create a new RulesExecutor instance
        RulesExecutor rulesExecutor2 = RulesExecutorFactory.createFromJson(ALL_CONDITION_RULE);

        // Retrieve the persisted session state
        SessionState retrievedState = stateManager.getSessionState();

//        stateManager.restoreSession();
    }
}
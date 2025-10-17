package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.calculateStateSHA;
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
    private static final String RULE_SET_NAME = "Test Ruleset";

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

        // This test works without HARulesExecutor
        RulesExecutor rulesExecutor1 = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK), ALL_CONDITION_RULE);
        long createdTime = rulesExecutor1.asKieSession().getSessionClock().getCurrentTime();

        rulesExecutor1.advanceTime(10, java.util.concurrent.TimeUnit.SECONDS);

        String eventJson = "{\"i\":1}";
        long insertedAt = createdTime + 10 * 1000; // 10 seconds later

        EventRecord event1 = new EventRecord("1", eventJson, insertedAt);
        List<EventRecord> partialEvents = List.of(event1);

        List<Match> matchList = rulesExecutor1.processEvents(eventJson).join(); // partial match
        assertThat(matchList).isEmpty();

        long persistedTime = insertedAt;

        // Create and persist session state
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setRulebookHash("abc123");
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setPartialEvents(partialEvents);
        sessionState.setPersistedTime(persistedTime);
        sessionState.setVersion(1);
        sessionState.setCreatedTime(createdTime);

        stateManager.persistSessionState(sessionState);

        //--------

        // Simulate that a node crashes
        rulesExecutor1 = null;
        stateManager = null;

        // Recovery----
        // This test simulates that the restarted node recovers the session, assuming that the leader is taken over by another node
        HAStateManager stateManager2 = HAStateManagerFactory.create();
        stateManager2.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG);
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK).toRulesSet(RuleFormat.JSON, ALL_CONDITION_RULE);

        SessionState retrievedSessionState = stateManager2.getSessionState(rulesSet.getName());
        RulesExecutor rulesExecutorRecovered = stateManager2.recoverSession(rulesSet, retrievedSessionState);

        rulesExecutorRecovered.advanceTime(10, java.util.concurrent.TimeUnit.SECONDS);

        String eventJson2 = "{\"j\":2}";

        matchList = rulesExecutorRecovered.processEvents(eventJson2).join();
        assertThat(matchList).hasSize(1);
    }

    public static final String ALL_CONDITION_WITH_FACT_RULE =
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
                                                    "Fact": "i"
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
    void testFactRecordsReplayedOnRecovery() {
        stateManager.enableLeader(LEADER_ID);

        // This test works without HARulesExecutor
        RulesExecutor rulesExecutor1 = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK), ALL_CONDITION_WITH_FACT_RULE);
        long createdTime = rulesExecutor1.asKieSession().getSessionClock().getCurrentTime();

        long insertedAt = createdTime + 1_000;
        String factJson = "{\"i\":1}";
        String factIdentifier = HAUtils.sha256(factJson);

        List<Match> matchList = rulesExecutor1.processFacts(factJson).join(); // partial match
        assertThat(matchList).isEmpty();

        EventRecord factRecord = new EventRecord(factIdentifier, factJson, insertedAt, EventRecord.RecordType.FACT);

        String rulebookHash = HAUtils.sha256(ALL_CONDITION_WITH_FACT_RULE);
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setRulebookHash(rulebookHash);
        sessionState.setPartialEvents(List.of(factRecord));
        sessionState.setCreatedTime(createdTime);
        sessionState.setPersistedTime(insertedAt);
        sessionState.setCurrentStateSHA(calculateStateSHA(rulebookHash, factIdentifier));
        sessionState.setPreviousStateSHA(rulebookHash);
        sessionState.setLastProcessedEventUuid(factIdentifier);
        sessionState.setVersion(1);

        stateManager.persistSessionState(sessionState);
        stateManager.shutdown();
        stateManager = null;

        HAStateManager stateManager2 = HAStateManagerFactory.create();
        stateManager2.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG);
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK)
                .toRulesSet(RuleFormat.JSON, ALL_CONDITION_WITH_FACT_RULE);

        SessionState retrievedState = stateManager2.getSessionState(rulesSet.getName());
        RulesExecutor recoveredExecutor = stateManager2.recoverSession(rulesSet, retrievedState);

        String recoveredFacts = recoveredExecutor.getAllFactsAsJson();
        assertThat(recoveredFacts).contains("\"i\":1");

        stateManager2.shutdown();
    }

    @Test
    void testSessionStateShaFieldsPersistAndLoad() {
        stateManager.enableLeader(LEADER_ID);

        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setRulebookHash("rulebook-sha-001");
        sessionState.setPartialEvents(List.of());

        long now = System.currentTimeMillis();
        sessionState.setCreatedTime(now);
        sessionState.setPersistedTime(now);

        sessionState.setCurrentStateSHA("sha-current-001");
        sessionState.setPreviousStateSHA("sha-prev-000");
        sessionState.setLastProcessedEventUuid("event-uuid-001");

        stateManager.persistSessionState(sessionState);

        stateManager.shutdown();

        stateManager = HAStateManagerFactory.create();
        stateManager.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG);

        SessionState retrieved = stateManager.getSessionState(RULE_SET_NAME);
        assertThat(retrieved.getCurrentStateSHA()).isEqualTo("sha-current-001");
        assertThat(retrieved.getPreviousStateSHA()).isEqualTo("sha-prev-000");
        assertThat(retrieved.getLastProcessedEventUuid()).isEqualTo("event-uuid-001");
        assertThat(retrieved.getRulebookHash()).isEqualTo("rulebook-sha-001");
    }

    @Test
    void testMultipleRuleSetsPersistIndependently() {
        stateManager.enableLeader(LEADER_ID);

        SessionState rulesetA = new SessionState();
        rulesetA.setHaUuid(HA_UUID);
        rulesetA.setRuleSetName("rulesetA");
        rulesetA.setRulebookHash("hashA");
        rulesetA.setCurrentStateSHA("shaA");
        rulesetA.setPreviousStateSHA("shaPrevA");
        rulesetA.setLastProcessedEventUuid("eventA");
        stateManager.persistSessionState(rulesetA);

        SessionState rulesetB = new SessionState();
        rulesetB.setHaUuid(HA_UUID);
        rulesetB.setRuleSetName("rulesetB");
        rulesetB.setRulebookHash("hashB");
        rulesetB.setCurrentStateSHA("shaB");
        rulesetB.setPreviousStateSHA("shaPrevB");
        rulesetB.setLastProcessedEventUuid("eventB");
        stateManager.persistSessionState(rulesetB);

        SessionState retrievedA = stateManager.getSessionState("rulesetA");
        SessionState retrievedB = stateManager.getSessionState("rulesetB");

        assertThat(retrievedA.getRuleSetName()).isEqualTo("rulesetA");
        assertThat(retrievedA.getCurrentStateSHA()).isEqualTo("shaA");
        assertThat(retrievedA.getLastProcessedEventUuid()).isEqualTo("eventA");

        assertThat(retrievedB.getRuleSetName()).isEqualTo("rulesetB");
        assertThat(retrievedB.getCurrentStateSHA()).isEqualTo("shaB");
        assertThat(retrievedB.getLastProcessedEventUuid()).isEqualTo("eventB");
    }
}

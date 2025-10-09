package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_HA_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_PG_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * Integration tests for AstRulesEngine with HA functionality
 */
class HAIntegrationTestMultiConditionFact extends HAIntegrationTestBase {

    // Multi condition rule
    private static final String RULE_SET_MULTI_CONDITION_FACT = """
                {
                    "name": "Multi Condition Fact Ruleset",
                    "rules": [
                        {"Rule": {
                            "name": "temperature_alert",
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
                            "action": {
                                "run_playbook": [
                                    {
                                        "name": "send_alert.yml",
                                        "extra_vars": {
                                            "message": "High temperature detected"
                                        }
                                    }
                                ]
                            }
                        }}
                    ]
                }
                """;

    @Override
    protected String getRuleSet() {
        return RULE_SET_MULTI_CONDITION_FACT;
    }

    @Test
    void testSessionRecoveryWithPartialMatchOnTheSameNode() {
        // Step 1: Node 1 becomes leader and processes first fact (partial match)
        rulesEngine1.enableLeader("node-1");

        // Process first fact that creates partial match
        String firstFact = "{\"i\":1}";
        String result1 = rulesEngine1.assertFact(sessionId1, firstFact);

        // Should be empty since rule requires both i=1 AND j=2
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

        // Advance time to simulate processing delay
        rulesEngine1.advanceTime(sessionId1, 5, "SECONDS");

        // Step 2: Simulate Node 1 crash/shutdown
        rulesEngine1.disableLeader("node-1");
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Step 3: Node 1 restarted. Not a leader.
        //         Leader is taken over by Node 2, but Node 2 is not the scope of this test
        AstRulesEngine rulesEngine1Restart = new AstRulesEngine();
        rulesEngine1Restart.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG); // The same cluster. Both nodes share same DB
        long sessionId1Restart = rulesEngine1Restart.createRuleset(getRuleSet());
        AsyncConsumer consumer1restart = new AsyncConsumer("consumer1-restart");
        consumer1restart.startConsuming(rulesEngine1Restart.port());

        // Step 4: Node 1 processes second event that should complete the match
        // The recovered session should have the partial match from the first fact
        String secondEvent = createEvent("{\"j\":2}");
        String result2 = rulesEngine1Restart.assertEvent(sessionId1Restart, secondEvent);

        // Should now have a complete match since both conditions are satisfied
        // (i=1 from recovered state + j=2 from current event)
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matches).hasSize(1);

        // TODO: HA response format for non-leader. meUUID
//        Map<String, Object> match = matches.get(0);
//        assertThat(match.get("name")).isEqualTo("temperature_alert");
//        assertThat(match).containsKey("matching_uuid");
    }

    @Test
    void testHaStatsIncrementOnFactAssertion() {
        rulesEngine1.enableLeader("node-1");

        Map<String, Object> statsAfterLeader = rulesEngine1.getHAStats();
        assertThat(statsAfterLeader.get("current_leader")).isEqualTo("node-1");
        assertThat(((Number) statsAfterLeader.get("events_processed_in_term")).intValue()).isZero();

        rulesEngine1.assertFact(sessionId1, "{\"i\":1}");

        Map<String, Object> statsAfterFact = rulesEngine1.getHAStats();
        assertThat(((Number) statsAfterFact.get("events_processed_in_term")).intValue()).isEqualTo(1);
    }
}

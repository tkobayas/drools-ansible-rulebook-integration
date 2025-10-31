package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * PostgreSQL version of HAIntegrationMultiConditionTest using Testcontainers
 */
class HAIntegrationMultiConditionPostgreSQLTest extends PostgreSQLTestBase {

    private static final String HA_UUID = "integration-ha-postgres-multi";

    // Multi condition rule
    private static final String RULE_SET_MULTI_CONDITION = """
                {
                    "name": "Multi Condition Ruleset",
                    "rules": [
                        {"Rule": {
                            "name": "temperature_alert",
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

    private AstRulesEngine rulesEngine1; // node 1
    private AstRulesEngine rulesEngine2; // node 2

    private long sessionId1; // node1
    private long sessionId2; // node2

    private HAIntegrationTestBase.AsyncConsumer consumer1; // node1
    private HAIntegrationTestBase.AsyncConsumer consumer2; // node2

    @BeforeEach
    void setUp() {
        rulesEngine1 = new AstRulesEngine();
        rulesEngine1.initializeHA(HA_UUID, getPostgresParams(), getPostgresHAConfig());
        sessionId1 = rulesEngine1.createRuleset(RULE_SET_MULTI_CONDITION, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());

        rulesEngine2 = new AstRulesEngine();
        rulesEngine2.initializeHA(HA_UUID, getPostgresParams(), getPostgresHAConfig());
        sessionId2 = rulesEngine2.createRuleset(RULE_SET_MULTI_CONDITION, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
        consumer2.startConsuming(rulesEngine2.port());
    }

    @AfterEach
    void tearDown() {
        if (consumer1 != null) {
            consumer1.stop();
        }
        if (consumer2 != null) {
            consumer2.stop();
        }

        if (rulesEngine1 != null) {
            rulesEngine1.dispose(sessionId1);
        }
        if (rulesEngine2 != null) {
            rulesEngine2.dispose(sessionId2);
        }

        dropPostgresTables();
    }

    @Test
    void testSessionRecoveryWithPartialMatch() {
        // Step 1: Node 1 becomes leader and processes first event (partial match)
        rulesEngine1.enableLeader("node-1");

        // Process first event that creates partial match
        String firstEvent = createEvent("{\"i\":1}");
        String result1 = rulesEngine1.assertEvent(sessionId1, firstEvent);

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

        // Step 3: Node 2 takes over and recovers session
        rulesEngine2.enableLeader("node-2");

        // Step 4: Node 2 processes second event that should complete the match
        // The recovered session should have the partial match from the first event
        String secondEvent = createEvent("{\"j\":2}");
        String result2 = rulesEngine2.assertEvent(sessionId2, secondEvent);

        // Should now have a complete match since both conditions are satisfied
        // (i=1 from recovered state + j=2 from current event)
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matches).hasSize(1);

        Map<String, Object> match = matches.get(0);
        assertThat(match).containsEntry("name", "temperature_alert")
                .containsKey("matching_uuid");

        System.out.println("PostgreSQL test passed: Session recovery with partial match works correctly");
    }
}

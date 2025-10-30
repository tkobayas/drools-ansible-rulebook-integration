package org.drools.ansible.rulebook.integration.ha.tests;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_HA_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_PG_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * Integration tests for AstRulesEngine with HA functionality
 */
class HAIntegrationEdgeCaseTest extends HAIntegrationTestBase {

    // Basic rule
    private static final String RULE_SET_BASIC = """
                {
                    "name": "Test Ruleset",
                    "sources": {"EventSource": "test"},
                    "rules": [
                        {"Rule": {
                            "name": "temperature_alert",
                            "condition": {
                                "GreaterThanExpression": {
                                    "lhs": {
                                        "Event": "temperature"
                                    },
                                    "rhs": {
                                        "Integer": 30
                                    }
                                }
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

    protected String getRuleSet() {
        return RULE_SET_BASIC;
    }

    @BeforeEach
    @Override
    void setUp() {
        rulesEngine1 = new AstRulesEngine();
        rulesEngine1.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG); // The same cluster. Both nodes share same DB

        consumer1 = new AsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());
    }

    @Test
    void testCurrentStateSha_createRulesetAfterBecomingLeader() {
        // Scenario: Become leader -> Create ruleset
        rulesEngine1.enableLeader("leader-1");
        sessionId1 = rulesEngine1.createRuleset(getRuleSet());

        String eventUuid1 = "11111111-2222-3333-4444-555555555555";
        String event1 = """
                {
                    "meta": {"uuid": "%s"},
                    "temperature": 35
                }
                """.formatted(eventUuid1);

        String result1 = rulesEngine1.assertEvent(sessionId1, event1); // matches the rule. the event is discarded
        assertThat(result1).contains("temperature_alert");

        HAStateManager haManagerForAssertion = createHAStateManagerForAssertion();
        SessionState state1 = haManagerForAssertion.getPersistedSessionState(getRuleSetNameValue());

        assertThat(state1).isNotNull();
        // SHA is calculated from complete state content
        assertThat(state1.getCurrentStateSHA()).isNotNull();

        // Verify integrity by recalculating SHA
        String recalculatedSha1 = HAUtils.calculateStateSHA(state1);
        assertThat(state1.getCurrentStateSHA()).isEqualTo(recalculatedSha1);

        String eventUuid2 = "XXXXXXXX-2222-3333-4444-555555555555";
        String event2 = """
                {
                    "meta": {"uuid": "%s"},
                    "no-match": 35
                }
                """.formatted(eventUuid2);

        String result2 = rulesEngine1.assertEvent(sessionId1, event2); // doesn't match the rule. the event is also discarded
        System.out.println("Result2: " + result2);

        SessionState state2 = haManagerForAssertion.getPersistedSessionState(getRuleSetNameValue());

        assertThat(state2).isNotNull();

        assertThat(state2.getCurrentStateSHA()).isNotNull();

        // Verify integrity by recalculating SHA
        String recalculatedSha2 = HAUtils.calculateStateSHA(state2);
        assertThat(state2.getCurrentStateSHA()).isEqualTo(recalculatedSha2);
    }

    // This is a little tricky scenario. Usually we expect a leader is taken over by another node.
    // But this HA implementation allows the same node to restart the engine process and become leader again.
    // Not a real requirement, but probably good to keep this capability.
    @Test
    void testSingleRestart() {
        sessionId1 = rulesEngine1.createRuleset(getRuleSet());
        rulesEngine1.enableLeader("engine-1");

        // Process an event
        String event = createEvent("""
                {
                    "temperature": 45,
                    "critical": true
                }
                """);
        String result = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(result).contains("temperature_alert");

        // Simulate engine-1 crash
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Simulate restarting engine-1 on the same node. The old instance is gone, so we create a new one
        AstRulesEngine rulesEngine1Restart = new AstRulesEngine();
        rulesEngine1Restart.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG);
        long sessionId1Restart = rulesEngine1Restart.createRuleset(getRuleSet());
        AsyncConsumer consumer1restart = new AsyncConsumer("consumer1-restart");
        consumer1restart.startConsuming(rulesEngine1Restart.port());

        rulesEngine1Restart.enableLeader("engine-1");

        // Process another event
        String event2 = createEvent("""
                {
                    "temperature": 50,
                    "critical": true
                }
                """);
        String result2 = rulesEngine1Restart.assertEvent(sessionId1Restart, event2);
        assertThat(result2).contains("temperature_alert");

        consumer1restart.stop();
    }
}

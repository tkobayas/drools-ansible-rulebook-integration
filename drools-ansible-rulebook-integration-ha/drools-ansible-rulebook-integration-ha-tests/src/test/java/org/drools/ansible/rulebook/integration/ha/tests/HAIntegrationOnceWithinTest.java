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
class HAIntegrationOnceWithinTest extends HAIntegrationTestBase {

    // OnceWithin rule - fires only once within 10 seconds for the same host
    private static final String RULE_SET_ONCE_WITHIN = """
                {
                    "name": "OnceWithin Ruleset",
                    "rules": [
                        {"Rule": {
                            "name": "alert_throttle",
                            "condition": {
                                "AllCondition": [
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "alert.type"
                                            },
                                            "rhs": {
                                                "String": "warning"
                                            }
                                        }
                                    }
                                ],
                                "throttle": {
                                    "group_by_attributes": [
                                        "event.alert.host"
                                    ],
                                    "once_within": "10 seconds"
                                }
                            },
                            "action": {
                                "run_playbook": [
                                    {
                                        "name": "alert_handler.yml",
                                        "extra_vars": {
                                            "message": "Alert received"
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
        return RULE_SET_ONCE_WITHIN;
    }

    @Test
    void testSessionRecoveryWithinTimeWindow() {
        // Step 1: Node 1 becomes leader and processes first event
        rulesEngine1.enableLeader("node-1");

        // Process first event that matches the rule (t=0)
        String firstEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        String result1 = rulesEngine1.assertEvent(sessionId1, firstEvent);

        // Should match since it's the first event for h1
        List<Map<String, Object>> matches1 = readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matches1).hasSize(1);
        assertThat(matches1.get(0))
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");

        // Advance time by 3 seconds (t=3)
        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");

        // Process second event for same host within time window
        String secondEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        String result2 = rulesEngine1.assertEvent(sessionId1, secondEvent);

        // Should NOT match because still within 10-second window
        assertThat(readValueAsListOfMapOfStringAndObject(result2)).isEmpty();

        // Advance time by 2 more seconds (t=5, still within window)
        // Note: this advance is not persisted in SessionState
        rulesEngine1.advanceTime(sessionId1, 2, "SECONDS");

        // Step 2: Simulate Node 1 crash/shutdown
        rulesEngine1.disableLeader("node-1");
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Step 3: Node 1 restarted. Not a leader.
        //         Leader is taken over by Node 2, but Node 2 is not the scope of this test
        AstRulesEngine rulesEngine1Restart = new AstRulesEngine();
        rulesEngine1Restart.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG);
        long sessionId1Restart = rulesEngine1Restart.createRuleset(getRuleSet());
        AsyncConsumer consumer1restart = new AsyncConsumer("consumer1-restart");
        consumer1restart.startConsuming(rulesEngine1Restart.port());

        // Need to advance time to catch up with the simulated "current time" (3 seconds was already advanced during recovery, but the 2 seconds was not persisted)
        // This is just a hack for test. In real scenario, the new session should be fine with System.currentTimeMillis() in RulesExecutorSession.initClock()
        rulesEngine1Restart.advanceTime(sessionId1Restart, 2, "SECONDS");

        // Step 4: Process third event for same host (t=5, still within window from t=0)
        // The recovered session should maintain the once_within control event
        String thirdEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        String result3 = rulesEngine1Restart.assertEvent(sessionId1Restart, thirdEvent);

        // Should still NOT match because still within 10-second window
        assertThat(readValueAsListOfMapOfStringAndObject(result3)).isEmpty();

        // Advance time by 6 more seconds (t=11, outside window)
        rulesEngine1Restart.advanceTime(sessionId1Restart, 6, "SECONDS");

        // Step 5: Process fourth event for same host (t=11, outside window)
        String fourthEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        String result4 = rulesEngine1Restart.assertEvent(sessionId1Restart, fourthEvent);

        // Should match again since we're outside the 10-second window
        List<Map<String, Object>> matches4 = readValueAsListOfMapOfStringAndObject(result4);
        assertThat(matches4).hasSize(1);
        assertThat(matches4.get(0))
                .containsEntry("name", "alert_throttle")
                .containsEntry("matching_uuid", "");

        // Step 6: Test different host - should match immediately regardless of window
        String differentHostEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h2\"}}");
        String result5 = rulesEngine1Restart.assertEvent(sessionId1Restart, differentHostEvent);

        List<Map<String, Object>> matches5 = readValueAsListOfMapOfStringAndObject(result5);
        assertThat(matches5).hasSize(1);
        assertThat(matches5.get(0))
                .containsEntry("name", "alert_throttle")
                .containsEntry("matching_uuid", "");

        // Clean up
        rulesEngine1Restart.close();
        consumer1restart.stop();
    }
}

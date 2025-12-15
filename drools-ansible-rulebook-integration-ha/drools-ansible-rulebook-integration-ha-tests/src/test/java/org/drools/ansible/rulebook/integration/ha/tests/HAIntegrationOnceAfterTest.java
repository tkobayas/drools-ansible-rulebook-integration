package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

class HAIntegrationOnceAfterTest extends HAIntegrationTestBase {

    private static final String RULE_SET_ONCE_AFTER = """
                {
                    "name": "OnceAfter Ruleset",
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
                                    "once_after": "10 seconds"
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
        return RULE_SET_ONCE_AFTER;
    }

    @Test
    void testOnceAfterSurvivesRecoveryAndFiresAfterWindow() {
        rulesEngine1.enableLeader();

        String firstEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, firstEvent))).isEmpty();

        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");

        String secondEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, secondEvent))).isEmpty();

        // Total elapsed time on leader: 5 seconds
        rulesEngine1.advanceTime(sessionId1, 2, "SECONDS");
        rulesEngine2.advanceTime(sessionId2, 5, "SECONDS"); // keep follower clock aligned

        // Simulate failover
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        rulesEngine2.enableLeader();

        // Advance time past the 10-second window to trigger once_after
        String recoveryAdvanceResult = rulesEngine2.advanceTime(sessionId2, 6, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(recoveryAdvanceResult);

        assertThat(matches).hasSize(1);
        Map<String, Object> match = matches.get(0);
        assertThat(match)
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");
        Map<String, Object> events = (Map<String, Object>) match.get("events");
        assertThat(events).hasSize(1);
        assertThat(events.keySet()).containsExactlyInAnyOrder("m");

        assertThat(events)
                .extracting("m")
                .extracting("meta")
                .extracting("rule_engine")
                .satisfies(ruleEngineMeta -> assertThat((Map<String, Integer>) ruleEngineMeta)
                        .containsEntry("events_in_window", 2));
    }
}

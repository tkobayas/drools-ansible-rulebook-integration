package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SessionStatsTest {

    @Test
    void testWithDisabledRule() {
        String json =
                """
                {
                  "rules": [
                    {"Rule": {
                      "name": "R1",
                      "condition": "sensu.data.i == 1",
                      "action": {
                        "assert_fact": {
                          "ruleset": "Test rules4",
                          "fact": {
                            "j": 1
                          }
                        }
                      }
                    }},
                    {"Rule": {
                      "name": "R2",
                      "condition": "sensu.data.i == 2",
                      "action": {
                        "run_playbook": [
                          {
                            "name": "hello_playbook.yml"
                          }
                        ]
                      }
                    }},
                    {"Rule": {
                      "name": "R3",
                      "condition": "sensu.data.i == 3",
                      "action": {
                        "retract_fact": {
                          "ruleset": "Test rules4",
                          "fact": {
                            "j": 3
                          }
                        }
                      }
                    }},
                    {"Rule": {
                      "name": "R4",
                      "enabled": false,
                      "condition": "j == 1",
                      "action": {
                        "post_event": {
                          "ruleset": "Test rules4",
                          "fact": {
                            "j": 4
                          }
                        }
                      }
                    }}
                  ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(json);

        SessionStats beforeFiringStats = rulesExecutor.getSessionStats();
        assertNull( beforeFiringStats.getLastRuleFiredAt() );
        assertNull( beforeFiringStats.getLastEventReceivedAt() );

        rulesExecutor.processEvents( "{ \"sensu\": { \"data\": { \"i\":42 } } }" ).join();
        SessionStats statsAfterNonMatchingEvent = rulesExecutor.getSessionStats();
        assertNull( statsAfterNonMatchingEvent.getLastRuleFiredAt() );
        assertNotNull( statsAfterNonMatchingEvent.getLastEventReceivedAt() );

        List<Match> matchedRules = rulesExecutor.processFacts( "{ \"sensu\": { \"data\": { \"i\":1 } } }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "R1", matchedRules.get(0).getRule().getName() );

        matchedRules = rulesExecutor.processFacts( "{ \"j\":1 }" ).join();
        assertEquals( 0, matchedRules.size() );

        SessionStats stats = rulesExecutor.getSessionStats();
        assertEquals( 3, stats.getNumberOfRules() );
        assertEquals( 1, stats.getNumberOfDisabledRules() );
        assertEquals( 1, stats.getRulesTriggered() );
        assertEquals( 1, stats.getPermanentStorageCount() );
        assertTrue( stats.getPermanentStorageSize() > 10 );
        assertTrue( stats.getPermanentStorageSize() < 1000 );
        assertEquals( "R1", stats.getLastRuleFired() );
        assertNull( stats.getEnd() );

        SessionStats disposeStats = rulesExecutor.dispose();
        assertNotNull( disposeStats.getEnd() );
    }

    @Test
    void baseLevelMemory() {
        String rule =
                """
                {
                    "rules": [
                            {
                                "Rule": {
                                    "name": "R1",
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
                                            }
                                        ]
                                    }
                                }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(rule);

        SessionStats beforeFiringStats = rulesExecutor.getSessionStats();

        long baseLevelMemory = beforeFiringStats.getBaseLevelMemory();
        assertNotEquals(-1, baseLevelMemory);

        rulesExecutor.processEvents("{ \"i\": 1 }").join();
        SessionStats statsAfterProcessEvent = rulesExecutor.getSessionStats();

        assertEquals(baseLevelMemory, statsAfterProcessEvent.getBaseLevelMemory());
    }

    @Test
    void peakMemory() {
        String rule =
                """
                {
                    "rules": [
                            {
                                "Rule": {
                                    "name": "R1",
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
                                            }
                                        ]
                                    }
                                }
                            }
                        ]
                }
                """;

        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(rule);
        SessionStats beforeFiringStats = rulesExecutor.getSessionStats();
        assertThat(beforeFiringStats.getPeakMemory()).isGreaterThan(0L);

        for (int i = 0; i < 100; i++) {
            rulesExecutor.processEvents("{ \"i\": 1 }").join();
        }
        SessionStats statsAfterProcessEvent = rulesExecutor.getSessionStats();
        assertThat(statsAfterProcessEvent.getPeakMemory()).isGreaterThan(0L);
    }
}

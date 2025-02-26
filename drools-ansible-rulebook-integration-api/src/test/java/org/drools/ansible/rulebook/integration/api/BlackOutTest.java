package org.drools.ansible.rulebook.integration.api;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.junit.Test;
import org.kie.api.runtime.rule.Match;

import static org.assertj.core.api.Assertions.assertThat;

public class BlackOutTest {

    private static final String COMMON_JSON_TEMPLATE =
            """
            {
               "rules":[
                  {
                     "Rule":{
                        "name": "r1",
                        "condition":{
                           "EqualsExpression":{
                              "lhs":{
                                 "sensu":"data.i"
                              },
                              "rhs":{
                                 "Integer":1
                              }
                           }
                        },
                        "actions": [
                           {
                              "Action": {
                                 "action": "debug",
                                 "action_args": {}
                              }
                           }
                        ],
                        "black_out": %s
                     }
                  }
               ]
            }
            """;

    @Test
    public void annual() throws ExecutionException, InterruptedException {
        String annualBlackOut =
                """
                {
                   "trigger": "all",
                   "timezone": "local",
                   "start_time": {
                      "minute": 0,
                      "hour": 14,
                      "day_of_month": 1,
                      "month": 7
                   },
                   "end_time": {
                      "minute": 0,
                      "hour": 16,
                      "day_of_month": 1,
                      "month": 7
                   }
                }
                """;
        String json = String.format(COMMON_JSON_TEMPLATE, annualBlackOut);
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        ZonedDateTime testDateTime = LocalDateTime.of(2025, 7, 1, 15, 0).atZone(ZoneId.systemDefault()); // in the blackout period
        advanceTimeTo(testDateTime, rulesExecutor);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(matchedRules).isEmpty();

        CompletableFuture<List<Match>> matches = rulesExecutor.advanceTime(2, TimeUnit.HOURS); // after blackout
        assertThat(matches.get()).hasSize(1);
    }

    private static void advanceTimeTo(ZonedDateTime testDateTime, RulesExecutor rulesExecutor) {
        ZonedDateTime startDateTime = ZonedDateTime.now();
        long secondsToAdd = Duration.between(startDateTime, testDateTime).toSeconds();
        rulesExecutor.advanceTime(secondsToAdd, TimeUnit.SECONDS);
    }

    @Test
    public void annualAcrossYearEnd() throws ExecutionException, InterruptedException {
        String annualBlackOut =
                """
                {
                   "trigger": "all",
                   "timezone": "local",
                   "start_time": {
                      "hour": 0,
                      "day_of_month": 23,
                      "month": 12
                   },
                   "end_time": {
                      "hour": 0,
                      "day_of_month": 2,
                      "month": 1
                   }
                }
                """;
        String json = String.format(COMMON_JSON_TEMPLATE, annualBlackOut);
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        ZonedDateTime testDateTime = LocalDateTime.of(2025, 12, 23, 15, 0).atZone(ZoneId.systemDefault()); // in the blackout period
        advanceTimeTo(testDateTime, rulesExecutor);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(matchedRules).isEmpty();

        CompletableFuture<List<Match>> matches = rulesExecutor.advanceTime(10, TimeUnit.DAYS); // after blackout
        assertThat(matches.get()).hasSize(1);
    }

    @Test
    public void monthly() throws ExecutionException, InterruptedException {
        String monthlyBlackOut =
                """
                {
                   "trigger": "all",
                   "timezone": "local",
                   "start_time": {
                      "minute": 0,
                      "hour": 11,
                      "day_of_month": 1
                   },
                   "end_time": {
                      "minute": 0,
                      "hour": 13,
                      "day_of_month": 1
                   }
                }
                """;
        String json = String.format(COMMON_JSON_TEMPLATE, monthlyBlackOut);
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        ZonedDateTime testDateTime = LocalDateTime.of(2025, 7, 1, 12, 0).atZone(ZoneId.systemDefault()); // in the blackout period
        advanceTimeTo(testDateTime, rulesExecutor);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(matchedRules).isEmpty();

        CompletableFuture<List<Match>> matches = rulesExecutor.advanceTime(2, TimeUnit.HOURS); // after blackout
        assertThat(matches.get()).hasSize(1);
    }

    @Test
    public void weekly() throws ExecutionException, InterruptedException {
        String weeklyBlackOut =
                """
                {
                   "trigger": "all",
                   "timezone": "local",
                   "start_time": {
                      "hour": 3,
                      "day_of_week": 5
                   },
                   "end_time": {
                      "hour": 4,
                      "day_of_week": 5
                   }
                }
                """;
        String json = String.format(COMMON_JSON_TEMPLATE, weeklyBlackOut);
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        ZonedDateTime testDateTime = LocalDateTime.of(2025, 2, 21, 3, 30).atZone(ZoneId.systemDefault()); // in the blackout period
        advanceTimeTo(testDateTime, rulesExecutor);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(matchedRules).isEmpty();

        CompletableFuture<List<Match>> matches = rulesExecutor.advanceTime(2, TimeUnit.HOURS); // after blackout
        assertThat(matches.get()).hasSize(1);
    }

    @Test
    public void daily() throws ExecutionException, InterruptedException {
        String dailyBlackOut =
                """
                {
                   "trigger": "all",
                   "timezone": "local",
                   "start_time": {
                      "minute": 30,
                      "hour": 14
                   },
                   "end_time": {
                      "minute": 15,
                      "hour": 16
                   }
                }
                """;
        String json = String.format(COMMON_JSON_TEMPLATE, dailyBlackOut);
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        ZonedDateTime testDateTime = LocalDateTime.of(2025, 7, 1, 15, 0).atZone(ZoneId.systemDefault()); // in the blackout period
        advanceTimeTo(testDateTime, rulesExecutor);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(matchedRules).isEmpty();

        CompletableFuture<List<Match>> matches = rulesExecutor.advanceTime(2, TimeUnit.HOURS); // after blackout
        assertThat(matches.get()).hasSize(1);
    }

    @Test
    public void dailyUtc() throws ExecutionException, InterruptedException {
        String dailyBlackOut =
                """
                {
                   "trigger": "all",
                   "timezone": "utc",
                   "start_time": {
                      "minute": 30,
                      "hour": 14
                   },
                   "end_time": {
                      "minute": 15,
                      "hour": 16
                   }
                }
                """;
        String json = String.format(COMMON_JSON_TEMPLATE, dailyBlackOut);
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        ZonedDateTime testDateTime = LocalDateTime.of(2025, 7, 1, 15, 0).atZone(ZoneOffset.UTC); // in the blackout period
        advanceTimeTo(testDateTime, rulesExecutor);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(matchedRules).isEmpty();

        CompletableFuture<List<Match>> matches = rulesExecutor.advanceTime(2, TimeUnit.HOURS); // after blackout
        assertThat(matches.get()).hasSize(1);
    }

    @Test
    public void dailyUtcWithDifferentTimezoneSystem() throws ExecutionException, InterruptedException {
        String dailyBlackOut =
                """
                {
                   "trigger": "all",
                   "timezone": "utc",
                   "start_time": {
                      "minute": 30,
                      "hour": 14
                   },
                   "end_time": {
                      "minute": 15,
                      "hour": 16
                   }
                }
                """;
        String json = String.format(COMMON_JSON_TEMPLATE, dailyBlackOut);
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // Equivalent to 15:00:00 UTC
        ZonedDateTime testDateTime = LocalDateTime.of(2025, 7, 1, 11, 0).atZone(ZoneId.of("America/New_York"));
        advanceTimeTo(testDateTime, rulesExecutor);

        List<Match> matchedRules = rulesExecutor.processEvents("{ \"sensu\": { \"data\": { \"i\":1 } } }").join();
        assertThat(matchedRules).isEmpty();

        CompletableFuture<List<Match>> matches = rulesExecutor.advanceTime(2, TimeUnit.HOURS); // after blackout
        assertThat(matches.get()).hasSize(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidTimeSpec() {
        // day_of_month and day_of_week are mutually exclusive
        String invalidTimeSpec =
                """
                {
                   "trigger": "all",
                   "timezone": "local",
                   "start_time": {
                      "minute": 0,
                      "hour": 14,
                      "day_of_month": 1,
                      "day_of_week": 1,
                      "month": 7
                   },
                   "end_time": {
                      "minute": 0,
                      "hour": 16,
                      "day_of_month": 1,
                      "day_of_week": 1,
                      "month": 7
                   }
                }
                """;
        String json = String.format(COMMON_JSON_TEMPLATE, invalidTimeSpec);
        RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);
    }

    @Test
    public void triggerAll() throws ExecutionException, InterruptedException {
        List<Map<String, Map>> matchList = trigger("all");

        assertThat(matchList).hasSize(2);

        // group 1 : i, j, k
        assertMatch((Map<String, Map<String, Map<String, Integer>>>) matchList.get(0).get("r1"),
                    1, 1, 2, 3);

        // group 2 : i, j, k
        assertMatch((Map<String, Map<String, Map<String, Integer>>>) matchList.get(1).get("r1"),
                    2, 1, 2, 3);
    }

    private void assertMatch(Map<String, Map<String, Map<String, Integer>>> match,
                             int expectedGroup,
                             int expectedI,
                             int expectedJ,
                             int expectedK) {
        assertThat(match.get("m_0").get("data"))
                .contains(Map.entry("group", expectedGroup), Map.entry("i", expectedI));
        assertThat(match.get("m_1").get("data"))
                .contains(Map.entry("group", expectedGroup), Map.entry("j", expectedJ));
        assertThat(match.get("m_2").get("data"))
                .contains(Map.entry("group", expectedGroup), Map.entry("k", expectedK));
    }

    @Test
    public void triggerFirst() throws ExecutionException, InterruptedException {
        List<Map<String, Map>> matchList = trigger("first");

        assertThat(matchList).hasSize(1);

        // group 1 : i, j, k
        assertMatch((Map<String, Map<String, Map<String, Integer>>>) matchList.get(0).get("r1"),
                    1, 1, 2, 3);
    }

    @Test
    public void triggerLast() throws ExecutionException, InterruptedException {
        List<Map<String, Map>> matchList = trigger("last");

        assertThat(matchList).hasSize(1);

        // group 2 : i, j, k
        assertMatch((Map<String, Map<String, Map<String, Integer>>>) matchList.get(0).get("r1"),
                    2, 1, 2, 3);
    }

    private List<Map<String, Map>> trigger(String trigger) throws ExecutionException, InterruptedException {
        String jsonTemplate =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "name": "r1",
                            "condition":{
                                "AllCondition": [
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "data.i"
                                            },
                                            "rhs": {
                                                "Integer": 1
                                            }
                                        }
                                    },
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "data.j"
                                            },
                                            "rhs": {
                                                "Integer": 2
                                            }
                                        }
                                    },
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "data.k"
                                            },
                                            "rhs": {
                                                "Integer": 3
                                            }
                                        }
                                    }
                                ]
                            },
                            "actions": [
                               {
                                  "Action": {
                                     "action": "debug",
                                     "action_args": {}
                                  }
                               }
                            ],
                            "black_out": {
                               "trigger": "%s",
                               "timezone": "local",
                               "start_time": {
                                  "minute": 30,
                                  "hour": 14
                               },
                               "end_time": {
                                  "minute": 15,
                                  "hour": 16
                               }
                            }
                         }
                      }
                   ],
                   "default_events_ttl": "10 hours"
                }
                """;
        String json = String.format(jsonTemplate, trigger);
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        ZonedDateTime testDateTime = LocalDateTime.of(2025, 7, 1, 14, 0).atZone(ZoneId.systemDefault()); // before blackout
        advanceTimeTo(testDateTime, rulesExecutor);

        // group 1 : i is inserted before blackout. j and k are inserted during blackout
        List<Match> matchedRules = rulesExecutor.processEvents("{ \"data\": { \"group\": 1, \"i\": 1 } }").join();
        assertThat(matchedRules).isEmpty();

        rulesExecutor.advanceTime(45, TimeUnit.MINUTES); // in the blackout period

        matchedRules = rulesExecutor.processEvents("{ \"data\": { \"group\": 1, \"j\": 2 } }").join();
        assertThat(matchedRules).isEmpty();
        matchedRules = rulesExecutor.processEvents("{ \"data\": { \"group\": 1, \"k\": 3 } }").join();
        assertThat(matchedRules).isEmpty();

        // group 2 : i, j, and k are inserted during blackout
        matchedRules = rulesExecutor.processEvents("{ \"data\": { \"group\": 2, \"i\": 1 } }").join();
        assertThat(matchedRules).isEmpty();
        matchedRules = rulesExecutor.processEvents("{ \"data\": { \"group\": 2, \"j\": 2 } }").join();
        assertThat(matchedRules).isEmpty();
        matchedRules = rulesExecutor.processEvents("{ \"data\": { \"group\": 2, \"k\": 3 } }").join();
        assertThat(matchedRules).isEmpty();

        matchedRules = rulesExecutor.processEvents("{ \"data\": { \"group\": 3, \"i\": 1 } }").join();
        assertThat(matchedRules).isEmpty();

        CompletableFuture<List<Match>> matches = rulesExecutor.advanceTime(2, TimeUnit.HOURS); // after blackout
        List<Map<String, Map>> matchListAfterBlackOut = RuleMatch.asList(matches.get());

        matchedRules = rulesExecutor.processEvents("{ \"data\": { \"group\": 3, \"j\": 2 } }").join();
        assertThat(matchedRules).isEmpty();
        matchedRules = rulesExecutor.processEvents("{ \"data\": { \"group\": 3, \"k\": 3 } }").join();
        List<Map<String, Map>> matchListForGroup3 = RuleMatch.asList(matchedRules);
        assertThat(matchListForGroup3).hasSize(1);

        return matchListAfterBlackOut;
    }

    // test multiple rules with different blackouts. Check if blackOuts/blackOutEndTimes/blackOutMatches are correctly managed
    @Test
    public void multipleRules() throws ExecutionException, InterruptedException {
        String jsonTemplate =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "name": "r1",
                            "condition":{
                                "EqualsExpression": {
                                    "lhs": {
                                        "Event": "data.i"
                                    },
                                    "rhs": {
                                        "Integer": 1
                                    }
                                }
                            },
                            "actions": [
                               {
                                  "Action": {
                                     "action": "debug",
                                     "action_args": {}
                                  }
                               }
                            ],
                            "black_out": {
                               "trigger": "all",
                               "timezone": "local",
                               "start_time": {
                                  "minute": 30,
                                  "hour": 14
                               },
                               "end_time": {
                                  "minute": 15,
                                  "hour": 16
                               }
                            }
                         }
                      },
                      {
                         "Rule":{
                            "name": "r2",
                            "condition":{
                                "EqualsExpression": {
                                    "lhs": {
                                        "Event": "data.i"
                                    },
                                    "rhs": {
                                        "Integer": 2
                                    }
                                }
                            },
                            "actions": [
                               {
                                  "Action": {
                                     "action": "debug",
                                     "action_args": {}
                                  }
                               }
                            ],
                            "black_out": {
                               "trigger": "all",
                               "timezone": "local",
                               "start_time": {
                                  "minute": 30,
                                  "hour": 15
                               },
                               "end_time": {
                                  "minute": 30,
                                  "hour": 17
                               }
                            }
                         }
                      }
                   ],
                   "default_events_ttl": "10 hours"
                }
                """;
        String json = String.format(jsonTemplate);
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        System.out.println("Advance time to 15:00");
        ZonedDateTime testDateTime = LocalDateTime.of(2025, 7, 1, 15, 0).atZone(ZoneId.systemDefault()); // in the first blackout, before the second blackout
        advanceTimeTo(testDateTime, rulesExecutor);

        List<Match> matches = rulesExecutor.processEvents("{ \"data\": { \"i\": 1 } }").join();
        assertThat(matches).isEmpty();

        matches = rulesExecutor.processEvents("{ \"data\": { \"i\": 2 } }").join();
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).getRule().getName()).isEqualTo("r2");

        System.out.println("Advance time to 16:00");
        matches = rulesExecutor.advanceTime(1, TimeUnit.HOURS).get(); // 16:00 in the both blackouts
        assertThat(matches).isEmpty();

        matches = rulesExecutor.processEvents("{ \"data\": { \"i\": 2 } }").join(); // insert again
        assertThat(matches).isEmpty();

        System.out.println("Advance time to 17:00");
        matches = rulesExecutor.advanceTime(1, TimeUnit.HOURS).get(); // 17:00 after the first blackout, in the second blackout
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).getRule().getName()).isEqualTo("r1");

        System.out.println("Advance time to 18:00");
        matches = rulesExecutor.advanceTime(1, TimeUnit.HOURS).get(); // 18:00 after the second blackout
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).getRule().getName()).isEqualTo("r2");
    }

    @Test
    public void inheritBlackOut() {
        String jsonTemplate =
                """
                {
                   "rules":[
                      {
                         "Rule":{
                            "name": "r1",
                            "condition":{
                                "EqualsExpression": {
                                    "lhs": {
                                        "Event": "data.i"
                                    },
                                    "rhs": {
                                        "Integer": 1
                                    }
                                }
                            },
                            "actions": [
                               {
                                  "Action": {
                                     "action": "debug",
                                     "action_args": {}
                                  }
                               }
                            ]
                         }
                      },
                      {
                         "Rule":{
                            "name": "r2",
                            "condition":{
                                "EqualsExpression": {
                                    "lhs": {
                                        "Event": "data.i"
                                    },
                                    "rhs": {
                                        "Integer": 2
                                    }
                                }
                            },
                            "black_out": {
                               "trigger": "first",
                               "timezone": "local",
                               "start_time": {
                                  "hour": 3,
                                  "day_of_week": 4
                               },
                               "end_time": {
                                  "hour": 5,
                                  "day_of_week": 4
                               }
                            },
                            "actions": [
                               {
                                  "Action": {
                                     "action": "debug",
                                     "action_args": {}
                                  }
                               }
                            ]
                         }
                      }
                   ],
                   "default_events_ttl": "10 hours",
                   "black_out": {
                      "trigger": "all",
                      "timezone": "local",
                      "start_time": {
                         "minute": 30,
                         "hour": 14
                      },
                      "end_time": {
                         "minute": 15,
                         "hour": 16
                      }
                   }
                }
                """;
        String json = String.format(jsonTemplate);
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), json);

        // r1 inherits the blackOut from the ruleset: daily 14:30-16:15
        // r2 has its own blackOut: weekly Thursday 03:00-05:00

        // 2025-07-02 15:00:00 is Wednesday, in the first blackout, before the second blackout
        ZonedDateTime testDateTime = LocalDateTime.of(2025, 7, 2, 15, 0).atZone(ZoneId.systemDefault());
        advanceTimeTo(testDateTime, rulesExecutor);

        List<Match> matches = rulesExecutor.processEvents("{ \"data\": { \"i\": 1 } }").join();
        assertThat(matches).isEmpty();

        matches = rulesExecutor.processEvents("{ \"data\": { \"i\": 2 } }").join();
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).getRule().getName()).isEqualTo("r2");

        System.out.println("Advance time to 17:00");
        matches = rulesExecutor.advanceTime(2, TimeUnit.HOURS).join();
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).getRule().getName()).isEqualTo("r1");

        System.out.println("Advance time to Thursday 4:00");
        matches = rulesExecutor.advanceTime(11, TimeUnit.HOURS).join();
        assertThat(matches).isEmpty();

        matches = rulesExecutor.processEvents("{ \"data\": { \"i\": 2 } }").join();
        assertThat(matches).isEmpty();

        System.out.println("Advance time to Thursday 6:00");
        matches = rulesExecutor.advanceTime(2, TimeUnit.HOURS).join();
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).getRule().getName()).isEqualTo("r2");


    }

    // TODO: test for weekday 0/7
}

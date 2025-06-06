package org.drools.ansible.rulebook.integration.api;

import java.util.LinkedHashMap;
import java.util.List;

import org.drools.ansible.rulebook.integration.api.domain.Rule;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.domain.actions.MapAction;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ActionTest {

    @Test
    void singleAction() {
        // Generated by rules_with_assignment.yml in drools_jpy
        String json =
                """
                {
                  "hosts":[
                    "localhost"
                  ],
                  "name":"Demo rules with assignment",
                  "rules":[
                    {
                      "Rule":{
                        "action":{
                          "Action":{
                            "action":"debug",
                            "action_args":{
                              "events_event":"{{events.first}}"
                            }
                          }
                        },
                        "condition":{
                          "AllCondition":[
                            {
                              "AssignmentExpression":{
                                "lhs":{
                                  "Events":"first"
                                },
                                "rhs":{
                                  "EqualsExpression":{
                                    "lhs":{
                                      "Event":"i"
                                    },
                                    "rhs":{
                                      "Integer":67
                                    }
                                  }
                                }
                              }
                            }
                          ]
                        },
                        "enabled":true,
                        "name":"assignment"
                      }
                    }
                  ]
                }
                """;

        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, json);
        Rule rule = rulesSet.getRules().get(0).getRule();

        // assert getAction
        MapAction action = (MapAction) rule.getAction();
        assertThat(action).isNotNull();
        assertThat(action.get("Action")).isNotNull();
        LinkedHashMap valueMap = (LinkedHashMap)action.get("Action");
        assertThat(valueMap.get("action")).isEqualTo("debug");
        assertThat(((LinkedHashMap)valueMap.get("action_args")).get("events_event")).isEqualTo("{{events.first}}");

        // assert getActions same as getAction
        List<MapAction> actions = (List) rule.getActions();
        assertThat(actions).hasSize(1);
        MapAction firstAction = actions.get(0);
        assertThat(firstAction.get("Action")).isNotNull();
        LinkedHashMap firstActionValueMap = (LinkedHashMap)firstAction.get("Action");
        assertThat(firstActionValueMap.get("action")).isEqualTo("debug");
        assertThat(((LinkedHashMap)firstActionValueMap.get("action_args")).get("events_event")).isEqualTo("{{events.first}}");
    }

    @Test
    void singleActionInActions() {
        // Generated by retract_matching_facts.yml in drools_jpy
        String json =
                """
                {
                  "hosts":[
                    "all"
                  ],
                  "name":"example",
                  "rules":[
                    {
                      "Rule":{
                        "actions":[
                          {
                            "Action":{
                              "action":"print_event",
                              "action_args":{
                                "pretty":true
                              }
                            }
                          }
                        ],
                        "condition":{
                          "AllCondition":[
                            {
                              "GreaterThanExpression":{
                                "lhs":{
                                  "Event":"i"
                                },
                                "rhs":{
                                  "Integer":2
                                }
                              }
                            },
                            {
                              "GreaterThanExpression":{
                                "lhs":{
                                  "Event":"x"
                                },
                                "rhs":{
                                  "Integer":34
                                }
                              }
                            }
                          ]
                        },
                        "enabled":true,
                        "name":"r1"
                      }
                    }
                  ]
                }
                """;

        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, json);
        Rule rule = rulesSet.getRules().get(0).getRule();

        // assert getAction
        MapAction action = (MapAction) rule.getAction();
        assertThat(action).isNotNull();
        assertThat(action.get("Action")).isNotNull();
        LinkedHashMap valueMap = (LinkedHashMap)action.get("Action");
        assertThat(valueMap.get("action")).isEqualTo("print_event");
        assertThat(((LinkedHashMap)valueMap.get("action_args")).get("pretty")).isEqualTo(true);

        // assert getActions same as getAction
        List<MapAction> actions = (List) rule.getActions();
        assertThat(actions).hasSize(1);
        MapAction firstAction = actions.get(0);
        assertThat(firstAction.get("Action")).isNotNull();
        LinkedHashMap firstActionValueMap = (LinkedHashMap)firstAction.get("Action");
        assertThat(firstActionValueMap.get("action")).isEqualTo("print_event");
        assertThat(((LinkedHashMap)firstActionValueMap.get("action_args")).get("pretty")).isEqualTo(true);
    }

    @Test
    void multipleActions() {
        // Generated by 59_multiple_actions.yml in ansible-rulebook
        String json =
                """
                {
                  "name":"59 Multiple Actions",
                  "hosts":[
                    "all"
                  ],
                  "rules":[
                    {
                      "Rule":{
                        "name":"r1",
                        "condition":{
                          "AllCondition":[
                            {
                              "EqualsExpression":{
                                "lhs":{
                                  "Event":"i"
                                },
                                "rhs":{
                                  "Integer":1
                                }
                              }
                            }
                          ]
                        },
                        "actions":[
                          {
                            "Action":{
                              "action":"debug",
                              "action_args":{
                                "pretty":false
                              }
                            }
                          },
                          {
                            "Action":{
                              "action":"print_event",
                              "action_args":{
                                "pretty":true
                              }
                            }
                          },
                          {
                            "Action":{
                              "action":"debug",
                              "action_args":{
                                "msg":"Multiple Action Message1"
                              }
                            }
                          }
                        ],
                        "enabled":true
                      }
                    }
                  ]
                }
                """;

        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, json);
        Rule rule = rulesSet.getRules().get(0).getRule();

        // assert getAction, but only the first action
        MapAction action = (MapAction) rule.getAction();
        assertThat(action).isNotNull();
        assertThat(action.get("Action")).isNotNull();
        LinkedHashMap valueMap = (LinkedHashMap) action.get("Action");
        assertThat(valueMap.get("action")).isEqualTo("debug");
        assertThat(((LinkedHashMap) valueMap.get("action_args")).get("pretty")).isEqualTo(false);

        // assert getActions
        List<MapAction> actions = (List) rule.getActions();
        assertThat(actions).hasSize(3);
        MapAction firstAction = actions.get(0);
        assertThat(firstAction.get("Action")).isNotNull();
        LinkedHashMap firstActionValueMap = (LinkedHashMap) firstAction.get("Action");
        assertThat(firstActionValueMap.get("action")).isEqualTo("debug");
        assertThat(((LinkedHashMap) firstActionValueMap.get("action_args")).get("pretty")).isEqualTo(false);

        MapAction secondAction = actions.get(1);
        assertThat(secondAction.get("Action")).isNotNull();
        LinkedHashMap secondActionValueMap = (LinkedHashMap) secondAction.get("Action");
        assertThat(secondActionValueMap.get("action")).isEqualTo("print_event");
        assertThat(((LinkedHashMap) secondActionValueMap.get("action_args")).get("pretty")).isEqualTo(true);

        MapAction thirdAction = actions.get(2);
        assertThat(thirdAction.get("Action")).isNotNull();
        LinkedHashMap thirdActionValueMap = (LinkedHashMap) thirdAction.get("Action");
        assertThat(thirdActionValueMap.get("action")).isEqualTo("debug");
        assertThat(((LinkedHashMap) thirdActionValueMap.get("action_args")).get("msg")).isEqualTo("Multiple Action Message1");
    }
}

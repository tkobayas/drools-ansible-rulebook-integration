package org.drools.ansible.rulebook.integration.api;

import org.drools.base.reteoo.InitialFactImpl;
import org.junit.jupiter.api.Test;
import org.kie.api.prototype.PrototypeFactInstance;
import org.kie.api.runtime.rule.Match;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcessEventTest {

    public static final String JSON1 =
            """
            {
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
                                        }
                                    ]
                                },
                                "enabled": true,
                                "name": null
                            }
                        },
                        {
                            "Rule": {
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "IsNotDefinedExpression": {
                                                "Event": "msg"
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
    void testExecuteRules() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\": 1 }" ).join();
        assertEquals( 1, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );

        rulesExecutor.dispose();
    }

    public static final String JSON2 =
            """
            {
                "rules": [
                        {
                            "Rule": {
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Fact": "os"
                                                },
                                                "rhs": {
                                                    "String": "linux"
                                                }
                                            }
                                        },
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
                                },
                                "enabled": true,
                                "name": null
                            }
                        }
                    ]
            }
            """;

    @Test
    void testEventShouldProduceMultipleMatchesForSameRule() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON2);

        rulesExecutor.processFacts( "{ \"host\": \"A\", \"os\": \"linux\" }" );
        rulesExecutor.processFacts( "{ \"host\": \"B\", \"os\": \"windows\" }" );
        rulesExecutor.processFacts( "{ \"host\": \"C\", \"os\": \"linux\" }" );

        List<Match> matchedRules = rulesExecutor.processEvents( "{ \"i\": 1 }" ).join();
        assertEquals( 2, matchedRules.size() );
        assertEquals( "r_0", matchedRules.get(0).getRule().getName() );
        assertEquals( "r_0", matchedRules.get(1).getRule().getName() );

        List<String> hosts = matchedRules.stream()
                .flatMap( m -> m.getObjects().stream() )
                .map( PrototypeFactInstance.class::cast)
                .filter( p -> p.has("host") )
                .map( p -> p.get("host") )
                .map( String.class::cast)
                .collect(Collectors.toList());

        assertEquals( 2, hosts.size() );
        assertTrue( hosts.containsAll(Arrays.asList("A", "C") ));

        rulesExecutor.dispose();
    }

    public static final String JSON_IS_NOT_DEFINED =
            """
            {
                "rules": [
                        {
                            "Rule": {
                                "name": "r1",
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "IsNotDefinedExpression": {
                                                "Event": "beta.xheaders.age"
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
                                "enabled": true
                            }
                        }
                    ]
            }
            """;

    @Test
    void isNotDefinedExpression() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_IS_NOT_DEFINED);

        // A rule containing only a "isNotDefined" constraint matches only the first time ...
        List<Match> matchedRules = rulesExecutor.processEvents("{\"meta\":{\"headers\":{\"token\":123}}}").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r1", matchedRules.get(0).getRule().getName());
        assertEquals(InitialFactImpl.class, matchedRules.get(0).getObjects().get(0).getClass());

        // ... and then no more, to avoid overwhelming the system with firings for each and every event not having that field
        matchedRules = rulesExecutor.processEvents("{\"beta\":{\"headers\":{\"age\":23}}}").join();
        assertEquals(0, matchedRules.size());

        // ... unless an event having that field is inserted
        matchedRules = rulesExecutor.processEvents("{\"beta\":{\"xheaders\":{\"age\":23}}}").join();
        assertEquals(0, matchedRules.size());

        // ... and then that event is explicitly removed from the system or expires
        matchedRules = rulesExecutor.processRetractMatchingFacts("{\"beta\":{\"xheaders\":{\"age\":23}}}", false).join();
        assertEquals(1, matchedRules.size());
        assertEquals("r1", matchedRules.get(0).getRule().getName());

        rulesExecutor.dispose();
    }

    public static final String JSON_IS_DEFINED_AND_IS_NOT_DEFINED =
            """
            {
                "rules": [
                    {
                        "Rule": {
                            "name": "r1",
                            "condition": {
                                "AllCondition": [
                                    {
                                        "AndExpression": {
                                            "lhs": {
                                                "IsDefinedExpression": {
                                                    "Event": "meta"
                                                }
                                            },
                                            "rhs": {
                                                "IsNotDefinedExpression": {
                                                    "Event": "meta.headers.token"
                                                }
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
                            "enabled": true
                        }
                    }
                ]
            }
            """;

    @Test
    void isDefinedAndIsNotDefinedExpression() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_IS_DEFINED_AND_IS_NOT_DEFINED);

        List<Match> matchedRules = rulesExecutor.processEvents("{\"meta\":{\"headers\":{\"token\":123}}}").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{\"meta\":{\"headers\":{\"age\":23}}}").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r1", matchedRules.get(0).getRule().getName());

        rulesExecutor.dispose();
    }

    public static final String JSON_IS_DEFINED_IS_NOT_DEFINED_IN_ALL =
            """
            {
                "rules": [
                    {
                        "Rule": {
                            "name": "r1",
                            "condition": {
                                "AllCondition": [
                                    {
                                        "IsDefinedExpression": {
                                            "Event": "meta"
                                        }
                                    },
                                    {
                                        "IsNotDefinedExpression": {
                                            "Event": "meta.headers.token"
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
                            "enabled": true
                        }
                    }
                ]
            }
            """;

    @Test
    void isDefinedIsNotDefinedInAllExpression() {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_IS_DEFINED_IS_NOT_DEFINED_IN_ALL);

        List<Match> matchedRules = rulesExecutor.processEvents("{\"meta\":{\"headers\":{\"token\":123}}}").join();
        assertEquals(0, matchedRules.size());

        matchedRules = rulesExecutor.processEvents("{\"meta\":{\"headers\":{\"age\":23}}}").join();
        assertEquals(1, matchedRules.size());
        assertEquals("r1", matchedRules.get(0).getRule().getName());

        rulesExecutor.dispose();
    }}

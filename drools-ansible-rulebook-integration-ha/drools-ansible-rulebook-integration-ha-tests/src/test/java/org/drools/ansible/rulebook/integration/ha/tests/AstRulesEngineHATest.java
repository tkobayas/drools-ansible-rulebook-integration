package org.drools.ansible.rulebook.integration.ha.tests;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for AstRulesEngine with HA functionality.
 * <p>
 * This test class focuses on simple method calls. For complex scenarios, use AstRulesEngineHAIntegrationTest.
 */
class AstRulesEngineHATest {

    private static final String RULE_SET = """
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

    @Test
    void testHAOperationsWithoutConfiguration() {
        try (AstRulesEngine rulesEngine = new AstRulesEngine()) {
            rulesEngine.createRuleset(RULE_SET);
            assertThrows(IllegalStateException.class, () -> {
                rulesEngine.enableLeader("leader-1");
            });
        }
    }
}
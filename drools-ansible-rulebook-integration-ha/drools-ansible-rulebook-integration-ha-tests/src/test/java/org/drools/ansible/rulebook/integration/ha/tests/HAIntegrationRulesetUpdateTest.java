package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * Tests that HA correctly handles ruleset updates by detecting rulebook hash mismatch
 * and creating a fresh session instead of recovering stale state.
 */
class HAIntegrationRulesetUpdateTest extends AbstractHATestBase {

    private static final String HA_UUID = "ruleset-update-ha-1";

    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_test", "HA ruleset update tests");
        } else {
            initializeH2();
        }
    }

    // V1: triggers on temperature > 30
    private static final String RULE_SET_V1 = """
            {
                "name": "Test Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "temperature_alert",
                        "condition": {
                            "GreaterThanExpression": {
                                "lhs": {"Event": "temperature"},
                                "rhs": {"Integer": 30}
                            }
                        },
                        "action": {
                            "run_playbook": [{"name": "alert_v1.yml"}]
                        }
                    }}
                ]
            }
            """;

    // V2: same name, but triggers on humidity > 70
    private static final String RULE_SET_V2 = """
            {
                "name": "Test Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "humidity_alert",
                        "condition": {
                            "GreaterThanExpression": {
                                "lhs": {"Event": "humidity"},
                                "rhs": {"Integer": 70}
                            }
                        },
                        "action": {
                            "run_playbook": [{"name": "alert_v2.yml"}]
                        }
                    }}
                ]
            }
            """;

    private AstRulesEngine engine1;
    private AstRulesEngine engine2;
    private long sessionId1;
    private long sessionId2;
    private HAIntegrationTestBase.AsyncConsumer consumer1;
    private HAIntegrationTestBase.AsyncConsumer consumer2;

    @AfterEach
    void tearDown() {
        if (consumer1 != null) {
            consumer1.stop();
        }
        if (consumer2 != null) {
            consumer2.stop();
        }
        if (engine1 != null) {
            engine1.close();
        }
        if (engine2 != null) {
            engine2.close();
        }
        cleanupDatabase();
    }

    /**
     * Scenario: Leader node updates its ruleset.
     *
     * 1. Node1 starts with V1 rules, becomes leader, processes events
     * 2. Node1 disposes session, creates new session with V2 rules
     * 3. V2 should NOT recover V1's persisted state (hash mismatch)
     * 4. V2 rules should work correctly (humidity events match, temperature events don't)
     */
    @Test
    void testUpdateRulesetOnSameNode() {
        // Phase 1: Start with V1 ruleset
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId1 = engine1.createRuleset(RULE_SET_V1, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Process a temperature event that matches V1
        String tempEvent = createEvent("{\"temperature\": 35}");
        String result1 = engine1.assertEvent(sessionId1, tempEvent);
        assertThat(result1).contains("temperature_alert");

        // Verify state is persisted
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState stateV1 = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateV1).isNotNull();
            assertThat(stateV1.getRulebookHash()).isNotNull();
            String v1Hash = stateV1.getRulebookHash();

            // Phase 2: Dispose and recreate with V2 ruleset (same name)
            engine1.dispose(sessionId1);
            sessionId1 = engine1.createRuleset(RULE_SET_V2, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

            // Verify the new session state has a different hash
            SessionState stateV2 = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateV2).isNotNull();
            assertThat(stateV2.getRulebookHash()).isNotEqualTo(v1Hash);

            // Phase 3: Verify V2 rules work correctly
            // Humidity event should match V2
            String humidityEvent = createEvent("{\"humidity\": 80}");
            String result2 = engine1.assertEvent(sessionId1, humidityEvent);
            assertThat(result2).contains("humidity_alert");

            // Temperature event should NOT match V2
            String tempEvent2 = createEvent("{\"temperature\": 35}");
            String result3 = engine1.assertEvent(sessionId1, tempEvent2);
            List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result3);
            assertThat(matchList).isEmpty();
        } finally {
            assertionManager.shutdown();
        }
    }

    /**
     * Scenario: Node2 has updated ruleset and takes over as leader after failover.
     *
     * 1. Node1 starts with V1 rules, becomes leader, processes events
     * 2. Node2 starts with V2 rules (same name, different conditions)
     * 3. Node1 fails, Node2 becomes leader
     * 4. Node2 should detect hash mismatch and skip recovery of V1 state
     * 5. Node2's V2 rules should work correctly
     */
    @Test
    void testUpdateRulesetOnNode2AndFailover() {
        // Phase 1: Node1 with V1 ruleset as leader
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId1 = engine1.createRuleset(RULE_SET_V1, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Process events with V1
        String tempEvent = createEvent("{\"temperature\": 35}");
        String result1 = engine1.assertEvent(sessionId1, tempEvent);
        assertThat(result1).contains("temperature_alert");

        // Phase 2: Node2 starts with V2 ruleset (same name "Test Ruleset")
        engine2 = new AstRulesEngine();
        engine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, dbHAConfigJson);
        sessionId2 = engine2.createRuleset(RULE_SET_V2, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
        consumer2.startConsuming(engine2.port());

        // Phase 3: Failover - Node1 goes down, Node2 becomes leader
        engine1.disableLeader();
        engine2.enableLeader();

        // Phase 4: Verify Node2 uses V2 rules (not recovered V1 state)
        // Humidity event should match V2
        String humidityEvent = createEvent("{\"humidity\": 80}");
        String result2 = engine2.assertEvent(sessionId2, humidityEvent);
        assertThat(result2).contains("humidity_alert");

        // Temperature event should NOT match V2
        String tempEvent2 = createEvent("{\"temperature\": 35}");
        String result3 = engine2.assertEvent(sessionId2, tempEvent2);
        List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result3);
        assertThat(matchList).isEmpty();

        // Verify fresh state is persisted with V2 hash
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState state = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(state).isNotNull();
            // The persisted state should have the V2 rulebook hash, not V1
            String v1Hash = org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256(RULE_SET_V1);
            String v2Hash = org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256(RULE_SET_V2);
            assertThat(state.getRulebookHash()).isNotEqualTo(v1Hash);
            assertThat(state.getRulebookHash()).isEqualTo(v2Hash);
        } finally {
            assertionManager.shutdown();
        }
    }

    private HAStateManager createHAStateManagerForAssertion() {
        HAStateManager manager = HAStateManagerFactory.create();
        manager.initializeHA(HA_UUID, "FOR_ASSERTION", dbParams, dbHAConfig);
        return manager;
    }
}

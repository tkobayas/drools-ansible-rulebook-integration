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
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
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

    // Config JSON with overwrite_if_rulebook_changes disabled (keep old persisted data despite mismatch)
    private static final String dbHAConfigJsonWithNoOverwrite = toJson(Map.of("write_after", 1, "overwrite_if_rulebook_changes", false));

    /**
     * Scenario: Leader node updates its ruleset with overwrite_if_rulebook_changes=false.
     *
     * 1. Node1 starts with V1 rules, becomes leader, processes events
     * 2. Node1 disposes session, creates new session with V2 rules (overwrite_if_rulebook_changes=false in config)
     * 3. V2 should recover from persisted state despite hash mismatch (because overwrite_if_rulebook_changes=false)
     * 4. The persisted session state should NOT be deleted
     */
    @Test
    void testUpdateRulesetWithNoOverwrite() {
        // Phase 1: Start with V1 ruleset (using no-overwrite config from the start)
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJsonWithNoOverwrite);
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
            String v1Hash = stateV1.getRulebookHash();

            // Phase 2: Dispose and recreate with V2 ruleset (overwrite_if_rulebook_changes=false in config)
            engine1.dispose(sessionId1);
            sessionId1 = engine1.createRuleset(RULE_SET_V2, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

            // Verify the session state was NOT deleted (overwrite_if_rulebook_changes=false skips deletion)
            SessionState stateAfterOverwrite = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateAfterOverwrite).isNotNull();
            // The hash should still be V1's hash because overwrite_if_rulebook_changes=false skipped the delete+fresh-persist
            assertThat(stateAfterOverwrite.getRulebookHash()).isEqualTo(v1Hash);
        } finally {
            assertionManager.shutdown();
        }
    }

    /**
     * Scenario: Node2 has updated ruleset with overwrite_if_rulebook_changes=false and takes over as leader.
     *
     * 1. Node1 starts with V1 rules, becomes leader, processes events
     * 2. Node2 starts with V2 rules (overwrite_if_rulebook_changes=false in config)
     * 3. Node1 fails, Node2 becomes leader
     * 4. Node2 should recover from V1's persisted state despite hash mismatch (because overwrite_if_rulebook_changes=false)
     * 5. The persisted session state should NOT be deleted
     */
    @Test
    void testUpdateRulesetOnNode2WithNoOverwriteAndFailover() {
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

        // Phase 2: Node2 starts with V2 ruleset (overwrite_if_rulebook_changes=false in config)
        engine2 = new AstRulesEngine();
        engine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, dbHAConfigJsonWithNoOverwrite);
        sessionId2 = engine2.createRuleset(RULE_SET_V2, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
        consumer2.startConsuming(engine2.port());

        // Phase 3: Failover - Node1 goes down, Node2 becomes leader
        engine1.disableLeader();
        engine2.enableLeader();

        // Phase 4: Verify persisted state was NOT deleted
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState state = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(state).isNotNull();
            // The hash should still be V1's hash because overwrite_if_rulebook_changes=false skipped the delete
            String v1Hash = org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256(RULE_SET_V1);
            assertThat(state.getRulebookHash()).isEqualTo(v1Hash);
        } finally {
            assertionManager.shutdown();
        }
    }

    /**
     * Scenario: Single server restarts with updated ruleset (overwrite_if_rulebook_changes=false).
     *
     * 1. Node1 starts with V1 rules, becomes leader, processes events
     * 2. Node1 shuts down (engine.close())
     * 3. Node1 restarts with V2 rules (overwrite_if_rulebook_changes=false in config), becomes leader
     * 4. Should recover from V1's persisted state despite hash mismatch (because overwrite_if_rulebook_changes=false)
     * 5. The persisted session state should NOT be deleted
     */
    @Test
    void testUpdateRulesetWithNoOverwriteAndRestart() {
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

        // Record V1 hash before shutdown
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        String v1Hash;
        try {
            SessionState stateV1 = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateV1).isNotNull();
            v1Hash = stateV1.getRulebookHash();
        } finally {
            assertionManager.shutdown();
        }

        // Phase 2: Shut down engine1 completely (simulates server restart)
        consumer1.stop();
        consumer1 = null;
        engine1.close();
        engine1 = null;

        // Phase 3: Restart with V2 ruleset (overwrite_if_rulebook_changes=false in config)
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJsonWithNoOverwrite);
        sessionId1 = engine1.createRuleset(RULE_SET_V2, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1-restarted");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Phase 4: Verify persisted state was NOT deleted (overwrite_if_rulebook_changes=false skips deletion)
        assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState state = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(state).isNotNull();
            // The hash should still be V1's hash because overwrite_if_rulebook_changes=false skipped the delete
            assertThat(state.getRulebookHash()).isEqualTo(v1Hash);
        } finally {
            assertionManager.shutdown();
        }
    }

    /**
     * Scenario: Single server restarts with updated ruleset (overwrite=true, default).
     *
     * 1. Node1 starts with V1 rules, becomes leader, processes events
     * 2. Node1 shuts down (engine.close())
     * 3. Node1 restarts with V2 rules (overwrite=true, default), becomes leader
     * 4. Should detect hash mismatch and delete old session state, starting fresh
     * 5. V2 rules should work correctly
     */
    @Test
    void testUpdateRulesetWithoutOverwriteAndRestart() {
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

        // Phase 2: Shut down engine1 completely (simulates server restart)
        consumer1.stop();
        consumer1 = null;
        engine1.close();
        engine1 = null;

        // Phase 3: Restart with V2 ruleset (no overwrite - default)
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId1 = engine1.createRuleset(RULE_SET_V2, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1-restarted");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Phase 4: Verify old state was deleted and fresh state with V2 hash is persisted
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState state = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(state).isNotNull();
            String v1Hash = org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256(RULE_SET_V1);
            String v2Hash = org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256(RULE_SET_V2);
            assertThat(state.getRulebookHash()).isNotEqualTo(v1Hash);
            assertThat(state.getRulebookHash()).isEqualTo(v2Hash);
        } finally {
            assertionManager.shutdown();
        }

        // Phase 5: Verify V2 rules work correctly
        // Humidity event should match V2
        String humidityEvent = createEvent("{\"humidity\": 80}");
        String result2 = engine1.assertEvent(sessionId1, humidityEvent);
        assertThat(result2).contains("humidity_alert");

        // Temperature event should NOT match V2
        String tempEvent2 = createEvent("{\"temperature\": 35}");
        String result3 = engine1.assertEvent(sessionId1, tempEvent2);
        List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result3);
        assertThat(matchList).isEmpty();
    }

    // V1_MULTI: requires BOTH temperature > 30 AND humidity > 50 to fire.
    // Sending only a temperature event creates a partial match (event stays in working memory).
    private static final String RULE_SET_V1_MULTI = """
            {
                "name": "Test Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "temp_and_humidity_alert",
                        "condition": {
                            "AllCondition": [
                                {
                                    "GreaterThanExpression": {
                                        "lhs": {"Event": "temperature"},
                                        "rhs": {"Integer": 30}
                                    }
                                },
                                {
                                    "GreaterThanExpression": {
                                        "lhs": {"Event": "humidity"},
                                        "rhs": {"Integer": 50}
                                    }
                                }
                            ]
                        },
                        "action": {
                            "run_playbook": [{"name": "alert_multi.yml"}]
                        }
                    }}
                ]
            }
            """;

    // V_PRESSURE: completely unrelated domain - triggers on pressure > 100
    private static final String RULE_SET_V_PRESSURE = """
            {
                "name": "Test Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "pressure_alert",
                        "condition": {
                            "GreaterThanExpression": {
                                "lhs": {"Event": "pressure"},
                                "rhs": {"Integer": 100}
                            }
                        },
                        "action": {
                            "run_playbook": [{"name": "alert_pressure.yml"}]
                        }
                    }}
                ]
            }
            """;

    /**
     * Verifies that stale partial events from an old ruleset are cleared after recovery
     * when overwrite_if_rulebook_changes=false.
     *
     * 1. Node starts with V1_MULTI rules (AllCondition: temperature > 30 AND humidity > 50).
     *    Sends only a temperature event → partial match, event persisted in partialEvents.
     * 2. Node shuts down. Persisted state contains the stale temperature partial event.
     * 3. Node restarts with V_PRESSURE rules (pressure > 100) — completely different domain.
     *    Because overwrite_if_rulebook_changes=false, the old state is recovered.
     *    During recovery, Drools replays the temperature event but discards it (no matching rule).
     *    After recovery, the refreshed state is persisted — the stale partial event is gone.
     */
    @Test
    void testStalePartialEventsClearedAfterRecoveryWithNoOverwrite() {
        // Phase 1: Start with V1_MULTI ruleset (requires both temperature AND humidity)
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJsonWithNoOverwrite);
        sessionId1 = engine1.createRuleset(RULE_SET_V1_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Send only a temperature event — this creates a partial match (waiting for humidity)
        String tempEvent = createEvent("{\"temperature\": 35}");
        String result1 = engine1.assertEvent(sessionId1, tempEvent);
        // No match yet — rule needs both temperature AND humidity
        List<Map<String, Object>> matchList1 = JsonMapper.readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matchList1).isEmpty();

        // Verify partial event is persisted
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState stateV1 = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateV1).isNotNull();
            assertThat(stateV1.getPartialEvents())
                    .as("V1 should have a partial event (temperature waiting for humidity)")
                    .isNotEmpty();
        } finally {
            assertionManager.shutdown();
        }

        // Phase 2: Shut down and restart with completely different ruleset (pressure rules)
        consumer1.stop();
        consumer1 = null;
        engine1.close();
        engine1 = null;

        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJsonWithNoOverwrite);
        sessionId1 = engine1.createRuleset(RULE_SET_V_PRESSURE, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1-pressure");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Phase 3: Verify the stale temperature partial event has been cleared after recovery
        assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState stateAfterRestart = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateAfterRestart).isNotNull();

            boolean hasTemperatureEvent = stateAfterRestart.getPartialEvents().stream()
                    .anyMatch(e -> e.getEventJson().contains("temperature"));
            assertThat(hasTemperatureEvent)
                    .as("Stale temperature partial event from V1_MULTI should be cleared " +
                        "after recovery persists the refreshed state")
                    .isFalse();
        } finally {
            assertionManager.shutdown();
        }
    }

    private HAStateManager createHAStateManagerForAssertion() {
        HAStateManager manager = HAStateManagerFactory.create(TEST_DB_TYPE);
        manager.initializeHA(HA_UUID, "FOR_ASSERTION", dbParams, dbHAConfig);
        return manager;
    }
}

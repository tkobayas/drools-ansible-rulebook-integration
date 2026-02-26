package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.crypto.KeyGenerator;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * Full AstRulesEngine integration tests with encryption enabled.
 * Tests encrypted session persist, recovery, and failover.
 */
class HAIntegrationEncryptionTest extends AbstractHATestBase {

    private static final String HA_UUID = "enc-integration-ha-1";

    private static final String ENCRYPTION_KEY;
    private static final String ENCRYPTION_KEY_ROTATED;

    static {
        ENCRYPTION_KEY = generateBase64Key();
        ENCRYPTION_KEY_ROTATED = generateBase64Key();

        if (USE_POSTGRES) {
            initializePostgres("eda_ha_enc_test", "HA encryption integration tests");
        } else {
            initializeH2();
        }
    }

    private static String generateBase64Key() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256);
            return Base64.getEncoder().encodeToString(keyGen.generateKey().getEncoded());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Rule set for tests
    private static final String RULE_SET = """
            {
                "name": "Encryption Test Ruleset",
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
                            "run_playbook": [{"name": "alert.yml"}]
                        }
                    }}
                ]
            }
            """;

    private AstRulesEngine rulesEngine1;
    private AstRulesEngine rulesEngine2;
    private long sessionId1;
    private long sessionId2;
    private HAIntegrationTestBase.AsyncConsumer consumer1;
    private HAIntegrationTestBase.AsyncConsumer consumer2;

    private String encryptedConfigJson;

    @BeforeEach
    void setUp() {
        // Build config with encryption keys
        Map<String, Object> encConfig = new HashMap<>(dbHAConfig);
        encConfig.put("encryption_key_primary", ENCRYPTION_KEY);
        encryptedConfigJson = toJson(encConfig);

        rulesEngine1 = new AstRulesEngine();
        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());
        rulesEngine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, encryptedConfigJson);
        sessionId1 = rulesEngine1.createRuleset(RULE_SET, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        rulesEngine2 = new AstRulesEngine();
        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
        consumer2.startConsuming(rulesEngine2.port());
        rulesEngine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, encryptedConfigJson);
        sessionId2 = rulesEngine2.createRuleset(RULE_SET, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
    }

    @AfterEach
    void tearDown() {
        if (consumer1 != null) consumer1.stop();
        if (consumer2 != null) consumer2.stop();
        if (rulesEngine1 != null) {
            rulesEngine1.dispose(sessionId1);
            rulesEngine1.close();
        }
        if (rulesEngine2 != null) {
            rulesEngine2.dispose(sessionId2);
            rulesEngine2.close();
        }
        cleanupDatabase();
    }

    @Test
    void encryptedSessionPersistAndRecovery() {
        // Node 1 as leader — trigger a match
        rulesEngine1.enableLeader();

        String event = createEvent("{\"temperature\": 45}");
        String result = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(result).isNotNull();

        List<Map<String, Object>> matchList = readValueAsListOfMapOfStringAndObject(result);
        assertThat(matchList).hasSize(1);

        String meUuid = TestUtils.extractMatchingUuidFromResponse(result);
        assertThat(meUuid).isNotEmpty();

        // Verify raw DB columns are encrypted
        String rawEventData = TestUtils.queryRawColumn(dbParams,
                "SELECT event_data FROM drools_ansible_matching_event WHERE ha_uuid = ?", HA_UUID);
        assertThat(rawEventData).startsWith("$ENCRYPTED$");
        assertThat(rawEventData).doesNotContain("temperature");

        String rawPartialEvents = TestUtils.queryRawColumn(dbParams,
                "SELECT partial_matching_events FROM drools_ansible_session_state WHERE ha_uuid = ?", HA_UUID);
        assertThat(rawPartialEvents).startsWith("$ENCRYPTED$");

        // Verify matching event is persisted and readable with encryption
        HAStateManager assertionManager = createEncryptedStateManager();
        try {
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            assertThat(pendingEvents).isNotEmpty();
            assertThat(pendingEvents.get(0).getMeUuid()).isEqualTo(meUuid);
            assertThat(pendingEvents.get(0).getEventData()).contains("temperature");
        } finally {
            assertionManager.shutdown();
        }
    }

    @Test
    void encryptedMatchingEventRecoveryViaAsyncChannel() {
        // Node 1 as leader
        rulesEngine1.enableLeader();

        String event = createEvent("{\"temperature\": 45}");
        String result = rulesEngine1.assertEvent(sessionId1, event);
        String meUuid = TestUtils.extractMatchingUuidFromResponse(result);

        // Verify raw DB column is encrypted
        String rawEventData = TestUtils.queryRawColumn(dbParams,
                "SELECT event_data FROM drools_ansible_matching_event WHERE ha_uuid = ?", HA_UUID);
        assertThat(rawEventData).startsWith("$ENCRYPTED$");
        assertThat(rawEventData).doesNotContain("temperature");

        // Failover: node1 stops leading, node2 becomes leader
        rulesEngine1.disableLeader();
        rulesEngine2.enableLeader();

        // Wait for async recovery message on node2
        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> !consumer2.getReceivedMessages().isEmpty());

        String asyncResult = consumer2.getReceivedMessages().get(0);
        Map<String, Object> asyncResultMap = readValueAsMapOfStringAndObject(asyncResult);
        assertThat(asyncResultMap).containsKey("result");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> resultList = (List<Map<String, Object>>) asyncResultMap.get("result");
        assertThat(resultList).isNotEmpty();
        assertThat(resultList.get(0)).containsEntry("matching_uuid", meUuid);
    }

    @Test
    void keyRotationWithFailover() {
        // Node 1 as leader with original key
        rulesEngine1.enableLeader();

        String event = createEvent("{\"temperature\": 45}");
        String result = rulesEngine1.assertEvent(sessionId1, event);
        String meUuid = TestUtils.extractMatchingUuidFromResponse(result);
        assertThat(meUuid).isNotEmpty();

        // Verify raw DB column is encrypted with original key
        String rawEventData = TestUtils.queryRawColumn(dbParams,
                "SELECT event_data FROM drools_ansible_matching_event WHERE ha_uuid = ?", HA_UUID);
        assertThat(rawEventData).startsWith("$ENCRYPTED$");
        assertThat(rawEventData).doesNotContain("temperature");

        // Simulate crash + key rotation: node1 goes down, node2 starts with rotated key (+ old key as secondary)
        rulesEngine1.disableLeader();
        rulesEngine1.dispose(sessionId1);
        rulesEngine1.close();
        rulesEngine1 = null;

        // Stop consumer2 and engine2, recreate with rotated keys
        consumer2.stop();
        rulesEngine2.dispose(sessionId2);
        rulesEngine2.close();

        Map<String, Object> rotatedConfig = new HashMap<>(dbHAConfig);
        rotatedConfig.put("encryption_key_primary", ENCRYPTION_KEY_ROTATED);
        rotatedConfig.put("encryption_key_secondary", ENCRYPTION_KEY);
        String rotatedConfigJson = toJson(rotatedConfig);

        rulesEngine2 = new AstRulesEngine();
        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2-rotated");
        consumer2.startConsuming(rulesEngine2.port());
        rulesEngine2.initializeHA(HA_UUID, "worker-2-rotated", dbParamsJson, rotatedConfigJson);
        sessionId2 = rulesEngine2.createRuleset(RULE_SET, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
        rulesEngine2.enableLeader();

        // Wait for async recovery with rotated key — should use secondary key fallback
        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> !consumer2.getReceivedMessages().isEmpty());

        String asyncResult = consumer2.getReceivedMessages().get(0);
        Map<String, Object> asyncResultMap = readValueAsMapOfStringAndObject(asyncResult);
        assertThat(asyncResultMap).containsKey("result");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> resultList = (List<Map<String, Object>>) asyncResultMap.get("result");
        assertThat(resultList).isNotEmpty();
        assertThat(resultList.get(0)).containsEntry("matching_uuid", meUuid);
    }

    private HAStateManager createEncryptedStateManager() {
        Map<String, Object> encConfig = new HashMap<>(dbHAConfig);
        encConfig.put("encryption_key_primary", ENCRYPTION_KEY);
        HAStateManager manager = org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory.create(TEST_DB_TYPE);
        manager.initializeHA(HA_UUID, "FOR_ASSERTION", dbParams, encConfig);
        return manager;
    }
}

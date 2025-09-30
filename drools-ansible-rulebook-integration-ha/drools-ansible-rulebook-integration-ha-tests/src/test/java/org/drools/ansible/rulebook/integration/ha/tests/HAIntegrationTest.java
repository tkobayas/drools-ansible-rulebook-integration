package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_HA_CONFIG;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.TEST_PG_CONFIG;

/**
 * Integration tests for AstRulesEngine with HA functionality
 */
class HAIntegrationTest extends HAIntegrationTestBase {

    // Basic rule
    private static final String RULE_SET_BASIC = """
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

    protected String getRuleSet() {
        return RULE_SET_BASIC;
    }

    @Test
    void testBasicBehaviorOtherThanHA() {
        // Enable leader mode
        rulesEngine1.enableLeader("engine-leader-1");

        // Process an event that triggers a rule
        String event = """
                {
                    "temperature": 35,
                    "timestamp": "2024-01-01T10:00:00Z"
                }
                """;

        String result = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(result).isNotNull();

        System.out.println("======");
        System.out.println("Result: " + result);
        System.out.println("======");

        // Parse result to verify ME UUID is included
        List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
        assertThat(matchList).hasSize(1);

        Map<String, Object> matchJson = matchList.get(0);
        assertThat(matchJson).containsKey("temperature_alert");

        Map<String, Object> ruleObject = (Map<String, Object>) matchJson.get("temperature_alert");
        assertThat(ruleObject).containsKey("m"); // Original match data
        assertThat(ruleObject).containsKey("meUuid"); // ME UUID should be included

        String meUuid = (String) ruleObject.get("meUuid");
        assertThat(meUuid).isNotEmpty();
        assertThat(meUuid).hasSize(36); // UUID format check

        // Get session stats to verify rule triggered
        String stats = rulesEngine1.sessionStats(sessionId1);
        assertThat(stats).isNotNull();
        Map<String, Object> statsMap = JsonMapper.readValueAsMapOfStringAndObject(stats);
        assertThat(statsMap.get("rulesTriggered")).isEqualTo(1);
    }

    @Test
    void testMatchingEventPersistence() {
        rulesEngine1.enableLeader("leader-1");

        // Process an event that triggers a rule
        String event = """
                {
                    "temperature": 35,
                    "sensor": "temp_01"
                }
                """;

        String result = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(result).isNotNull();
        List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
        String meUuid = (String) ((Map<String, Object>) matchList.get(0).get("temperature_alert")).get("meUuid");

        // Verify that matching events were persisted to database
        // We can check this by accessing another HAStateManager directly, so this is a relatively white-box test
        HAStateManager haStateManagerForAssertion = createHAStateManagerForAssertion();

        try {
            List<MatchingEvent> pendingEvents = haStateManagerForAssertion.getPendingMatchingEvents();
            assertThat(pendingEvents).isNotEmpty();

            MatchingEvent me = pendingEvents.get(0);
            assertThat(me.getRuleName()).isEqualTo("temperature_alert");
            assertThat(me.getMeUuid()).isEqualTo(meUuid);
        } finally {
            haStateManagerForAssertion.shutdown();
        }
    }

    @Test
    void testCurrentStateSha() {
        // Scenario: Create ruleset (done by @BeforeEach) -> Become leader
        rulesEngine1.enableLeader("leader-1");

        String eventUuid1 = "11111111-2222-3333-4444-555555555555";
        String event1 = """
                {
                    "meta": {"uuid": "%s"},
                    "temperature": 35
                }
                """.formatted(eventUuid1);

        String result1 = rulesEngine1.assertEvent(sessionId1, event1); // matches the rule. the event is discarded
        assertThat(result1).contains("temperature_alert");

        String baseSha = HAUtils.sha256(getRuleSet());
        String stateSha1 = HAUtils.calculateStateSHA(baseSha, eventUuid1);

        HAStateManager haManagerForAssertion = createHAStateManagerForAssertion();
        SessionState state1 = haManagerForAssertion.getSessionState();

        assertThat(state1).isNotNull();
        assertThat(state1.getLastProcessedEventUuid()).isEqualTo(eventUuid1);
        assertThat(state1.getPreviousStateSHA()).isEqualTo(baseSha);
        assertThat(state1.getCurrentStateSHA()).isEqualTo(stateSha1);

        String eventUuid2 = "XXXXXXXX-2222-3333-4444-555555555555";
        String event2 = """
                {
                    "meta": {"uuid": "%s"},
                    "no-match": 35
                }
                """.formatted(eventUuid2);

        String result2 = rulesEngine1.assertEvent(sessionId1, event2); // doesn't match the rule. the event is also discarded
        System.out.println("Result2: " + result2);

        String stateSha2 = HAUtils.calculateStateSHA(stateSha1, eventUuid2);

        SessionState state2 = haManagerForAssertion.getSessionState();

        assertThat(state2).isNotNull();
        assertThat(state2.getLastProcessedEventUuid()).isEqualTo(eventUuid2);
        assertThat(state2.getPreviousStateSHA()).isEqualTo(stateSha1);
        assertThat(state2.getCurrentStateSHA()).isEqualTo(stateSha2);
    }

    @Test
    void testActionStateManagement() {
        rulesEngine1.enableLeader("leader-1");

        // First trigger a rule to create a matching event
        String event = """
                {
                    "temperature": 35,
                    "location": "server_room"
                }
                """;

        String result = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(result).isNotNull();

        // Simulate that the Python side extracts the ME UUID from the result
        List<Map<String, Object>> resultMaps = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
        String meUuid = (String) ((Map<String, Object>) resultMaps.get(0).get("temperature_alert")).get("meUuid");
        assertThat(meUuid).isNotNull();

        // Set ActionInfo
        String actionData = "{\"name\":\"send_alert\",\"status\":\"running\",\"reference_id\":\"job-456\"}";
        rulesEngine1.addActionInfo(sessionId1, meUuid, 0, actionData);

        // Check action exists and get it
        assertThat(rulesEngine1.actionInfoExists(sessionId1, meUuid, 0)).isTrue();
        String retrieved = rulesEngine1.getActionInfo(sessionId1, meUuid, 0);
        assertThat(retrieved).isEqualTo(actionData);

        // Update ActionInfo
        String updatedActionData = "{\"name\":\"send_alert\",\"status\":\"success\",\"reference_id\":\"job-456\"}";
        rulesEngine1.updateActionInfo(sessionId1, meUuid, 0, updatedActionData);

        // Delete ActionInfo and MarchingEvent when complete
        rulesEngine1.deleteActionInfo(sessionId1, meUuid);

        // Should not exist after deletion
        assertThat(rulesEngine1.actionInfoExists(sessionId1, meUuid, 0)).isFalse();
    }



    @Test
    void testLeaderTransition() {
        // Set as leader
        rulesEngine1.enableLeader("leader-1");

        // Process some events
        String event = """
                {
                    "temperature": 35,
                    "zone": "production"
                }
                """;
        rulesEngine1.assertEvent(sessionId1, event);

        // Simulate leader failure - unset leader
        rulesEngine1.disableLeader("leader-1");

        // New leader takes over
        rulesEngine2.enableLeader("leader-2");

        // Verify we can still process events
        String event2 = """
                {
                    "temperature": 40,
                    "zone": "production"
                }
                """;
        String result2 = rulesEngine2.assertEvent(sessionId2, event2);
        assertThat(result2).isNotNull();

        // Check that both events created matching events
        HAStateManager haStateManagerForAssertion = createHAStateManagerForAssertion();

        try {
            List<MatchingEvent> pendingEvents = haStateManagerForAssertion.getPendingMatchingEvents();
            assertThat(pendingEvents).hasSize(2); // Both events should create MEs
        } finally {
            haStateManagerForAssertion.shutdown();
        }
    }

    @Test
    void testHAWithFailoverRecovery() {
        rulesEngine1.enableLeader("engine-1");

        // Process an event
        String event = """
                {
                    "temperature": 45,
                    "critical": true
                }
                """;
        String result = rulesEngine1.assertEvent(sessionId1, event);
        List<Map<String, Object>> resultMaps = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
        String meUuid = (String) ((Map<String, Object>) resultMaps.get(0).get("temperature_alert")).get("meUuid");
        assertThat(meUuid).isNotNull();

        // Simulate engine-1 failure
        rulesEngine1.disableLeader("engine-1");

        // enableLeader triggers sending pending matches to async channel
        rulesEngine2.enableLeader("engine-2");

        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> consumer2.getReceivedMessages().size() >= 1);

        String asyncResult = consumer2.getReceivedMessages().get(0);

        Map<String, Object> asyncResultMap = readValueAsMapOfStringAndObject(asyncResult);

        assertThat(asyncResultMap).isNotNull();
        assertThat(asyncResultMap).containsKey("session_id");
        Map<String, Object> matchingEvent = (Map<String, Object>) asyncResultMap.get("result");
        assertThat(matchingEvent).containsEntry("me_uuid", meUuid);
        assertThat(matchingEvent).containsEntry("ruleset_name", "Test Ruleset");
        assertThat(matchingEvent).containsEntry("rule_name", "temperature_alert");
        Map<String, Map> eventData = (Map<String, Map>) matchingEvent.get("event_data");
        assertThat(eventData.get("m")).containsEntry("critical", true);
        assertThat(eventData.get("m")).containsEntry("temperature", 45);
    }

    @Test
    void testGetHAStats() {
        // Check initial stats
        Map<String, Object> stats = rulesEngine1.getHAStats();
        assertThat(stats).isNotNull();
        assertThat(stats.get("current_leader")).isNull();
        assertThat(stats.get("leader_switches")).isEqualTo(0);
        assertThat(stats.get("events_processed_in_term")).isEqualTo(0);
        assertThat(stats.get("actions_processed_in_term")).isEqualTo(0);

        // Enable leader
        rulesEngine1.enableLeader("test-leader");

        stats = rulesEngine1.getHAStats();
        assertThat(stats.get("current_leader")).isEqualTo("test-leader");
        assertThat(stats.get("leader_switches")).isEqualTo(1);
        assertThat(stats.get("current_term_started_at")).isNotNull();

        // Process an event and action
        String result = rulesEngine1.assertEvent(sessionId1, "{\"temperature\": 35}");
        List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
        String meUuid = (String) ((Map<String, Object>) matchList.get(0).get("temperature_alert")).get("meUuid");

        rulesEngine1.addActionInfo(sessionId1, meUuid, 0, "{\"name\":\"test\",\"status\":\"running\"}");

        stats = rulesEngine1.getHAStats();
        assertThat(stats.get("events_processed_in_term")).isEqualTo(1); // TODO: implement event count increment on assertEvent
        assertThat(stats.get("actions_processed_in_term")).isEqualTo(1);
    }

    @Test
    void testSingleRestart() {
        rulesEngine1.enableLeader("engine-1");

        // Process an event
        String event = """
                {
                    "temperature": 45,
                    "critical": true
                }
                """;
        String result = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(result).contains("temperature_alert");

        // Simulate engine-1 crash
        rulesEngine1 = null;
        consumer1.stop();

        // Simulate restarting engine-1 on the same node. The old instance is gone, so we create a new one
        AstRulesEngine rulesEngine1Restart = new AstRulesEngine();
        rulesEngine1Restart.initializeHA(HA_UUID, TEST_PG_CONFIG, TEST_HA_CONFIG);
        long sessionId1Restart = rulesEngine1Restart.createRuleset(getRuleSet());
        AsyncConsumer consumer1restart = new AsyncConsumer("consumer1-restart");
        consumer1restart.startConsuming(rulesEngine1Restart.port());

        rulesEngine1Restart.enableLeader("engine-1");

        // Process another event
        String event2 = """
                {
                    "temperature": 50,
                    "critical": true
                }
                """;
        String result2 = rulesEngine1Restart.assertEvent(sessionId1Restart, event2);
        assertThat(result2).contains("temperature_alert");

        consumer1restart.stop();
    }
}
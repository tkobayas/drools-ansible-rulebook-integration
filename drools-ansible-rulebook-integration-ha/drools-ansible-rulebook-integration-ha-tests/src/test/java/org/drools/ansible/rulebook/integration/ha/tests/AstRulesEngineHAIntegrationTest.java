package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests for AstRulesEngine with HA functionality
 */
public class AstRulesEngineHAIntegrationTest {

    private AstRulesEngine rulesEngine;
    private long sessionId;
    private static final String SHARED_DB_URL = "jdbc:h2:mem:ast_integration_test;DB_CLOSE_DELAY=-1";

    @BeforeEach
    public void setUp() {
        rulesEngine = new AstRulesEngine();

        // Create a ruleset using the correct AST format
        String ruleset = """
                {
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

        sessionId = rulesEngine.createRuleset(ruleset);
    }

    @AfterEach
    public void tearDown() {
        if (rulesEngine != null) {
            rulesEngine.dispose(sessionId);
        }
    }

    // Helper method to create HAStateManager with shared database
    private HAStateManager createSharedHAStateManager(String uuid) {
        Map<String, Object> config = new HashMap<>();
        config.put("db_url", getCurrentTestDatabaseUrl());
        config.put("write_after", 1);
        HAStateManager manager = HAStateManagerFactory.create();
        manager.initializeHA(uuid, new HashMap<>(), config);
        return manager;
    }

    // Get unique database URL for current test
    private String getCurrentTestDatabaseUrl() {
        // Get the current test method name from the stack trace
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : stackTrace) {
            if (element.getClassName().equals(this.getClass().getName()) && element.getMethodName().startsWith("test")) {
                return "jdbc:h2:mem:" + element.getMethodName() + ";DB_CLOSE_DELAY=-1";
            }
        }
        return SHARED_DB_URL; // fallback
    }

    @Test
    public void testHAConfiguration() {
        // Initialize HA mode with new API
        String uuid = "test-engine-ha";
        Map<String, Object> postgresParams = new HashMap<>();
        // Empty postgres params will fall back to H2

        Map<String, Object> config = new HashMap<>();
        config.put("db_url", getCurrentTestDatabaseUrl());
        config.put("write_after", 1);

        // Should not throw exception
        rulesEngine.initializeHA(uuid, postgresParams, config);

        // Enable leader mode
        rulesEngine.enableLeader("engine-leader-1");

        // Process an event that triggers a rule
        String event = """
                {
                    "temperature": 35,
                    "timestamp": "2024-01-01T10:00:00Z"
                }
                """;

        String result = rulesEngine.assertEvent(sessionId, event);
        assertThat(result).isNotNull();

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
        String stats = rulesEngine.sessionStats(sessionId);
        assertThat(stats).isNotNull();
        Map<String, Object> statsMap = JsonMapper.readValueAsMapOfStringAndObject(stats);
        assertThat(statsMap.get("rulesTriggered")).isEqualTo(1);
    }

    @Test
    public void testMatchingEventPersistence() {
        // Configure HA
        Map<String, Object> config = new HashMap<>();
        config.put("db_url", getCurrentTestDatabaseUrl());
        config.put("write_after", 1);
        rulesEngine.initializeHA("test-uuid-" + System.nanoTime(), new HashMap<>(), config);
        rulesEngine.enableLeader("leader-1");

        // Process an event that triggers a rule
        String event = """
                {
                    "temperature": 35,
                    "sensor": "temp_01"
                }
                """;

        String result = rulesEngine.assertEvent(sessionId, event);
        assertThat(result).isNotNull();

        // Verify that matching events were persisted to database
        // We can check this by accessing the HA state manager directly
        HAStateManager haStateManagerForAssertion = createSharedHAStateManager("test-assertion");

        try {
            List<MatchingEvent> pendingEvents = haStateManagerForAssertion.getPendingMatchingEvents(String.valueOf(sessionId));
            assertThat(pendingEvents).isNotEmpty();

            MatchingEvent me = pendingEvents.get(0);
            assertThat(me.getRuleName()).isEqualTo("temperature_alert");
            // MatchingEvents no longer have status - they exist until deleted
            assertThat(me.getMeUuid()).isNotNull();
        } finally {
            haStateManagerForAssertion.shutdown();
        }
    }

    @Test
    public void testActionStateManagement() {
        // Configure HA
        Map<String, Object> haConfig = new HashMap<>();
        haConfig.put("database_type", "H2");
        haConfig.put("db_url", "jdbc:h2:mem:test_action_state;DB_CLOSE_DELAY=-1");
        haConfig.put("username", "sa");
        haConfig.put("password", "");

        Map<String, Object> config = new HashMap<>();
        config.put("db_url", getCurrentTestDatabaseUrl());
        config.put("write_after", 1);
        rulesEngine.initializeHA("test-uuid-" + System.nanoTime(), new HashMap<>(), config);
        rulesEngine.enableLeader("leader-1");

        // First trigger a rule to create a matching event
        String event = """
                {
                    "temperature": 35,
                    "location": "server_room"
                }
                """;

        String result = rulesEngine.assertEvent(sessionId, event);
        assertThat(result).isNotNull();

        // Get the ME UUID from the database (in real usage, Python would extract from response)
        HAStateManager haStateManagerForAssertion = createSharedHAStateManager("test-assertion-" + System.nanoTime());

        try {
            List<MatchingEvent> pendingEvents = haStateManagerForAssertion.getPendingMatchingEvents(String.valueOf(sessionId));
            assertThat(pendingEvents).hasSize(1);

            String meUuid = pendingEvents.get(0).getMeUuid();

            // Set action state
            // Use new action management APIs
            String actionData = "{\"name\":\"send_alert\",\"status\":\"running\",\"reference_id\":\"job-456\"}";

            rulesEngine.addActionState(sessionId, meUuid, 0, actionData);

            // Check action exists and get it
            assertThat(rulesEngine.actionStateExists(sessionId, meUuid, 0)).isTrue();
            String retrieved = rulesEngine.getActionState(sessionId, meUuid, 0);
            assertThat(retrieved).isEqualTo(actionData);

            // Delete actions when complete
            rulesEngine.deleteActionStates(sessionId, meUuid);

            // Should not exist after deletion
            assertThat(rulesEngine.actionStateExists(sessionId, meUuid, 0)).isFalse();
        } finally {
            haStateManagerForAssertion.shutdown();
        }
    }

    @Test
    public void testLeaderTransition() {
        // Configure HA
        Map<String, Object> haConfig = new HashMap<>();
        haConfig.put("database_type", "H2");
        haConfig.put("db_url", "jdbc:h2:mem:test_leader_transition;DB_CLOSE_DELAY=-1");
        haConfig.put("username", "sa");
        haConfig.put("password", "");

        Map<String, Object> config = new HashMap<>();
        config.put("db_url", getCurrentTestDatabaseUrl());
        config.put("write_after", 1);
        rulesEngine.initializeHA("test-uuid-" + System.nanoTime(), new HashMap<>(), config);

        // Set as leader
        rulesEngine.enableLeader("leader-1");

        // Process some events
        String event = """
                {
                    "temperature": 35,
                    "zone": "production"
                }
                """;
        rulesEngine.assertEvent(sessionId, event);

        // Simulate leader failure - unset leader
        rulesEngine.disableLeader("leader-1");

        // New leader takes over
        rulesEngine.enableLeader("leader-2");

        // Verify we can still process events
        String event2 = """
                {
                    "temperature": 40,
                    "zone": "production"
                }
                """;
        String result2 = rulesEngine.assertEvent(sessionId, event2);
        assertThat(result2).isNotNull();

        // Check that both events created matching events
        HAStateManager haStateManagerForAssertion = createSharedHAStateManager("test-assertion-" + System.nanoTime());

        try {
            List<MatchingEvent> pendingEvents = haStateManagerForAssertion.getPendingMatchingEvents(String.valueOf(sessionId));
            assertThat(pendingEvents).hasSize(2); // Both events should create MEs
        } finally {
            haStateManagerForAssertion.shutdown();
        }
    }

    @Test
    public void testHAOperationsWithoutConfiguration() {
        // Try to set leader without configuring HA
        assertThrows(IllegalStateException.class, () -> {
            rulesEngine.enableLeader("leader-1");
        });
    }

    @Test
    public void testSeparateSessionHA() {
        // Configure HA
        Map<String, Object> haConfig = new HashMap<>();
        haConfig.put("database_type", "H2");
        haConfig.put("db_url", "jdbc:h2:mem:test_separate_session;DB_CLOSE_DELAY=-1");
        haConfig.put("username", "sa");
        haConfig.put("password", "");

        Map<String, Object> config = new HashMap<>();
        config.put("db_url", getCurrentTestDatabaseUrl());
        config.put("write_after", 1);
        rulesEngine.initializeHA("test-uuid-" + System.nanoTime(), new HashMap<>(), config);
        rulesEngine.enableLeader("leader-1");

        // Process event in first session
        rulesEngine.assertEvent(sessionId, "{\"temperature\": 35}");

        // Verify the matching event was created for this session
        HAStateManager haStateManagerForAssertion = createSharedHAStateManager("test-assertion-" + System.nanoTime());
        haStateManagerForAssertion.enableLeader("leader-1");

        try {
            List<MatchingEvent> sessionEvents = haStateManagerForAssertion.getPendingMatchingEvents(String.valueOf(sessionId));
            assertThat(sessionEvents).hasSize(1);
            assertThat(sessionEvents.get(0).getRuleName()).isEqualTo("temperature_alert");
            assertThat(sessionEvents.get(0).getSessionId()).isEqualTo(String.valueOf(sessionId));
        } finally {
            haStateManagerForAssertion.shutdown();
        }
    }

    @Test
    public void testHAWithFailoverRecovery() {
        // Configure shared HA database
        String sharedDbUrl = "jdbc:h2:mem:test_failover_recovery;DB_CLOSE_DELAY=-1";
        Map<String, Object> haConfig = new HashMap<>();
        haConfig.put("database_type", "H2");
        haConfig.put("db_url", sharedDbUrl);
        haConfig.put("username", "sa");
        haConfig.put("password", "");

        // First engine acts as leader
        Map<String, Object> config = new HashMap<>();
        config.put("db_url", getCurrentTestDatabaseUrl());
        config.put("write_after", 1);
        rulesEngine.initializeHA("test-uuid-" + System.nanoTime(), new HashMap<>(), config);
        rulesEngine.enableLeader("engine-1");

        // Process an event
        String event = """
                {
                    "temperature": 45,
                    "critical": true
                }
                """;
        rulesEngine.assertEvent(sessionId, event);

        // Simulate engine-1 failure
        rulesEngine.disableLeader("leader-1");

        // Create second engine (simulating failover)
        AstRulesEngine engine2 = new AstRulesEngine();
        long session2Id = engine2.createRuleset("""
                                                        {
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
                                                                                "name": "send_alert.yml"
                                                                            }
                                                                        ]
                                                                    }
                                                                }}
                                                            ]
                                                        }
                                                        """);

        try {
            // Engine-2 takes over as leader
            Map<String, Object> config2 = new HashMap<>();
            config2.put("db_url", getCurrentTestDatabaseUrl());
            config2.put("write_after", 1);
            engine2.initializeHA("test-uuid-engine2", new HashMap<>(), config2);
            engine2.enableLeader("engine-2");

            // Verify engine-2 can see pending MEs from engine-1
            HAStateManager haStateManagerForAssertion = createSharedHAStateManager("test-assertion-" + System.nanoTime());
            haStateManagerForAssertion.enableLeader("engine-2");

            try {
                // Should see the ME created by engine-1
                List<MatchingEvent> allPendingEvents = haStateManagerForAssertion.getPendingMatchingEvents(String.valueOf(sessionId));
                assertThat(allPendingEvents).hasSize(1);

                MatchingEvent recoveredEvent = allPendingEvents.get(0);
                assertThat(recoveredEvent.getRuleName()).isEqualTo("temperature_alert");
                assertThat(recoveredEvent.getEventData()).isNotNull();
                // Event data is stored as JSON
                assertThat(recoveredEvent.getEventData()).isNotEmpty();
            } finally {
                haStateManagerForAssertion.shutdown();
            }
        } finally {
            engine2.dispose(session2Id);
        }
    }

    @Test
    public void testNewActionAPIs() {
        // Initialize HA
        Map<String, Object> config = new HashMap<>();
        config.put("db_url", getCurrentTestDatabaseUrl());
        config.put("write_after", 1);
        rulesEngine.initializeHA("test-new-actions", new HashMap<>(), config);
        rulesEngine.enableLeader("leader-1");

        // Trigger a rule to create matching event
        String result = rulesEngine.assertEvent(sessionId, "{\"temperature\": 35}");
        assertThat(result).isNotNull();

        // Extract ME UUID from result (in real usage, Python would do this)
        List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
        String meUuid = (String) ((Map<String, Object>) matchList.get(0).get("temperature_alert")).get("meUuid");

        // Test addAction
        String action = "{\"name\":\"send_alert\",\"status\":\"running\",\"reference_id\":\"job-123\",\"start_time\":\"2024-01-01T10:00:00Z\"}";

        rulesEngine.addActionState(sessionId, meUuid, 0, action);

        // Test actionExists
        assertThat(rulesEngine.actionStateExists(sessionId, meUuid, 0)).isTrue();
        assertThat(rulesEngine.actionStateExists(sessionId, meUuid, 1)).isFalse();

        // Test getAction
        String retrieved = rulesEngine.getActionState(sessionId, meUuid, 0);
        assertThat(retrieved).isEqualTo(action);

        // Test updateAction
        String updatedAction = "{\"name\":\"send_alert\",\"status\":\"success\",\"reference_id\":\"job-123\",\"start_time\":\"2024-01-01T10:00:00Z\",\"end_time\":\"2024-01-01T10:05:00Z\"}";
        rulesEngine.updateActionState(sessionId, meUuid, 0, updatedAction);

        retrieved = rulesEngine.getActionState(sessionId, meUuid, 0);
        assertThat(retrieved).isEqualTo(updatedAction);

        // Test deleteActions
        rulesEngine.deleteActionStates(sessionId, meUuid);
        assertThat(rulesEngine.actionStateExists(sessionId, meUuid, 0)).isFalse();
        assertThat(rulesEngine.getActionState(sessionId, meUuid, 0)).isEmpty();
    }

    @Test
    public void testGetHAStats() {
        // Initialize HA
        Map<String, Object> config = new HashMap<>();
        config.put("db_url", getCurrentTestDatabaseUrl());
        config.put("write_after", 1);
        rulesEngine.initializeHA("test-ha-stats", new HashMap<>(), config);

        // Check initial stats
        Map<String, Object> stats = rulesEngine.getHAStats();
        assertThat(stats).isNotNull();
        assertThat(stats.get("current_leader")).isNull();
        assertThat(stats.get("leader_switches")).isEqualTo(0);
        assertThat(stats.get("events_processed_in_term")).isEqualTo(0);
        assertThat(stats.get("actions_processed_in_term")).isEqualTo(0);

        // Enable leader
        rulesEngine.enableLeader("test-leader");

        stats = rulesEngine.getHAStats();
        assertThat(stats.get("current_leader")).isEqualTo("test-leader");
        assertThat(stats.get("leader_switches")).isEqualTo(1);
        assertThat(stats.get("current_term_started_at")).isNotNull();

        // Process an event and action
        String result = rulesEngine.assertEvent(sessionId, "{\"temperature\": 35}");
        List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
        String meUuid = (String) ((Map<String, Object>) matchList.get(0).get("temperature_alert")).get("meUuid");

        rulesEngine.addActionState(sessionId, meUuid, 0, "{\"name\":\"test\",\"status\":\"running\"}");

        stats = rulesEngine.getHAStats();
        assertThat(stats.get("events_processed_in_term")).isEqualTo(1);
        assertThat(stats.get("actions_processed_in_term")).isEqualTo(1);
    }
}
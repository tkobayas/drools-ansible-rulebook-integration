package org.drools.ansible.rulebook.integration.ha.tests;

import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.ActionState;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Integration tests for AstRulesEngine with HA functionality
 */
public class AstRulesEngineHAIntegrationTest {
    
    private AstRulesEngine rulesEngine;
    private long sessionId;
    
    @Before
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
    
    @After
    public void tearDown() {
        if (rulesEngine != null) {
            rulesEngine.dispose(sessionId);
        }
    }
    
    @Test
    public void testHAConfiguration() {
        // Configure HA mode
        Map<String, Object> haConfig = new HashMap<>();
        haConfig.put("database_type", "H2");
        haConfig.put("db_url", "jdbc:h2:mem:test_engine_ha;DB_CLOSE_DELAY=-1");
        haConfig.put("username", "sa");
        haConfig.put("password", "");
        
        // Should not throw exception
        rulesEngine.configureHA(haConfig);
        
        // Set as leader
        rulesEngine.setLeader("engine-leader-1");
        
        // Process an event that triggers a rule
        String event = """
            {
                "temperature": 35,
                "timestamp": "2024-01-01T10:00:00Z"
            }
            """;
        
        String result = rulesEngine.assertEvent(sessionId, event);
        assertNotNull(result);
        
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
        assertNotNull(stats);
        Map<String, Object> statsMap = JsonMapper.readValueAsMapOfStringAndObject(stats);
        assertThat(statsMap.get("rulesTriggered")).isEqualTo(1);
    }
    
    @Test
    public void testMatchingEventPersistence() {
        // Configure HA
        Map<String, Object> haConfig = new HashMap<>();
        haConfig.put("database_type", "H2");
        haConfig.put("db_url", "jdbc:h2:mem:test_me_persistence;DB_CLOSE_DELAY=-1");
        haConfig.put("username", "sa");
        haConfig.put("password", "");
        
        rulesEngine.configureHA(haConfig);
        rulesEngine.setLeader("leader-1");
        
        // Process an event that triggers a rule
        String event = """
            {
                "temperature": 35,
                "sensor": "temp_01"
            }
            """;
        
        String result = rulesEngine.assertEvent(sessionId, event);
        assertNotNull(result);
        
        // Verify that matching events were persisted to database
        // We can check this by accessing the HA state manager directly
        HAStateManager haStateManagerForAssertion = HAStateManagerFactory.create(haConfig);
        
        try {
            List<MatchingEvent> pendingEvents = haStateManagerForAssertion.getPendingMatchingEvents(String.valueOf(sessionId));
            assertThat(pendingEvents).isNotEmpty();
            
            MatchingEvent me = pendingEvents.get(0);
            assertEquals("temperature_alert", me.getRuleName());
            assertEquals(MatchingEvent.MatchingEventStatus.PENDING, me.getStatus());
            assertNotNull(me.getMeUuid());
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
        
        rulesEngine.configureHA(haConfig);
        rulesEngine.setLeader("leader-1");
        
        // First trigger a rule to create a matching event
        String event = """
            {
                "temperature": 35,
                "location": "server_room"
            }
            """;
        
        String result = rulesEngine.assertEvent(sessionId, event);
        assertNotNull(result);
        
        // Get the ME UUID from the database (in real usage, Python would extract from response)
        HAStateManager haStateManagerForAssertion = HAStateManagerFactory.create(haConfig);

        try {
            List<MatchingEvent> pendingEvents = haStateManagerForAssertion.getPendingMatchingEvents(String.valueOf(sessionId));
            assertThat(pendingEvents).hasSize(1);
            
            String meUuid = pendingEvents.get(0).getMeUuid();
            
            // Set action state
            ActionState actionState = new ActionState();
            actionState.setMeUuid(meUuid);
            ActionState.Action action = new ActionState.Action();
            action.setName("send_alert");
            action.setStatus(ActionState.Action.ActionStatus.STARTED);
            action.setReferenceId("job-456");
            actionState.getActions().add(action);
            
            rulesEngine.setActionState(sessionId, meUuid, actionStateToMap(actionState));
            
            // Get action state
            Map<String, Object> retrieved = rulesEngine.getActionState(sessionId, meUuid);
            assertNotNull(retrieved);
            assertEquals(meUuid, retrieved.get("me_uuid"));
            
            // Delete ME when actions complete
            rulesEngine.deleteMatchingEvent(sessionId, meUuid);
            
            // Should be null after deletion
            assertNull(rulesEngine.getActionState(sessionId, meUuid));
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
        
        rulesEngine.configureHA(haConfig);
        
        // Set as leader
        rulesEngine.setLeader("leader-1");
        
        // Process some events
        String event = """
            {
                "temperature": 35,
                "zone": "production"
            }
            """;
        rulesEngine.assertEvent(sessionId, event);
        
        // Simulate leader failure - unset leader
        rulesEngine.unsetLeader();
        
        // New leader takes over
        rulesEngine.setLeader("leader-2");
        
        // Verify we can still process events
        String event2 = """
            {
                "temperature": 40,
                "zone": "production"
            }
            """;
        String result2 = rulesEngine.assertEvent(sessionId, event2);
        assertNotNull(result2);
        
        // Check that both events created matching events
        HAStateManager haStateManagerForAssertion = HAStateManagerFactory.create(haConfig);

        try {
            List<MatchingEvent> pendingEvents = haStateManagerForAssertion.getPendingMatchingEvents(String.valueOf(sessionId));
            assertThat(pendingEvents).hasSize(2); // Both events should create MEs
        } finally {
            haStateManagerForAssertion.shutdown();
        }
    }
    
    @Test(expected = IllegalStateException.class)
    public void testHAOperationsWithoutConfiguration() {
        // Try to set leader without configuring HA
        rulesEngine.setLeader("leader-1");
    }
    
    @Test
    public void testSeparateSessionHA() {
        // Configure HA
        Map<String, Object> haConfig = new HashMap<>();
        haConfig.put("database_type", "H2");
        haConfig.put("db_url", "jdbc:h2:mem:test_separate_session;DB_CLOSE_DELAY=-1");
        haConfig.put("username", "sa");
        haConfig.put("password", "");
        
        rulesEngine.configureHA(haConfig);
        rulesEngine.setLeader("leader-1");
        
        // Process event in first session
        rulesEngine.assertEvent(sessionId, "{\"temperature\": 35}");
        
        // Verify the matching event was created for this session
        HAStateManager haStateManagerForAssertion = HAStateManagerFactory.create(haConfig);
        haStateManagerForAssertion.setLeader("leader-1");
        
        try {
            List<MatchingEvent> sessionEvents = haStateManagerForAssertion.getPendingMatchingEvents(String.valueOf(sessionId));
            assertThat(sessionEvents).hasSize(1);
            assertEquals("temperature_alert", sessionEvents.get(0).getRuleName());
            assertEquals(String.valueOf(sessionId), sessionEvents.get(0).getSessionId());
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
        rulesEngine.configureHA(haConfig);
        rulesEngine.setLeader("engine-1");
        
        // Process an event
        String event = """
            {
                "temperature": 45,
                "critical": true
            }
            """;
        rulesEngine.assertEvent(sessionId, event);
        
        // Simulate engine-1 failure
        rulesEngine.unsetLeader();
        
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
            engine2.configureHA(haConfig);
            engine2.setLeader("engine-2");
            
            // Verify engine-2 can see pending MEs from engine-1
            HAStateManager haStateManagerForAssertion = HAStateManagerFactory.create(haConfig);
            haStateManagerForAssertion.setLeader("engine-2");
            
            try {
                // Should see the ME created by engine-1
                List<MatchingEvent> allPendingEvents = haStateManagerForAssertion.getPendingMatchingEvents(String.valueOf(sessionId));
                assertThat(allPendingEvents).hasSize(1);
                
                MatchingEvent recoveredEvent = allPendingEvents.get(0);
                assertEquals("temperature_alert", recoveredEvent.getRuleName());
                assertNotNull(recoveredEvent.getMatchingFacts());
                // Facts are stored as internal Drools objects, not original JSON
                assertThat(recoveredEvent.getMatchingFacts()).isNotEmpty();
            } finally {
                haStateManagerForAssertion.shutdown();
            }
        } finally {
            engine2.dispose(session2Id);
        }
    }
    
    // Utility method to convert ActionState to Map for API compatibility
    private Map<String, Object> actionStateToMap(ActionState actionState) {
        Map<String, Object> map = new HashMap<>();
        map.put("me_uuid", actionState.getMeUuid());
        map.put("actions", actionState.getActions());
        return map;
    }
}
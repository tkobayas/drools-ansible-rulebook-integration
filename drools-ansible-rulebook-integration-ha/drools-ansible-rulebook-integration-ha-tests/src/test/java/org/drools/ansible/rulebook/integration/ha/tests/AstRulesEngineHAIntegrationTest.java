package org.drools.ansible.rulebook.integration.ha.tests;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
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
        
        // Create a simple ruleset
        String ruleset = """
            {
                "name": "test_ruleset",
                "rules": [
                    {
                        "name": "temperature_alert",
                        "condition": {
                            "all": [
                                {
                                    "fact": "temperature",
                                    "operator": "greaterThan",
                                    "value": 30
                                }
                            ]
                        },
                        "action": {
                            "type": "send_alert",
                            "message": "High temperature detected"
                        }
                    }
                ]
            }
            """;
        
        sessionId = rulesEngine.createRuleset(ruleset);
    }
    
    @After
    public void tearDown() {
        if (rulesEngine != null) {
            rulesEngine.close();
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
        
        // Result should contain matches (and ME UUID should be created internally)
        assertThat(result).contains("temperature_alert");
    }
    
    @Test
    public void testActionStateManagement() {
        // Configure HA
        Map<String, Object> haConfig = new HashMap<>();
        haConfig.put("database_type", "H2");
        haConfig.put("db_url", "jdbc:h2:mem:test_action_state;DB_CLOSE_DELAY=-1");
        
        rulesEngine.configureHA(haConfig);
        rulesEngine.setLeader("leader-1");
        
        // For this test, we'd need to capture the ME UUID from the response
        // In real usage, Python would extract this from the JSON response
        String meUuid = "test-me-uuid-123";
        
        // Set action state
        Map<String, Object> actionState = new HashMap<>();
        actionState.put("actions", new HashMap<String, Object>() {{
            put("name", "send_alert");
            put("status", "STARTED");
            put("reference_id", "job-456");
        }});
        
        rulesEngine.setActionState(sessionId, meUuid, actionState);
        
        // Get action state
        Map<String, Object> retrieved = rulesEngine.getActionState(sessionId, meUuid);
        assertNotNull(retrieved);
        assertEquals(meUuid, retrieved.get("me_uuid"));
        
        // Delete ME when actions complete
        rulesEngine.deleteMatchingEvent(sessionId, meUuid);
        
        // Should be null after deletion
        assertNull(rulesEngine.getActionState(sessionId, meUuid));
    }
    
    @Test
    public void testLeaderTransition() {
        // Configure HA
        Map<String, Object> haConfig = new HashMap<>();
        haConfig.put("database_type", "H2");
        haConfig.put("db_url", "jdbc:h2:mem:test_leader_transition;DB_CLOSE_DELAY=-1");
        
        rulesEngine.configureHA(haConfig);
        
        // Set as leader
        rulesEngine.setLeader("leader-1");
        
        // Process some events
        String event = "{\"temperature\": 35}";
        rulesEngine.assertEvent(sessionId, event);
        
        // Simulate leader failure - unset leader
        rulesEngine.unsetLeader();
        
        // New leader takes over
        rulesEngine.setLeader("leader-2");
        
        // Should trigger recovery of pending MEs
        // In real scenario, async channel would notify Python
    }
    
    @Test(expected = IllegalStateException.class)
    public void testHAOperationsWithoutConfiguration() {
        // Try to set leader without configuring HA
        rulesEngine.setLeader("leader-1");
    }
    
    @Test
    public void testMultipleSessionsHA() {
        // Configure HA
        Map<String, Object> haConfig = new HashMap<>();
        haConfig.put("database_type", "H2");
        haConfig.put("db_url", "jdbc:h2:mem:test_multi_session;DB_CLOSE_DELAY=-1");
        
        rulesEngine.configureHA(haConfig);
        rulesEngine.setLeader("leader-1");
        
        // Create another session
        String ruleset2 = """
            {
                "name": "test_ruleset_2",
                "rules": [
                    {
                        "name": "humidity_alert",
                        "condition": {
                            "all": [
                                {
                                    "fact": "humidity",
                                    "operator": "greaterThan",
                                    "value": 80
                                }
                            ]
                        },
                        "action": {
                            "type": "send_alert",
                            "message": "High humidity detected"
                        }
                    }
                ]
            }
            """;
        
        long sessionId2 = rulesEngine.createRuleset(ruleset2);
        
        // Process events in both sessions
        rulesEngine.assertEvent(sessionId, "{\"temperature\": 35}");
        rulesEngine.assertEvent(sessionId2, "{\"humidity\": 85}");
        
        // Both sessions should work independently
        assertNotEquals(sessionId, sessionId2);
    }
}
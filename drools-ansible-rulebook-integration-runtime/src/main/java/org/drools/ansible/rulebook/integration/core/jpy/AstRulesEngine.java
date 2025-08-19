package org.drools.ansible.rulebook.integration.core.jpy;

import org.drools.ansible.rulebook.integration.api.*;
import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.ActionState;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class AstRulesEngine implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AstRulesEngine.class);

    private final RulesExecutorContainer rulesExecutorContainer = new RulesExecutorContainer();
    
    private HAStateManager haStateManager;
    private boolean haMode = false;
    private boolean shutdown = false;

    public long createRuleset(String rulesetString) {
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString);
        return createRuleset(rulesSet);
    }

    public long createRuleset(RulesSet rulesSet) {
        checkAlive();
        if (rulesSet.hasTemporalConstraint()) {
            rulesSet.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK);
            if (rulesSet.hasAsyncExecution()) {
                rulesExecutorContainer.allowAsync();
            }
        }
        RulesExecutor executor = rulesExecutorContainer.register( RulesExecutorFactory.createRulesExecutor(rulesSet) );
        return executor.getId();
    }

    public String sessionStats(long sessionId) {
        RulesExecutor rulesExecutor = rulesExecutorContainer.get(sessionId);
        return rulesExecutor == null ? null : toJson( rulesExecutor.getSessionStats() );
    }

    public String dispose(long sessionId) {
        RulesExecutor rulesExecutor = rulesExecutorContainer.get(sessionId);
        return rulesExecutor == null ? null : toJson( rulesExecutor.dispose() );
    }

    @Deprecated
    public String retractFact(long sessionId, String serializedFact) {
        return matchesToJson( rulesExecutorContainer.get(sessionId).processRetractMatchingFacts(serializedFact, false).join() );
    }

    public String retractMatchingFacts(long sessionId, String serializedFact, boolean allowPartialMatch, String... keysToExclude) {
        return matchesToJson( rulesExecutorContainer.get(sessionId).processRetractMatchingFacts(serializedFact, allowPartialMatch, keysToExclude).join() );
    }

    public String assertFact(long sessionId, String serializedFact) {
        return matchesToJson( rulesExecutorContainer.get(sessionId).processFacts(serializedFact).join() );
    }

    public String assertEvent(long sessionId, String serializedFact) {
        List<Match> matches = rulesExecutorContainer.get(sessionId).processEvents(serializedFact).join();
        String result = matchesToJson(matches);
        
        // In HA mode, create matching events for triggered rules
        if (haMode && haStateManager != null && haStateManager.isLeader() && !matches.isEmpty()) {
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("matches", RuleMatch.asList(matches));
            
            // Create ME UUID for each match
            for (Match match : matches) {
                String ruleName = match.getRule().getName();
                String rulesetName = match.getRule().getPackageName();
                Map<String, Object> facts = new HashMap<>();
                match.getObjects().forEach(obj -> facts.put(obj.getClass().getSimpleName(), obj));
                
                String meUuid = haStateManager.addMatchingEvent(
                    String.valueOf(sessionId), rulesetName, ruleName, facts
                );
                logger.debug("Created ME UUID {} for rule {}/{}", meUuid, rulesetName, ruleName);
            }
        }
        
        return result;
    }

    /**
     * Advances the clock time in the specified unit amount.
     *
     * @param amount the amount of units to advance in the clock
     * @param unit the used time unit
     * @return the events that fired
     */
    public String advanceTime(long sessionId, long amount, String unit) {
        return matchesToJson( rulesExecutorContainer.get(sessionId).advanceTime(amount, TimeUnit.valueOf(unit.toUpperCase())).join() );
    }

    private static String matchesToJson(List<Match> matches) {
        return toJson(RuleMatch.asList(matches));
    }

    public String getFacts(long sessionId) {
        RulesExecutor executor = rulesExecutorContainer.get(sessionId);
        if (executor == null) {
            throw new NoSuchElementException("No such session id: " + sessionId + ". " + "Was it disposed?");
        }
        return toJson(executor.getAllFactsAsMap().stream().map(RulesModelUtil::factToMap).collect(Collectors.toList()));
    }

    public void shutdown() {
        close();
    }

    @Override
    public void close() {
        shutdown = true;
        if (haStateManager != null) {
            haStateManager.shutdown();
        }
        rulesExecutorContainer.disposeAll();
    }

    public int port() {
        return rulesExecutorContainer.port();
    }

    private void checkAlive() {
        if (shutdown) {
            throw new IllegalStateException("This AstRulesEngine is shutting down");
        }
    }
    
    // ========== High Availability APIs ==========
    
    /**
     * Configure HA mode with database connection parameters
     * Called by Python: self._api.configure_ha(...)
     */
    public void configureHA(Map<String, Object> config) {
        logger.info("Configuring HA mode with config: {}", config);
        
        try {
            this.haStateManager = HAStateManagerFactory.create(config);
            this.haMode = true;
            
            // Check for pending matching events on startup
            if (haStateManager.isLeader()) {
                recoverPendingMatchingEvents();
            }
            
            logger.info("HA mode configured successfully");
        } catch (Exception e) {
            logger.error("Failed to configure HA mode", e);
            throw new RuntimeException("Failed to configure HA mode: " + e.getMessage(), e);
        }
    }
    
    /**
     * Set this node as the leader
     * Called by Python: self._api.set_leader(...)
     */
    public void setLeader(String leaderId) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not configured");
        }
        
        logger.info("Setting node as leader with ID: {}", leaderId);
        haStateManager.setLeader(leaderId);
        
        // Recover pending actions when becoming leader
        recoverPendingMatchingEvents();
    }
    
    /**
     * Unset leader status
     * Called by Python: self._api.unset_leader(...)
     */
    public void unsetLeader() {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not configured");
        }
        
        logger.info("Unsetting leader status");
        haStateManager.unsetLeader();
    }
    
    /**
     * Get action state for a matching event
     * Called by Python: self._api.get_action_state(ruleset_session, me_uuid)
     */
    public Map<String, Object> getActionState(long sessionId, String meUuid) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not configured");
        }
        
        ActionState actionState = haStateManager.getActionState(String.valueOf(sessionId), meUuid);
        if (actionState == null) {
            return null;
        }
        
        // Convert to Map for Python/JPY
        Map<String, Object> result = new HashMap<>();
        result.put("me_uuid", actionState.getMeUuid());
        result.put("actions", actionState.getActions());
        return result;
    }
    
    /**
     * Set/update action state for a matching event
     * Called by Python: self._api.set_action_state(ruleset_session, me_uuid, action_state)
     */
    public void setActionState(long sessionId, String meUuid, Map<String, Object> actionStateMap) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not configured");
        }
        
        ActionState actionState = new ActionState();
        actionState.setMeUuid(meUuid);
        
        // Parse action state from Python
        if (actionStateMap.containsKey("actions")) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> actions = (List<Map<String, Object>>) actionStateMap.get("actions");
            // Convert to ActionState.Action objects
            // This would need proper mapping implementation
        }
        
        haStateManager.persistActionState(String.valueOf(sessionId), meUuid, actionState);
        logger.debug("Updated action state for ME UUID: {}", meUuid);
    }
    
    /**
     * Delete matching event and associated action state
     * Called by Python: self._api.delete_me(ruleset_session, me_uuid)
     */
    public void deleteMatchingEvent(long sessionId, String meUuid) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not configured");
        }
        
        haStateManager.removeMatchingEvent(String.valueOf(sessionId), meUuid);
        logger.debug("Deleted matching event: {}", meUuid);
    }
    
    /**
     * Recover pending matching events when becoming leader
     */
    private void recoverPendingMatchingEvents() {
        logger.info("Checking for pending matching events to recover");
        
        // Ensure async channel is available for recovery
        if (rulesExecutorContainer.getChannel() == null) {
            rulesExecutorContainer.allowAsync();
        }
        
        for (RulesExecutor executor : rulesExecutorContainer.getAllExecutors()) {
            String sessionId = String.valueOf(executor.getId());
            List<MatchingEvent> pendingEvents = haStateManager.getPendingMatchingEvents(sessionId);
            
            if (!pendingEvents.isEmpty()) {
                logger.info("Found {} pending matching events for session {}", 
                           pendingEvents.size(), sessionId);
                
                // Send each pending ME through async channel for Python to recover
                for (MatchingEvent pendingEvent : pendingEvents) {
                    sendMatchingEventRecovery(executor.getId(), pendingEvent);
                }
            }
        }
    }
    
    /**
     * Send a matching event recovery notification through the async channel
     */
    private void sendMatchingEventRecovery(long sessionId, MatchingEvent matchingEvent) {
        if (rulesExecutorContainer.getChannel() == null || !rulesExecutorContainer.getChannel().isConnected()) {
            logger.warn("Async channel not available for ME recovery: {}", matchingEvent.getMeUuid());
            return;
        }
        
        // Create recovery payload with ME UUID and action state
        Map<String, Object> recoveryData = new HashMap<>();
        recoveryData.put("type", "MATCHING_EVENT_RECOVERY");
        recoveryData.put("me_uuid", matchingEvent.getMeUuid());
        recoveryData.put("ruleset_name", matchingEvent.getRulesetName());
        recoveryData.put("rule_name", matchingEvent.getRuleName());
        recoveryData.put("matching_facts", matchingEvent.getMatchingFacts());
        
        // Include action state if available
        if (matchingEvent.getActionState() != null) {
            Map<String, Object> actionStateMap = new HashMap<>();
            actionStateMap.put("actions", matchingEvent.getActionState().getActions());
            recoveryData.put("action_state", actionStateMap);
        }
        
        // Send through async channel
        Response response = new Response(sessionId, recoveryData);
        rulesExecutorContainer.getChannel().write(response);
        
        logger.info("Sent ME recovery notification for UUID: {} on session: {}", 
                   matchingEvent.getMeUuid(), sessionId);
    }
}

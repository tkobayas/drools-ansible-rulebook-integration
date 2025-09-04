package org.drools.ansible.rulebook.integration.core.jpy;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.EventState;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
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

        if (haMode && haStateManager != null && haStateManager.isLeader()) {
            return processHA(sessionId, matches);
        } else {
            return matchesToJson(matches);
        }
    }

    private String processHA(long sessionId, List<Match> matches) {
        String result = matchesToJson(matches);

        // In HA mode, persist event state for statistics tracking
        try {
            // TODO: Populate full EventState with partial matches, time windows, clock time, etc.
            EventState eventState = new EventState();
            eventState.setSessionId(String.valueOf(sessionId));
            eventState.setSessionStats(rulesExecutorContainer.get(sessionId).getSessionStats());
            haStateManager.persistEventState(String.valueOf(sessionId), eventState);
        } catch (Exception e) {
            logger.warn("Failed to persist event state for HA statistics", e);
        }

        // In HA mode, create matching events for triggered rules and add ME UUIDs to response
        if (!matches.isEmpty()) {
            try {
                logger.debug("Original response before ME UUID addition: {}", result);

                // Parse the existing JSON result
                // TODO: This part can be optimized to avoid double serialization
                List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
                logger.debug("Parsed match list: {}", matchList);

                // Process each match and add ME UUID
                for (int i = 0; i < matches.size() && i < matchList.size(); i++) {
                    Match match = matches.get(i);
                    Map<String, Object> matchJson = matchList.get(i);

                    String ruleName = match.getRule().getName();
                    String rulesetName = match.getRule().getPackageName();
                    Map<String, Object> facts = new HashMap<>();
                    match.getObjects().forEach(obj -> facts.put(obj.getClass().getSimpleName(), obj));

                    // Create MatchingEvent object
                    MatchingEvent me = new MatchingEvent();
                    me.setSessionId(String.valueOf(sessionId));
                    me.setRuleSetName(rulesetName);
                    me.setRuleName(ruleName);

                    // Serialize facts as JSON
                    try {
                        String eventDataJson = toJson(facts);
                        me.setEventData(eventDataJson);
                    } catch (Exception e) {
                        logger.warn("Failed to serialize matching facts to JSON", e);
                        me.setEventData("{}");
                    }

                    String meUuid = haStateManager.addMatchingEvent(me);
                    logger.debug("Created ME UUID {} for rule {}/{}", meUuid, rulesetName, ruleName);

                    // Add meUuid to the rule object in JSON
                    // Structure: [{"ruleName": {"m": {...}, "meUuid": "..."}}]
                    if (matchJson.containsKey(ruleName)) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> ruleObject = (Map<String, Object>) matchJson.get(ruleName);
                        ruleObject.put("meUuid", meUuid);
                        logger.debug("Added meUuid to rule object for {}", ruleName);
                    } else {
                        logger.warn("Rule name {} not found in matchJson keys: {}", ruleName, matchJson.keySet());
                    }
                }

                result = toJson(matchList);
                logger.debug("Modified response with ME UUIDs: {}", result);
            } catch (Exception e) {
                logger.error("Failed to parse or modify assertEvent response for HA mode", e);
                // Fall back to original response if JSON processing fails
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
     * Initialize HA mode with UUID and database configuration
     * Called by Python: self._api.initializeHA(uuid, postgres_params, config)
     */
    public void initializeHA(String uuid, Map<String, Object> postgresParams, Map<String, Object> config) {
        logger.info("Initializing HA mode with UUID: {}", uuid);
        
        try {
            this.haStateManager = HAStateManagerFactory.create();
            this.haStateManager.initializeHA(uuid, postgresParams, config);
            this.haMode = true;
            
            logger.info("HA mode initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize HA mode", e);
            throw new RuntimeException("Failed to initialize HA mode: " + e.getMessage(), e);
        }
    }
    
    /**
     * Enable leader mode and start writing states to database
     * Called by Python: self._api.enableLeader(leader_name)
     */
    public void enableLeader(String leaderName) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        logger.info("Enabling leader mode for: {}", leaderName);
        haStateManager.enableLeader(leaderName);
        
        // Recover pending actions when becoming leader
        recoverPendingMatchingEvents();
    }
    
    /**
     * Disable leader mode and stop writing to database
     * Called by Python: self._api.disableLeader(leader_name)
     */
    public void disableLeader(String leaderName) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        logger.info("Disabling leader mode for: {}", leaderName);
        haStateManager.disableLeader(leaderName);
    }
    
    /**
     * Add an action for a matching event
     * Called by Python: self._api.addActionState(session, matching_uuid, index, action)
     */
    public void addActionState(long sessionId, String matchingUuid, int index, String action) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        haStateManager.addActionState(String.valueOf(sessionId), matchingUuid, index, action);
        logger.debug("Added action at index {} for ME UUID: {}", index, matchingUuid);
    }
    
    /**
     * Update an existing action
     * Called by Python: self._api.updateActionState(session, matching_uuid, index, action)
     */
    public void updateActionState(long sessionId, String matchingUuid, int index, String action) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        haStateManager.updateActionState(String.valueOf(sessionId), matchingUuid, index, action);
        logger.debug("Updated action at index {} for ME UUID: {}", index, matchingUuid);
    }
    
    /**
     * Check if an action exists
     * Called by Python: self._api.actionStateExists(session, matching_uuid, index)
     */
    public boolean actionStateExists(long sessionId, String matchingUuid, int index) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        return haStateManager.actionStateExists(String.valueOf(sessionId), matchingUuid, index);
    }
    
    /**
     * Get an action by index
     * Called by Python: self._api.getActionState(session, matching_uuid, index)
     */
    public String getActionState(long sessionId, String matchingUuid, int index) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        return haStateManager.getActionState(String.valueOf(sessionId), matchingUuid, index);
    }
    
    /**
     * Delete all actions and matching events for a matching UUID
     * Called by Python: self._api.deleteActionStates(session, matching_uuid)
     */
    public void deleteActionStates(long sessionId, String matchingUuid) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        haStateManager.deleteActionStates(String.valueOf(sessionId), matchingUuid);
        logger.debug("Deleted all actions for ME UUID: {}", matchingUuid);
    }
    
    /**
     * Get current HA statistics
     * Called by Python: self._api.getHAStats()
     */
    public Map<String, Object> getHAStats() {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        HAStats stats = haStateManager.getHAStats();
        Map<String, Object> result = new HashMap<>();
        result.put("current_leader", stats.getCurrentLeader());
        result.put("leader_switches", stats.getLeaderSwitches());
        result.put("current_term_started_at", stats.getCurrentTermStartedAt());
        result.put("events_processed_in_term", stats.getEventsProcessedInTerm());
        result.put("actions_processed_in_term", stats.getActionsProcessedInTerm());
        
        return result;
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
        
        // Create recovery payload with ME UUID
        Map<String, Object> recoveryData = new HashMap<>();
        recoveryData.put("type", "MATCHING_EVENT_RECOVERY");
        recoveryData.put("me_uuid", matchingEvent.getMeUuid());
        recoveryData.put("ruleset_name", matchingEvent.getRuleSetName());
        recoveryData.put("rule_name", matchingEvent.getRuleName());
        
        // Parse event data JSON back to object for compatibility
        try {
            if (matchingEvent.getEventData() != null) {
                Map<String, Object> eventData = readValueAsMapOfStringAndObject(matchingEvent.getEventData());
                recoveryData.put("matching_facts", eventData);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse event data JSON for recovery", e);
            recoveryData.put("matching_facts", new HashMap<>());
        }
        
        // Send through async channel
        Response response = new Response(sessionId, recoveryData);
        rulesExecutorContainer.getChannel().write(response);
        
        logger.info("Sent ME recovery notification for UUID: {} on session: {}", 
                   matchingEvent.getMeUuid(), sessionId);
    }
}

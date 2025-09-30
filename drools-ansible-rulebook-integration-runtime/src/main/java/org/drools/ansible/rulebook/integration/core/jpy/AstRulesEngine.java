package org.drools.ansible.rulebook.integration.core.jpy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.drools.ansible.rulebook.integration.ha.api.HARulesExecutor;
import org.drools.ansible.rulebook.integration.ha.api.HARulesExecutorFactory;
import org.drools.ansible.rulebook.integration.ha.api.HASessionContext;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.calculateStateSHA;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256;

public class AstRulesEngine implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AstRulesEngine.class);

    private final RulesExecutorContainer rulesExecutorContainer = new RulesExecutorContainer();
    
    private HAStateManager haStateManager;
    private boolean haMode = false;
    private boolean shutdown = false;

    public long createRuleset(String rulesetString) {
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString);
        return createRuleset(rulesSet, rulesetString);
    }

    public long createRuleset(RulesSet rulesSet) {
        return createRuleset(rulesSet, null);
    }

    public long createRuleset(RulesSet rulesSet, String rulesetString) {
        checkAlive();
        if (rulesSet.hasTemporalConstraint()) {
            rulesSet.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK);
            if (rulesSet.hasAsyncExecution()) {
                rulesExecutorContainer.allowAsync();
            }
        }

        RulesExecutor executor;

        if (haMode && haStateManager != null) {
            // regardless of leader or not, try to restore session state if exists
            executor = createHARulesExecutorWithSessionState(rulesSet, rulesetString);
        } else {
            // normal non-HA mode
            executor = RulesExecutorFactory.createRulesExecutor(rulesSet);
        }

        rulesExecutorContainer.register( executor );

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
            return processEventHA(sessionId, matches);
        } else {
            return matchesToJson(matches);
        }
    }

    private String processEventHA(long sessionId, List<Match> matches) {
        // matches is the internal representation of drools matches
        // matchList is the simplified Map structure for JSON-serialization
        List<Map<String, Map>> matchList = RuleMatch.asList(matches);

        try {
            HARulesExecutor rulesExecutor = (HARulesExecutor) rulesExecutorContainer.get(sessionId);

            SessionState sessionState = haStateManager.getSessionState();

            HASessionContext haSessionContext = rulesExecutor.getHaSessionContext();
            LinkedHashMap<String, EventRecord> eventUuidsInMemory = haSessionContext.getEventUuidsInMemory();
            sessionState.setPartialEvents(new ArrayList<>(eventUuidsInMemory.values()));
            sessionState.setPersistedTime(rulesExecutor.asKieSession().getSessionClock().getCurrentTime());

            String currentStateSHA = sessionState.getCurrentStateSHA();
            sessionState.setPreviousStateSHA(currentStateSHA);
            sessionState.setCurrentStateSHA(calculateStateSHA(currentStateSHA, haSessionContext.getCurrentEventUuid()));
            sessionState.setLastProcessedEventUuid(haSessionContext.getCurrentEventUuid());

            haStateManager.persistSessionState(sessionState);

            HAStats haStats = haStateManager.getHAStats();
            haStats.incrementEventsProcessed(); // TODO: increment by number of events if batch processing
            haStateManager.persistHAStats();
        } catch (Exception e) {
            logger.warn("Failed to persist event state for HA statistics", e);
        }

        // In HA mode, create matching events for triggered rules and add ME UUIDs to response
        if (!matches.isEmpty()) {
            try {
                // Process each match and add ME UUID
                for (int i = 0; i < matchList.size(); i++) {
                    Map<String, Map> matchData = matchList.get(i); // e.g. {"temperature_alert":{"m":{"critical":true,"temperature":45}}}
                    String ruleName = matchData.keySet().iterator().next(); // 1st level key is rule name
                    String rulesetName = rulesExecutorContainer.get(sessionId).getRuleSetName();
                    Map eventData = matchData.get(ruleName); // e.g. {"m":{"critical":true,"temperature":45}}

                    // Create MatchingEvent object
                    MatchingEvent me = new MatchingEvent();
                    me.setHaUuid(haStateManager.getHaUuid());
                    me.setRuleSetName(rulesetName);
                    me.setRuleName(ruleName);
                    me.setEventData(toJson(eventData));

                    String meUuid = haStateManager.addMatchingEvent(me);
                    logger.debug("Created ME UUID {} for rule {}/{}", meUuid, rulesetName, ruleName);

                    // Add meUuid to the rule object in JSON
                    // Structure: [{"ruleName": {"m": {...}, "meUuid": "..."}}]
                    Map<String, Object> eventMap = (Map<String, Object>) matchData.get(ruleName);
                    eventMap.put("meUuid", meUuid);
                    logger.debug("Added meUuid to result match for {}", ruleName);
                }

                String result = toJson(matchList);
                logger.debug("Modified response with ME UUIDs: {}", result);
                return result;
            } catch (Exception e) {
                logger.error("Failed to parse or modify assertEvent response for HA mode", e);
                // Fall back to original response if JSON processing fails
            }
        }

        return toJson(matchList);
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

            // HA mode always requires async channel
            rulesExecutorContainer.allowAsync();
            
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

        restoreAllSessions();

        // Recover pending actions when becoming leader
        recoverPendingMatchingEvents();
    }

    private void restoreAllSessions() {
        // TODO: Do we support multiple sessions in HA mode?

        Collection<RulesExecutor> executors = rulesExecutorContainer.getAllExecutors();
        if (executors.isEmpty()) {
            // No-op if no sessions exist yet
            return;
        }
        // Assume single session for now
        RulesExecutor executor = executors.iterator().next();

        if (!(executor instanceof HARulesExecutor)) {
            throw new IllegalStateException("Expected HARulesExecutor in HA mode");
        }

        restoreOrCreateSessionStateAsLeader(executor);
    }

    private void restoreOrCreateSessionStateAsLeader(RulesExecutor executor) {
        if (!haStateManager.isLeader()) {
            throw new IllegalStateException("This method should only be called by the leader");
        }

        // TODO: verify if the SessionState is the latest
        SessionState retrievedSessionState = haStateManager.getSessionState();
        if (retrievedSessionState == null) {
            // The first creation of SessionState
            // TODO: Confirm the spec that the current session is clean (nothing processed yet) at the moment
            SessionState sessionState = new SessionState();
            sessionState.setHaUuid(haStateManager.getHaUuid());
            sessionState.setPartialEvents(List.of());
            long currentTime = executor.asKieSession().getSessionClock().getCurrentTime();
            sessionState.setCreatedTime(currentTime);
            sessionState.setPersistedTime(currentTime);
            haStateManager.persistSessionState(sessionState);
        } else {
            // TODO: At the moment, we assume that the new leader's session is the same as the retrieved session state, so no need to restore
            return;

//            // restore session using SessionState
//            // TODO: compare SessionState with the current session. If the same, no need to restore. Typically, this is more likely the case
//            RulesExecutor recoveredRulesExecutor = haStateManager.recoverSession(executor.getRulesSet(), retrievedSessionState);
//            // replace the existing executor with the recovered one
//            rulesExecutorContainer.dispose(executor.getId());
//            rulesExecutorContainer.register(recoveredRulesExecutor);
//            // TODO: notify the new sessionId to Python side?
        }
    }

    private RulesExecutor createHARulesExecutorWithSessionState(RulesSet rulesSet, String rulesetString) {
        if (rulesetString == null) {
            throw new IllegalStateException("null rulesetString is not allowed in HA mode");
        }

        // TODO: verify if the SessionState is the latest
        SessionState retrievedSessionState = haStateManager.getSessionState();
        if (retrievedSessionState == null) {
            // No existing SessionState , create a new RulesExecutor
            RulesExecutor executor = rulesExecutorContainer.register(HARulesExecutorFactory.createRulesExecutor(rulesSet));

            if (haStateManager.isLeader()) {
                // The first creation of SessionState
                SessionState sessionState = new SessionState();
                sessionState.setHaUuid(haStateManager.getHaUuid());
                sessionState.setPartialEvents(List.of());
                long currentTime = executor.asKieSession().getSessionClock().getCurrentTime();
                sessionState.setCreatedTime(currentTime);
                sessionState.setPersistedTime(currentTime);

                sessionState.setRulebookHash(sha256(rulesetString));
                sessionState.setCurrentStateSHA(sessionState.getRulebookHash()); // the base SHA is the rulebook hash

                haStateManager.persistSessionState(sessionState);
            }
            return executor;
        } else {
            // restore session using SessionState. Assume the SessionState is correct one, so no need to recompute/verify
            return haStateManager.recoverSession(rulesSet, retrievedSessionState);
        }
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
     * Called by Python: self._api.addActionInfo(session, matching_uuid, index, action)
     */
    public void addActionInfo(long sessionId, String matchingUuid, int index, String action) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        haStateManager.addActionInfo(matchingUuid, index, action);
        logger.debug("Added action at index {} for ME UUID: {}", index, matchingUuid);
    }
    
    /**
     * Update an existing action
     * Called by Python: self._api.updateActionInfo(session, matching_uuid, index, action)
     */
    public void updateActionInfo(long sessionId, String matchingUuid, int index, String action) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        haStateManager.updateActionInfo(matchingUuid, index, action);
        logger.debug("Updated action at index {} for ME UUID: {}", index, matchingUuid);
    }
    
    /**
     * Check if an action exists
     * Called by Python: self._api.actionInfoExists(session, matching_uuid, index)
     */
    public boolean actionInfoExists(long sessionId, String matchingUuid, int index) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        return haStateManager.actionInfoExists(matchingUuid, index);
    }
    
    /**
     * Get an action by index
     * Called by Python: self._api.getActionInfo(session, matching_uuid, index)
     */
    public String getActionInfo(long sessionId, String matchingUuid, int index) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        return haStateManager.getActionInfo(matchingUuid, index);
    }
    
    /**
     * Delete all actions and matching events for a matching UUID
     * Called by Python: self._api.deleteActionInfo(session, matching_uuid)
     */
    public void deleteActionInfo(long sessionId, String matchingUuid) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
        
        haStateManager.deleteActionInfo(matchingUuid);
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

        for (RulesExecutor executor : rulesExecutorContainer.getAllExecutors()) {
            List<MatchingEvent> pendingEvents = haStateManager.getPendingMatchingEvents();
            
            if (!pendingEvents.isEmpty()) {
                logger.info("Found {} pending matching events for session {} : {}",
                           pendingEvents.size(), executor.getId(), executor.getRuleSetName());
                
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
                recoveryData.put("event_data", eventData);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse event data JSON for recovery", e);
            recoveryData.put("event_data", new HashMap<>());
        }
        
        // Send through async channel
        Response response = new Response(sessionId, recoveryData);
        rulesExecutorContainer.getChannel().write(response);
        
        logger.info("Sent ME recovery notification for UUID: {} on session: {}", 
                   matchingEvent.getMeUuid(), sessionId);
    }
}

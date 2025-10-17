package org.drools.ansible.rulebook.integration.core.jpy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
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
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.ansible.rulebook.integration.ha.model.SessionStateLite;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.calculateStateSHA;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.populateHAMatchResponse;
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
        List<Match> matches = rulesExecutorContainer.get(sessionId).processFacts(serializedFact).join();

        if (haMode && haStateManager != null) {
            if (haStateManager.isLeader()) {
                return processFactResponseHA(sessionId, matches, serializedFact);
            } else {
                updateSessionStateLiteAfterProcessing(sessionId, HAUtils.sha256(serializedFact));
            }
        }

        return matchesToJson(matches); // TODO: HA response format for non-leader? how about meUUID?
    }

    public String assertEvent(long sessionId, String serializedFact) {
        RulesExecutor executor = rulesExecutorContainer.get(sessionId);
        List<Match> matches = executor.processEvents(serializedFact).join();

        if (haMode && haStateManager != null) {
            if (haStateManager.isLeader()) {
                return processEventResponseHA(sessionId, matches);
            } else if (executor instanceof HARulesExecutor harRulesExecutor) {
                updateSessionStateLiteAfterProcessing(sessionId, harRulesExecutor.getHaSessionContext().getCurrentIdentifier());
            }
        }

        return matchesToJson(matches); // TODO: HA response format for non-leader? how about meUUID?
    }

    private String processEventResponseHA(long sessionId, List<Match> matches) {
        // matches is the internal representation of drools matches
        // matchList is the simplified Map structure for JSON-serialization
        List<Map<String, Map<String, Object>>> matchList = RuleMatch.asList(matches);

        try {
            HARulesExecutor rulesExecutor = (HARulesExecutor) rulesExecutorContainer.get(sessionId);

            String rulesetName = rulesExecutor.getRulesSet().getName();
            SessionState sessionState = haStateManager.getSessionState(rulesetName);

            HASessionContext haSessionContext = rulesExecutor.getHaSessionContext();
            LinkedHashMap<String, EventRecord> recordsInMemory = haSessionContext.getEventUuidsInMemory();
            sessionState.setPartialEvents(new ArrayList<>(recordsInMemory.values()));
            sessionState.setPersistedTime(rulesExecutor.asKieSession().getSessionClock().getCurrentTime());

            applyShaAdvance(sessionState, haSessionContext.getCurrentIdentifier());
            sessionState.setLastProcessedEventUuid(haSessionContext.getCurrentIdentifier());

            haStateManager.persistSessionState(sessionState);

            HAStats haStats = haStateManager.getHAStats();
            haStats.incrementEventsProcessed(); // TODO: increment by number of events if batch processing
            haStateManager.persistHAStats();
        } catch (Exception e) {
            logger.warn("Failed to persist event state for HA statistics", e);
        }

        return buildHaResponse(sessionId, matchList);
    }

    private String processFactResponseHA(long sessionId, List<Match> matches, String serializedFact) {
        List<Map<String, Map<String, Object>>> matchList = RuleMatch.asList(matches);

        try {
            HARulesExecutor rulesExecutor = (HARulesExecutor) rulesExecutorContainer.get(sessionId);

            String rulesetName = rulesExecutor.getRulesSet().getName();
            SessionState sessionState = haStateManager.getSessionState(rulesetName);

            HASessionContext haSessionContext = rulesExecutor.getHaSessionContext();
            LinkedHashMap<String, EventRecord> recordsInMemory = haSessionContext.getEventUuidsInMemory();
            sessionState.setPartialEvents(new ArrayList<>(recordsInMemory.values()));
            sessionState.setPersistedTime(rulesExecutor.asKieSession().getSessionClock().getCurrentTime());

            String factIdentifier = HAUtils.sha256(serializedFact);
            applyShaAdvance(sessionState, factIdentifier);
            sessionState.setLastProcessedEventUuid(factIdentifier);

            haStateManager.persistSessionState(sessionState);

            HAStats haStats = haStateManager.getHAStats();
            haStats.incrementEventsProcessed();
            haStateManager.persistHAStats();
        } catch (Exception e) {
            logger.warn("Failed to persist fact state for HA statistics", e);
        }

        return buildHaResponse(sessionId, matchList);
    }

    private void applyShaAdvance(SessionState sessionState, String processedIdentifier) {
        if (processedIdentifier == null) {
            return;
        }
        String currentStateSHA = sessionState.getCurrentStateSHA();
        sessionState.setPreviousStateSHA(currentStateSHA);
        sessionState.setCurrentStateSHA(calculateStateSHA(currentStateSHA, processedIdentifier));
    }

    private String buildHaResponse(long sessionId, List<Map<String, Map<String, Object>>> matchList) {
        if (matchList.isEmpty()) {
            return toJson(matchList);
        }

        try {
            String rulesetName = rulesExecutorContainer.get(sessionId).getRuleSetName();
            List<Map<String, Object>> haMatches = new ArrayList<>(matchList.size());
            for (Map<String, Map<String, Object>> matchData : matchList) {
                String ruleName = matchData.keySet().iterator().next();
                Map<String, Object> eventData = matchData.get(ruleName);

                MatchingEvent me = new MatchingEvent();
                me.setHaUuid(haStateManager.getHaUuid());
                me.setRuleSetName(rulesetName);
                me.setRuleName(ruleName);
                me.setEventData(toJson(eventData));

                String meUuid = haStateManager.addMatchingEvent(me);

                Map<String, Object> resultEntry = new LinkedHashMap<>();
                populateHAMatchResponse(resultEntry, ruleName, eventData, meUuid);
                haMatches.add(resultEntry);
            }
            return toJson(haMatches);
        } catch (Exception e) {
            logger.error("Failed to build HA response payload", e);
            return toJson(matchList);
        }
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

    private boolean rulebookHashMismatch(String rulesetName, SessionStateLite localLite, SessionState persistedState) {
        String persistedHash = persistedState.getRulebookHash();
        String localHash = localLite == null ? null : localLite.getRulebookHash();
        if (persistedHash == null || localHash == null) {
            return false;
        }
        if (!persistedHash.equals(localHash)) {
            logger.warn("Rulebook hash mismatch detected for {} (local {}, persisted {}); Make sure the rulebook is identical across HA nodes.",
                    rulesetName, localHash, persistedHash);
            return true;
        }
        return false;
    }

    private void updateSessionStateLiteAfterProcessing(long sessionId, String processedIdentifier) {
        if (!haMode || haStateManager == null || haStateManager.isLeader() || processedIdentifier == null) {
            return;
        }

        RulesExecutor executor = rulesExecutorContainer.get(sessionId);
        String rulesetName = executor.getRuleSetName();

        SessionStateLite sessionStateLite = haStateManager.getSessionStateLite(rulesetName);
        if (sessionStateLite == null || sessionStateLite.getCurrentStateSHA() == null) {
            // Nothing to advance yet
            return;
        }

        String nextSha = calculateStateSHA(sessionStateLite.getCurrentStateSHA(), processedIdentifier);
        haStateManager.registerSessionStateLite(rulesetName, new SessionStateLite(sessionStateLite.getRulebookHash(), nextSha, processedIdentifier));
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

        String rulesetName = executor.getRuleSetName();
        SessionState retrievedSessionState = haStateManager.getSessionState(rulesetName);
        if (retrievedSessionState == null) {
            SessionStateLite sessionStateLite = haStateManager.getSessionStateLite(rulesetName);
            if (sessionStateLite == null) {
                throw new IllegalStateException("Missing local SessionStateLite for ruleset " + rulesetName + " while promoting to leader");
            }
            if (sessionStateLite.getRulebookHash() == null) {
                throw new IllegalStateException("Missing rulebook hash for ruleset " + rulesetName + " while promoting to leader");
            }
            String rulebookHash = sessionStateLite.getRulebookHash();

            // The first creation of SessionState
            // TODO: Confirm the spec that the current session is clean (nothing processed yet) at the moment
            SessionState sessionState = new SessionState();
            sessionState.setHaUuid(haStateManager.getHaUuid());
            sessionState.setRuleSetName(((HARulesExecutor) executor).getRulesSet().getName());
            sessionState.setPartialEvents(List.of());
            long currentTime = executor.asKieSession().getSessionClock().getCurrentTime();
            sessionState.setCreatedTime(currentTime);
            sessionState.setPersistedTime(currentTime);
            sessionState.setRulebookHash(rulebookHash);
            sessionState.setCurrentStateSHA(rulebookHash); // the base SHA is the rulebook hash
            haStateManager.persistSessionState(sessionState);
        } else {
            SessionStateLite localLite = haStateManager.getSessionStateLite(rulesetName);
            if (localLite == null) {
                throw new IllegalStateException("Missing local SessionStateLite for ruleset " + rulesetName + " while promoting to leader");
            }
            if (rulebookHashMismatch(rulesetName, localLite, retrievedSessionState)) {
                throw new IllegalStateException("Rulebook hash mismatch detected for " + rulesetName + " while promoting to leader");
            }

            boolean mismatch = false;
            if (!Objects.equals(retrievedSessionState.getLastProcessedEventUuid(), localLite.getLastProcessedEventUuid())) {
                // This might be caused by timing issue. The leader crushes, but the follower has processed the event(s).
                // TODO: We can improve the root cause analysis by adding event count in SessionState and SessionStateLite
                logger.warn("Last processed event UUID mismatch detected for {} (local {}, persisted {}); Possible data loss or divergence.",
                        rulesetName, localLite.getLastProcessedEventUuid(), retrievedSessionState.getLastProcessedEventUuid());
                mismatch = true;
            }
            if (!Objects.equals(retrievedSessionState.getCurrentStateSHA(), localLite.getCurrentStateSHA())) {
                // This might be caused by timing issue. The leader crushes, but the follower has processed the event(s).
                // TODO: We can indentify a valid scenario by checking if the retrievedSessionState's previousStateSHA matches localLite's currentStateSHA (vise versa)
                logger.warn("Current state SHA mismatch detected for {} (local {}, persisted {}); Possible data loss or divergence.",
                        rulesetName, localLite.getCurrentStateSHA(), retrievedSessionState.getCurrentStateSHA());
                mismatch = true;
            }

            if (mismatch) {
                RulesExecutor recoveredRulesExecutor = haStateManager.recoverSession(((HARulesExecutor) executor).getRulesSet(), retrievedSessionState);
                long previousId = executor.getId();
                RulesExecutor removed = rulesExecutorContainer.removeExecutor(previousId);
                if (removed != null) {
                    removed.dispose();
                }
                // TODO: Review this approach. Now we keep the same session ID for continuity.
                // Other possible approaches:
                // A) Return a Map containing previous ID - new ID mappings
                // B) Send the previous ID - new ID mappings to Python layer via async channel
                rulesExecutorContainer.registerWithId(previousId, recoveredRulesExecutor);
                executor = recoveredRulesExecutor;
                logger.info("Recovered session {} from persisted SessionState version {}", rulesetName, retrievedSessionState.getVersion());
            } else {
                logger.info("No recovery needed for session {} upon leader promotion", rulesetName);
            }

            haStateManager.registerSessionStateLite(rulesetName,
                    new SessionStateLite(retrievedSessionState.getRulebookHash(),
                                         retrievedSessionState.getCurrentStateSHA(),
                                         retrievedSessionState.getLastProcessedEventUuid()));
        }
    }

    private RulesExecutor createHARulesExecutorWithSessionState(RulesSet rulesSet, String rulesetString) {
        if (rulesetString == null) {
            throw new IllegalStateException("null rulesetString is not allowed in HA mode");
        }

        // TODO: verify if the SessionState is the latest
        SessionState retrievedSessionState = haStateManager.getSessionState(rulesSet.getName());
        if (retrievedSessionState == null) {
            // No existing SessionState , create a new RulesExecutor
            RulesExecutor executor = rulesExecutorContainer.register(HARulesExecutorFactory.createRulesExecutor(rulesSet));

            String rulebookHash = sha256(rulesetString);

            if (haStateManager.isLeader()) {
                // The first creation of SessionState
                SessionState sessionState = new SessionState();
                sessionState.setHaUuid(haStateManager.getHaUuid());
                sessionState.setRuleSetName(rulesSet.getName());
                sessionState.setPartialEvents(List.of());
                long currentTime = executor.asKieSession().getSessionClock().getCurrentTime();
                sessionState.setCreatedTime(currentTime);
                sessionState.setPersistedTime(currentTime);
                sessionState.setRulebookHash(rulebookHash);
                sessionState.setCurrentStateSHA(rulebookHash); // the base SHA is the rulebook hash

                haStateManager.persistSessionState(sessionState);
            } else {
                haStateManager.registerSessionStateLite(rulesSet.getName(), new SessionStateLite(rulebookHash, rulebookHash, null));
            }
            return executor;
        } else {
            // restore session using SessionState. Assume the SessionState is correct one, so no need to recompute/verify
            RulesExecutor recoveredExecutor = haStateManager.recoverSession(rulesSet, retrievedSessionState);
            if (!haStateManager.isLeader()) {
                haStateManager.registerSessionStateLite(rulesSet.getName(),
                        new SessionStateLite(retrievedSessionState.getRulebookHash(),
                                             retrievedSessionState.getCurrentStateSHA(),
                                             retrievedSessionState.getLastProcessedEventUuid()));
            }
            return recoveredExecutor;
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
     * Get the stored status for an action
     * Called by Python: self._api.getActionStatus(session, matching_uuid, index)
     */
    public String getActionStatus(long sessionId, String matchingUuid, int index) {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }

        return haStateManager.getActionStatus(matchingUuid, index);
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

        // Parse event data JSON back to object for compatibility
        Map<String, Object> eventData = new HashMap<>();
        try {
            if (matchingEvent.getEventData() != null) {
                eventData = readValueAsMapOfStringAndObject(matchingEvent.getEventData());
            }
        } catch (Exception e) {
            logger.warn("Failed to parse event data JSON for recovery", e);
            // TBD: Should throw RuntimeException here?
        }

        // Create recovery payload with ME UUID
        Map<String, Object> recoveryData = new HashMap<>();
        populateHAMatchResponse(recoveryData,
                                matchingEvent.getRuleName(),
                                eventData,
                                matchingEvent.getMeUuid()); // these 3 fields are conformed to HA match response format
        recoveryData.put("type", "MATCHING_EVENT_RECOVERY");
        recoveryData.put("ruleset_name", matchingEvent.getRuleSetName());
        
        // Send through async channel
        Response response = new Response(sessionId, recoveryData);
        rulesExecutorContainer.getChannel().write(response);
        
        logger.info("Sent ME recovery notification for UUID: {} on session: {}", 
                   matchingEvent.getMeUuid(), sessionId);
    }
}

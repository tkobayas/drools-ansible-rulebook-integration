package org.drools.ansible.rulebook.integration.ha.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.domain.temporal.OnceAbstractTimeConstraint;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord.RecordType;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.kie.api.prototype.PrototypeEventInstance;
import org.drools.core.time.impl.PseudoClockScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHAStateManager implements HAStateManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHAStateManager.class);

    private final Map<String, SessionState> sessionStateMap = new HashMap<>();

    @Override
    public RulesExecutor recoverSession(RulesSet rulesSet, SessionState sessionState) {
        return HARulesExecutorFactory.createRulesExecutorWithRecovery(rulesSet, rulesExecutor -> {
            // Replay events to bring session up-to-date
            long currentTimeAtNewNode = ((PseudoClockScheduler) rulesExecutor.asKieSession().getSessionClock()).getCurrentTime();
            ((PseudoClockScheduler) rulesExecutor.asKieSession().getSessionClock()).setStartupTime(sessionState.getCreatedTime());
            long currentTime = sessionState.getCreatedTime();
            List<EventRecord> partialEvents = sessionState.getPartialEvents();
            for (EventRecord eventRecord : partialEvents) {
                rulesExecutor.advanceTime(eventRecord.getInsertedAt() - currentTime, java.util.concurrent.TimeUnit.MILLISECONDS);

                RecordType recordType = eventRecord.getRecordType();
                if (recordType == RecordType.CONTROL_ONCE_WITHIN || recordType == RecordType.CONTROL_ACCUMULATE_WITHIN) {
                    // Directly insert control event with proper prototype and expiration
                    Map<String, Object> eventData = JsonMapper.readValueAsMapOfStringAndObject(eventRecord.getEventJson());
                    PrototypeEventInstance controlEvent = OnceAbstractTimeConstraint.recreateControlEvent(
                            eventData, eventRecord.getExpirationDuration());
                    rulesExecutor.asKieSession().insert(controlEvent);
                    LOG.debug("Recovered control event at time {}, expiration duration: {} ms", eventRecord.getInsertedAt(), eventRecord.getExpirationDuration());
                } else if (recordType == RecordType.FACT) {
                    rulesExecutor.processFacts(eventRecord.getEventJson());
                } else {
                    // RecordType.EVENT
                    rulesExecutor.processEvents(eventRecord.getEventJson());
                }
                currentTime = eventRecord.getInsertedAt();
            }
            rulesExecutor.advanceTime(sessionState.getPersistedTime() - currentTime, java.util.concurrent.TimeUnit.MILLISECONDS);
            // TODO: Do we need to consider clock drift between nodes?
            if (currentTimeAtNewNode > sessionState.getPersistedTime()) {
                rulesExecutor.advanceTime(currentTimeAtNewNode - sessionState.getPersistedTime(), java.util.concurrent.TimeUnit.MILLISECONDS);
            }
        });
    }

    @Override
    public void registerSessionState(String ruleSetName, SessionState sessionState) {
        sessionStateMap.put(ruleSetName, sessionState);
    }

    @Override
    public SessionState getInMemorySessionState(String ruleSetName) {
        return sessionStateMap.get(ruleSetName);
    }

    /**
     * Verify the integrity of a loaded SessionState.
     * Compares stored SHA with recalculated SHA to detect corruption/tampering.
     *
     * @param sessionState The state loaded from persistence
     * @return true if integrity check passes, false if corruption detected
     */
    protected boolean verifyStateIntegrity(SessionState sessionState) {
        if (sessionState == null) {
            return false;
        }

        String storedSHA = sessionState.getCurrentStateSHA();
        if (storedSHA == null) {
            LOG.warn("SessionState has no SHA - cannot verify integrity for {}", sessionState.getRuleSetName());
            return true;  // Allow states without SHA (e.g., old persisted states before this feature)
        }

        // Recalculate SHA from content
        String recalculatedSHA = HAUtils.calculateStateSHA(sessionState);

        boolean valid = storedSHA.equals(recalculatedSHA);

        if (!valid) {
            LOG.error("SessionState integrity check FAILED! Stored SHA: {}, Recalculated SHA: {}",
                      storedSHA, recalculatedSHA);
            LOG.error("SessionState may be corrupted or tampered. RuleSetName: {}, HaUuid: {}, Version: {}",
                      sessionState.getRuleSetName(), sessionState.getHaUuid(), sessionState.getVersion());
        } else {
            LOG.debug("SessionState integrity check passed for {}", sessionState.getRuleSetName());
        }

        return valid;
    }

}

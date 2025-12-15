package org.drools.ansible.rulebook.integration.ha.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord.RecordType;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.kie.api.prototype.PrototypeEventInstance;
import org.drools.core.time.impl.PseudoClockScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.domain.temporal.OnceAbstractTimeConstraint.recreateControlEvent;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototypeEvent;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.normalizeControlEventData;

public abstract class AbstractHAStateManager implements HAStateManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHAStateManager.class);

    private final Map<String, SessionState> sessionStateMap = new HashMap<>();

    @Override
    public RulesExecutor recoverSession(String rulesetString, SessionState sessionState, long currentTimeAtNewNode) {
        return HARulesExecutorFactory.createRulesExecutorWithRecovery(rulesetString, rulesExecutor -> {
            // Replay events to bring session up-to-date
            ((PseudoClockScheduler) rulesExecutor.asKieSession().getSessionClock()).setStartupTime(sessionState.getCreatedTime());
            long currentTime = sessionState.getCreatedTime();
            List<EventRecord> partialEvents = sessionState.getPartialEvents();
            for (EventRecord eventRecord : partialEvents) {
                rulesExecutor.advanceTime(eventRecord.getInsertedAt() - currentTime, java.util.concurrent.TimeUnit.MILLISECONDS);
                RecordType recordType = eventRecord.getRecordType();
                if (recordType.isSynthetic()) {
                    // How time constraints are handled??
                    // OnceWithin : If a control event exists (= not yet expired), an event is discarded without firing the rule. [on recover]-> Inserting the control event back is sufficient.
                    // AggregateWithin : A control event holds the number of events. Events are discarded until the threshold is met. [on recover] -> Inserting the control event back is sufficient.
                    // TimeWindow : No control event. [on recover] -> Nothing to do.
                    Map<String, Object> eventData = normalizeControlEventData(JsonMapper.readValueAsMapOfStringAndObject(eventRecord.getEventJson()));
                    PrototypeEventInstance controlEvent = recreateControlEvent(eventData, eventRecord.getExpirationDuration());
                    rulesExecutor.asKieSession().insert(controlEvent);
                    if (eventRecord.getExpirationDuration() != Long.MAX_VALUE) {
                        LOG.debug("  * Recovered control event at time {}, expiration duration: {} ms : {}", eventRecord.getInsertedAt(), eventRecord.getExpirationDuration(), controlEvent);
                    } else {
                        LOG.debug("  * Recovered control event at time {}, no expiration : {}", eventRecord.getInsertedAt(), controlEvent);
                    }
                } else if (recordType == RecordType.FACT) {
                    LOG.debug("  * Replaying fact event at time {}: {}", eventRecord.getInsertedAt(), eventRecord.getEventJson());
                    rulesExecutor.processFacts(eventRecord.getEventJson());
                } else {
                    // RecordType.EVENT
                    LOG.debug("  * Replaying event at time {}: {}", eventRecord.getInsertedAt(), eventRecord.getEventJson());
                    rulesExecutor.processEvents(eventRecord.getEventJson());
                }
                currentTime = eventRecord.getInsertedAt();
            }
            rulesExecutor.advanceTime(sessionState.getPersistedTime() - currentTime, java.util.concurrent.TimeUnit.MILLISECONDS);
            // TODO: Do we need to consider clock drift between nodes?
            if (currentTimeAtNewNode > sessionState.getPersistedTime()) {
                LOG.debug("  Advancing recovered session clock from persisted time to current node time");
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
     * Counts the total number of partial events stored in memory across all session (= rule set) states.
     */
    protected int countPartialEventsInMemory() {
        return sessionStateMap.values().stream()
                .mapToInt(state -> {
                    List<EventRecord> partialEvents = state.getPartialEvents();
                    return partialEvents == null ? 0 : partialEvents.size();
                })
                .sum();
    }

    @Override
    public boolean verifySessionState(SessionState sessionState) {
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

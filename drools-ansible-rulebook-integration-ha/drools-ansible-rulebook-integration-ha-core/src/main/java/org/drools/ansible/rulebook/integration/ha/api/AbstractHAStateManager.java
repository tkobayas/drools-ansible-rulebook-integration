package org.drools.ansible.rulebook.integration.ha.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord.RecordType;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.core.time.impl.PseudoClockScheduler;
import org.kie.api.prototype.PrototypeEventInstance;
import org.kie.api.prototype.PrototypeFactInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.domain.temporal.OnceAbstractTimeConstraint.recreateControlEvent;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.normalizeControlEventData;

public abstract class AbstractHAStateManager implements HAStateManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHAStateManager.class);

    protected static final String DROOLS_VERSION_KEY = "drools_version";
    public static final String DROOLS_VERSION = "ha-poc-0.0.7";

    private final Map<String, SessionState> sessionStateMap = new HashMap<>();

    private HAEncryption encryption; // null = disabled

    /**
     * Common initialization for cross-cutting concerns (encryption, future features).
     * Must be called by every subclass at the end of {@code initializeHA()}.
     */
    protected final void commonInit(Map<String, Object> config) {
        initializeEncryption(config);
    }

    private void initializeEncryption(Map<String, Object> config) {
        if (config == null) return;
        String primaryKey = (String) config.get("encryption_key_primary");
        String secondaryKey = (String) config.get("encryption_key_secondary");
        if (primaryKey != null && !primaryKey.isEmpty()) {
            this.encryption = new HAEncryption(primaryKey, secondaryKey);
            LOG.info("HA encryption enabled (primary key configured, secondary key {})",
                     secondaryKey != null && !secondaryKey.isEmpty() ? "configured" : "not configured");
        } else {
            LOG.info("HA encryption disabled (no encryption_key_primary in config)");
        }
    }

    protected String encryptIfEnabled(String plaintext) {
        if (encryption == null || plaintext == null || plaintext.isEmpty()) return plaintext;
        return encryption.encrypt(plaintext);
    }

    protected String decryptIfEnabled(String data) {
        if (data == null) return data;
        if (encryption == null) {
            if (HAEncryption.isEncrypted(data)) {
                throw new HAEncryptionException(
                        "FATAL: Encrypted data found in database but no encryption keys configured. "
                        + "Provide encryption_key_primary before restarting.");
            }
            return data;
        }
        HAEncryption.DecryptResult result = encryption.decrypt(data);
        if (result.usedSecondaryKey()) {
            LOG.info("Data decrypted with secondary key (will be re-encrypted with primary on next write)");
        }
        return result.plaintext();
    }

    protected void ensureVersionInMetadata(Map<String, Object> metadata) {
        metadata.put(DROOLS_VERSION_KEY, DROOLS_VERSION);
    }

    @Override
    public RulesExecutor recoverSession(String rulesetString, SessionState sessionState, long currentTimeAtNewNode) {
        return HARulesExecutorFactory.createRulesExecutorWithRecovery(rulesetString, rulesExecutor -> {
            // Replay events to bring session up-to-date
            ((PseudoClockScheduler) rulesExecutor.asKieSession().getSessionClock()).setStartupTime(sessionState.getCreatedTime());
            long currentTime = sessionState.getCreatedTime();
            List<EventRecord> partialEvents = sessionState.getPartialEvents();

            // Pre-scan: collect EVENT maps that are embedded inside CONTROL_TIMED_OUT records.
            // These user events must NOT be replayed via processEvents() (which would re-trigger pattern rules).
            // Instead they will be inserted directly into WM when their parent control is recovered.
            Set<Map<String, Object>> embeddedEventMaps = new HashSet<>();
            for (EventRecord er : partialEvents) {
                if (er.getRecordType() == RecordType.CONTROL_TIMED_OUT) {
                    Map<String, Object> controlData = JsonMapper.readValueAsMapOfStringAndObject(er.getEventJson());
                    Object embeddedEvent = controlData.get("event");
                    if (embeddedEvent instanceof Map) {
                        embeddedEventMaps.add((Map<String, Object>) embeddedEvent);
                    }
                }
            }

            for (EventRecord eventRecord : partialEvents) {
                rulesExecutor.advanceTime(eventRecord.getInsertedAt() - currentTime, java.util.concurrent.TimeUnit.MILLISECONDS);
                RecordType recordType = eventRecord.getRecordType();
                if (recordType == RecordType.CONTROL_TIMED_OUT) {
                    // TimedOut control events hold a reference to the original user event in their "event" property.
                    // Cleanup rules use `this == $c.event` requiring object identity.
                    // We insert the embedded user event into WM first, then the control event.
                    // The control's "event" property already points to the same PrototypeFactInstance object.
                    Map<String, Object> eventData = normalizeControlEventData(JsonMapper.readValueAsMapOfStringAndObject(eventRecord.getEventJson()));
                    PrototypeEventInstance controlEvent = recreateControlEvent(eventData, eventRecord.getExpirationDuration());
                    Object embeddedEvent = controlEvent.get("event");
                    if (embeddedEvent instanceof PrototypeFactInstance) {
                        rulesExecutor.asKieSession().insert(embeddedEvent);
                        LOG.debug("  * Inserted embedded user event for CONTROL_TIMED_OUT at time {}", eventRecord.getInsertedAt());
                    }
                    rulesExecutor.asKieSession().insert(controlEvent);
                    if (eventRecord.getExpirationDuration() != Long.MAX_VALUE) {
                        LOG.debug("  * Recovered CONTROL_TIMED_OUT at time {}, expiration duration: {} ms : {}", eventRecord.getInsertedAt(), eventRecord.getExpirationDuration(), controlEvent);
                    } else {
                        LOG.debug("  * Recovered CONTROL_TIMED_OUT at time {}, no expiration : {}", eventRecord.getInsertedAt(), controlEvent);
                    }
                } else if (recordType.isSynthetic()) {
                    // How time constraints are handled??
                    // OnceWithin : If a control event exists (= not yet expired), an event is discarded without firing the rule. [on recover]-> Inserting the control event back is sufficient.
                    // AggregateWithin : A control event holds the number of events. Events are discarded until the threshold is met. [on recover] -> Inserting the control event back is sufficient.
                    // TimeWindow : No control event. [on recover] -> Nothing to do.
                    // OnceAfter : A main control event holds nested events (main control event may be multiple because of group_by).
                    //             When its start control event expires and its end control event is present, the rule fires. [on recover] -> Inserting the all control events back is sufficient.
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
                    // RecordType.EVENT — skip if already embedded in a CONTROL_TIMED_OUT record
                    if (!embeddedEventMaps.isEmpty()) {
                        Map<String, Object> eventMap = JsonMapper.readValueAsMapOfStringAndObject(eventRecord.getEventJson());
                        if (embeddedEventMaps.contains(eventMap)) {
                            LOG.debug("  * Skipping EVENT at time {} (already embedded in CONTROL_TIMED_OUT): {}", eventRecord.getInsertedAt(), eventRecord.getEventJson());
                            currentTime = eventRecord.getInsertedAt();
                            continue;
                        }
                    }
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

            // Restore processed event IDs from persisted state for duplicate detection
            if (rulesExecutor instanceof HARulesExecutor haExecutor && sessionState.getProcessedEventIds() != null) {
                haExecutor.getHaSessionContext().setProcessedEventIds(sessionState.getProcessedEventIds());
                LOG.debug("  Restored {} processed event IDs from persisted state", sessionState.getProcessedEventIds().size());
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

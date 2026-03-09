package org.drools.ansible.rulebook.integration.ha.api;

import java.util.UUID;

import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeConstraint;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.core.common.InternalFactHandle;
import org.kie.api.event.rule.DefaultRuleRuntimeEventListener;
import org.kie.api.event.rule.ObjectDeletedEvent;
import org.kie.api.event.rule.ObjectInsertedEvent;
import org.kie.api.event.rule.ObjectUpdatedEvent;
import org.kie.api.prototype.PrototypeEventInstance;
import org.kie.api.prototype.PrototypeFactInstance;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.getEventUuid;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.flattenPrototypeFact;

/**
 * KieSession event listener that tracks all fact/event insertions, deletions, and updates for HA persistence.
 *
 * Inserted objects fall into two categories:
 * <ul>
 *   <li><b>User events/facts</b> — inserted by client code with a {@link HASessionContext.PendingRecord}
 *       that carries the original identifier and JSON.</li>
 *   <li><b>Synthetic control events</b> — inserted by temporal operators (OnceWithin, AccumulateWithin, etc.)
 *       without a PendingRecord; tracked automatically.</li>
 * </ul>
 */
public class HARuleRuntimeEventListener extends DefaultRuleRuntimeEventListener {

    private static final Logger logger = LoggerFactory.getLogger(HARuleRuntimeEventListener.class);

    private final HASessionContext haSessionContext;
    private final KieSession kieSession;

    public HARuleRuntimeEventListener(HASessionContext haSessionContext, KieSession kieSession) {
        this.haSessionContext = haSessionContext;
        this.kieSession = kieSession;
    }

    @Override
    public void objectInserted(ObjectInsertedEvent event) {
        try {
            PrototypeFactInstance protoFact = toPrototypeFactOrNull(event.getObject());
            if (protoFact == null) {
                return;
            }

            InternalFactHandle factHandle = (InternalFactHandle) event.getFactHandle();
            boolean isEvent = protoFact instanceof PrototypeEventInstance;
            boolean isSynthetic = isSyntheticControlEvent(protoFact);
            HASessionContext.PendingRecord pending = haSessionContext.consumePendingRecord();

            if (pending != null) {
                // user event/fact
                trackPendingInsertion(pending, protoFact, factHandle, isEvent, isSynthetic);
                return;
            }

            // control event or other synthetic insertion
            trackAutoInsertion(protoFact, factHandle, isEvent, isSynthetic);

        } catch (Exception e) {
            logger.warn("Failed to track insertion in HA context", e);
        }
    }

    /**
     * Tracks a user event/fact insertion that has an associated PendingRecord.
     * Throws if a synthetic control event is inserted while a PendingRecord is active.
     */
    private void trackPendingInsertion(HASessionContext.PendingRecord pending, PrototypeFactInstance protoFact,
                                       InternalFactHandle factHandle, boolean isEvent, boolean isSynthetic) {
        if (isSynthetic) {
            throw new IllegalStateException("Synthetic control events must not be inserted before processing user event/fact");
        }

        String identifier = pending.getIdentifier();

        if (isEvent) {
            String eventUuid = getEventUuid(factHandle).orElse(null);
            if (eventUuid != null && !eventUuid.equals(identifier)) {
                throw new IllegalStateException("user event/fact must be inserted first. Expected identifier " + identifier + " but got event UUID " + eventUuid);
            }
        }

        String json = pending.getJson();
        if (json == null) {
            json = JsonMapper.toJson(protoFact.asMap());
        }

        long timestamp = kieSession.getSessionClock().getCurrentTime();
        EventRecord record = new EventRecord(json, timestamp, pending.getType(), null);
        haSessionContext.addTrackedRecord(identifier, record, factHandle.getId());
    }

    /**
     * Tracks an auto-detected insertion (no PendingRecord).
     * This covers synthetic control events (OnceWithin, AccumulateWithin, etc.) and any other
     * PrototypeFactInstance insertions not initiated by client code.
     */
    private void trackAutoInsertion(PrototypeFactInstance protoFact, InternalFactHandle factHandle,
                                    boolean isEvent, boolean isSynthetic) {
        String json = JsonMapper.toJson(flattenPrototypeFact(protoFact));
        String identifier = isEvent ? getEventUuid(factHandle).orElse(UUID.randomUUID().toString()) : UUID.randomUUID().toString();
        EventRecord.RecordType recordType = isEvent ? EventRecord.RecordType.EVENT : EventRecord.RecordType.FACT;
        Long expirationDuration = null;

        if (isSynthetic) {
            String controlName = (String) protoFact.get(TimeConstraint.CONTROL_NAME);
            if (controlName == null) {
                logger.warn("Synthetic control event missing control_name; defaulting to EVENT tracking");
                recordType = EventRecord.RecordType.EVENT;
            } else {
                recordType = EventRecord.RecordType.getByControlName(controlName);
            }
            PrototypeEventInstance controlEvent = (PrototypeEventInstance) protoFact;
            expirationDuration = controlEvent.getExpiration();
            logger.debug("Tracking synthetic control event {} with identifier: {}", recordType, identifier);
            if (expirationDuration != Long.MAX_VALUE) {
                logger.debug("Control event expiration duration: {} ms", expirationDuration);
            }
        }

        long timestamp = kieSession.getSessionClock().getCurrentTime();
        EventRecord record = new EventRecord(json, timestamp, recordType, expirationDuration);
        haSessionContext.addTrackedRecord(identifier, record, factHandle.getId());
    }

    @Override
    public void objectDeleted(ObjectDeletedEvent event) {
        try {
            InternalFactHandle factHandle = (InternalFactHandle) event.getFactHandle();

            // Try to remove by event UUID first, then by fact handle ID
            getEventUuid(factHandle).ifPresentOrElse(
                    haSessionContext::removeTrackedRecord,
                    () -> haSessionContext.removeTrackedRecordByFactHandle(factHandle.getId())
            );

        } catch (Exception e) {
            logger.warn("Failed to track deletion in HA context", e);
        }
    }

    @Override
    public void objectUpdated(ObjectUpdatedEvent event) {
        try {
            PrototypeFactInstance protoFact = toPrototypeFactOrNull(event.getObject());
            if (protoFact == null || !isSyntheticControlEvent(protoFact)) {
                return;
            }

            InternalFactHandle factHandle = (InternalFactHandle) event.getFactHandle();
            String updatedJson = JsonMapper.toJson(flattenPrototypeFact(protoFact));

            // Only the JSON content changes on update; expirationDuration remains unchanged
            haSessionContext.updateTrackedRecordByFactHandle(factHandle.getId(), updatedJson);

            logger.debug("Updated control event with fact handle ID: {}", factHandle.getId());
            if (logger.isTraceEnabled()) {
                logger.trace("Updated control event JSON: {}", updatedJson);
            }
        } catch (Exception e) {
            logger.warn("Failed to track update in HA context", e);
        }
    }

    private static PrototypeFactInstance toPrototypeFactOrNull(Object object) {
        return (object instanceof PrototypeFactInstance) ? (PrototypeFactInstance) object : null;
    }

    private static boolean isSyntheticControlEvent(PrototypeFactInstance protoFact) {
        return SYNTHETIC_PROTOTYPE_NAME.equals(protoFact.getPrototype().getName());
    }
}

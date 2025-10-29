package org.drools.ansible.rulebook.integration.ha.api;

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
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256;

/**
 * KieSession event listener that tracks all fact/event insertions and deletions for HA persistence.
 * This includes user events/facts AND control events (e.g., OnceWithin synthetic events).
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
            Object object = event.getObject();
            InternalFactHandle factHandle = (InternalFactHandle) event.getFactHandle();

            // Only track PrototypeFact/EventInstances (user events/facts and control events)
            if (!(object instanceof PrototypeFactInstance)) {
                return;
            }

            PrototypeFactInstance protoFact = (PrototypeFactInstance) object;
            boolean isEvent = object instanceof PrototypeEventInstance;
            boolean isSyntheticControlEvent = SYNTHETIC_PROTOTYPE_NAME.equals(protoFact.getPrototype().getName());

            // Check if this insertion already has a PendingRecord (user event/fact)
            HASessionContext.PendingRecord pending = haSessionContext.consumePendingRecord();

            String identifier;
            String json;
            EventRecord.RecordType recordType;
            Long expirationDuration = null;

            if (pending != null) {
                if (isSyntheticControlEvent) {
                    throw new IllegalStateException("Synthetic control events must not be inserted before processing user event/fact" );
                }

                // User event/fact: use PendingRecord info
                identifier = pending.getIdentifier();
                json = pending.getJson();
                recordType = pending.getType();

                if (isEvent) {
                    String eventUuid = getEventUuid(factHandle).orElse(null);
                    if (eventUuid != null && !eventUuid.equals(identifier)) {
                        throw new IllegalStateException("user event/fact must be inserted first. Expected identifier " + identifier + " but got event UUID " + eventUuid);
                    }
                }
                if (json == null) {
                    json = JsonMapper.toJson(protoFact.asMap());
                }
            } else {
                // Control event or other synthetic insertion: auto-generate tracking info
                json = JsonMapper.toJson(protoFact.asMap());
                identifier = isEvent ? getEventUuid(factHandle).orElse(sha256(json)) : sha256(json);
                recordType = isEvent ? EventRecord.RecordType.EVENT : EventRecord.RecordType.FACT;

                if (isSyntheticControlEvent) {
                    recordType = EventRecord.RecordType.getByControlName((String) protoFact.get(TimeConstraint.CONTROL_NAME));
                    PrototypeEventInstance controlEvent = (PrototypeEventInstance) object;
                    expirationDuration = controlEvent.getExpiration();
                    logger.debug("Tracking synthetic control event {} with identifier: {}", recordType, identifier);
                    logger.debug("Control event expiration duration: {} ms", expirationDuration);
                }
            }

            long timestamp = kieSession.getSessionClock().getCurrentTime();
            EventRecord record = new EventRecord(json, timestamp, recordType, expirationDuration);
            haSessionContext.addTrackedRecord(identifier, record, factHandle.getId());

        } catch (Exception e) {
            logger.warn("Failed to track insertion in HA context", e);
        }
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
            Object object = event.getObject();
            InternalFactHandle factHandle = (InternalFactHandle) event.getFactHandle();

            // Only track PrototypeFactInstance updates
            if (!(object instanceof PrototypeFactInstance)) {
                return;
            }

            PrototypeFactInstance protoFact = (PrototypeFactInstance) object;
            boolean isSyntheticControlEvent = SYNTHETIC_PROTOTYPE_NAME.equals(protoFact.getPrototype().getName());

            // Only track control event updates (e.g., AccumulateWithin increments current_count)
            if (isSyntheticControlEvent) {
                // Serialize updated control event
                String updatedJson = JsonMapper.toJson(protoFact.asMap());

                // Update the stored EventRecord with new state
                // Note: expirationDuration doesn't change on update, only the JSON content changes
                haSessionContext.updateTrackedRecordByFactHandle(factHandle.getId(), updatedJson);

                logger.debug("Updated control event with fact handle ID: {}", factHandle.getId());
                if (logger.isTraceEnabled()) {
                    logger.trace("Updated control event JSON: {}", updatedJson);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to track update in HA context", e);
        }
    }
}

package org.drools.ansible.rulebook.integration.ha.api;

import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeConstraint;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.core.common.InternalFactHandle;
import org.kie.api.event.rule.DefaultRuleRuntimeEventListener;
import org.kie.api.event.rule.ObjectDeletedEvent;
import org.kie.api.event.rule.ObjectInsertedEvent;
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
                // User event/fact: use PendingRecord info
                identifier = pending.getIdentifier();
                json = pending.getJson();
                recordType = pending.getType();

                if (isEvent) {
                    // Override with actual UUID from the inserted event if available
                    identifier = getEventUuid(factHandle).orElse(identifier);
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

            // Final fallback for identifier
            if (identifier == null) {
                identifier = sha256(json);
            }

            long timestamp = kieSession.getSessionClock().getCurrentTime();
            EventRecord record = new EventRecord(identifier, json, timestamp, recordType, expirationDuration);
            haSessionContext.addRecord(identifier, record, factHandle.getId());

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
                    haSessionContext::removeEventUuidInMemory,
                    () -> haSessionContext.removeRecordByFactHandle(factHandle.getId())
            );

        } catch (Exception e) {
            logger.warn("Failed to track deletion in HA context", e);
        }
    }
}

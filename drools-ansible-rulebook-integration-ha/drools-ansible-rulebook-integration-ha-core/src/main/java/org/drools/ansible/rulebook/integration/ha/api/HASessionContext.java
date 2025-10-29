package org.drools.ansible.rulebook.integration.ha.api;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HASessionContext {

    private static final Logger logger = LoggerFactory.getLogger(HASessionContext.class);

    // No concurrency expected, AbstractRulesEvaluator.atomicRuleEvaluation ensures synchronized access

    // Track events/facts existing in the session. Eventually persisted as SessionState.partialEvents
    private final LinkedHashMap<String, EventRecord> trackedRecords = new LinkedHashMap<>();

    // Associate factHandleIds to identifiers for efficient lookup during updates/deletions
    private final Map<Long, String> factHandleIndex = new HashMap<>();

    // Keep the incoming record from the client to preserve the original json (important for SHA generation) and pre-calculated identifier
    // Note: if we will not need to maintain SHA, we may remove this and directly create EventRecord on insertion
    private PendingRecord pendingRecord;

    public LinkedHashMap<String, EventRecord> getTrackedRecords() {
        return trackedRecords;
    }

    public void addTrackedRecord(String identifier, EventRecord record, Long factHandleId) {
        if (identifier == null || record == null) {
            return;
        }

        trackedRecords.put(identifier, record);
        if (factHandleId != null) {
            factHandleIndex.put(factHandleId, identifier);
        }
    }

    public void removeTrackedRecord(String identifier) {
        if (identifier == null) {
            return;
        }
        trackedRecords.remove(identifier);
    }

    public void removeTrackedRecordByFactHandle(long factHandleId) {
        String identifier = factHandleIndex.remove(factHandleId);
        if (identifier != null) {
            trackedRecords.remove(identifier);
        }
    }

    /**
     * Updates an existing EventRecord's JSON content.
     * Used when a control event is modified (e.g., AccumulateWithin increments current_count).
     *
     * @param factHandleId The fact handle ID of the object being updated
     * @param updatedJson The new JSON representation of the object
     */
    public void updateTrackedRecordByFactHandle(long factHandleId, String updatedJson) {
        String identifier = factHandleIndex.get(factHandleId);
        if (identifier != null) {
            EventRecord record = trackedRecords.get(identifier);
            if (record != null) {
                record.setEventJson(updatedJson);
                logger.debug("Updated EventRecord for identifier: {}, factHandleId: {}", identifier, factHandleId);
            } else {
                logger.warn("No EventRecord found for identifier: {} during update", identifier);
            }
        } else {
            logger.warn("No identifier mapping found for factHandleId: {} during update", factHandleId);
        }
    }

    public void preparePendingRecord(String identifier, String json, EventRecord.RecordType type) {
        pendingRecord = new PendingRecord(identifier, json, type);
    }

    public PendingRecord consumePendingRecord() {
        PendingRecord record = pendingRecord;
        pendingRecord = null;
        return record;
    }

    public static final class PendingRecord {
        private final String identifier;
        private final String json;
        private final EventRecord.RecordType type;

        PendingRecord(String identifier, String json, EventRecord.RecordType type) {
            this.identifier = identifier;
            this.json = json;
            this.type = type;
        }

        public String getIdentifier() {
            return identifier;
        }

        public String getJson() {
            return json;
        }

        public EventRecord.RecordType getType() {
            return type;
        }
    }
}

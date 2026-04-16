package org.drools.ansible.rulebook.integration.ha.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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

    // Circular buffer of processed event IDs for duplicate detection
    private final LinkedHashSet<String> processedEventIds = new LinkedHashSet<>();
    private static final int DEFAULT_MAX_PROCESSED_IDS = 5;
    private int maxProcessedIds = DEFAULT_MAX_PROCESSED_IDS;

    // Temporarily hold incoming event/fact metadata during insertion flow.
    // Preserves original JSON and identifier from client before Drools processing.
    // Used to distinguish user events/facts from synthetic control events.
    private PendingRecord pendingRecord;

    public LinkedHashMap<String, EventRecord> getTrackedRecords() {
        return trackedRecords;
    }

    public void addTrackedRecord(String identifier, EventRecord eventRecord, Long factHandleId) {
        if (identifier == null || eventRecord == null) {
            return;
        }

        trackedRecords.put(identifier, eventRecord);
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
            EventRecord eventRecord = trackedRecords.get(identifier);
            if (eventRecord != null) {
                eventRecord.setEventJson(updatedJson);
                logger.debug("Updated EventRecord for identifier: {}, factHandleId: {}", identifier, factHandleId);
            } else {
                logger.warn("No EventRecord found for identifier: {} during update", identifier);
            }
        } else {
            logger.warn("No identifier mapping found for factHandleId: {} during update", factHandleId);
        }
    }

    public void preparePendingRecord(String identifier, String json, EventRecord.RecordType type) {
        if (identifier == null || json == null || type == null) {
            throw new IllegalArgumentException("Invalid arguments passed to preparePendingRecord : " +
                                                       "identifier=" + identifier + ", json=" + json + ", type=" + type);
        }
        pendingRecord = new PendingRecord(identifier, json, type);
    }

    public PendingRecord consumePendingRecord() {
        PendingRecord theRecord = pendingRecord;
        pendingRecord = null;
        return theRecord;
    }

    public void setMaxProcessedIds(int maxProcessedIds) {
        this.maxProcessedIds = maxProcessedIds;
    }

    public boolean isAlreadyProcessed(String eventId) {
        return processedEventIds.contains(eventId);
    }

    public void recordProcessedEvent(String eventId) {
        processedEventIds.add(eventId);
        if (processedEventIds.size() > maxProcessedIds) {
            Iterator<String> it = processedEventIds.iterator();
            it.next();
            it.remove();
        }
    }

    public List<String> getProcessedEventIds() {
        return new ArrayList<>(processedEventIds);
    }

    public void setProcessedEventIds(List<String> ids) {
        processedEventIds.clear();
        if (ids != null) {
            processedEventIds.addAll(ids);
        }
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

package org.drools.ansible.rulebook.integration.ha.api;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.model.EventRecord;

public class HASessionContext {

    // TODO: consider thread-safety. processEvent is called by single client. But AutomaticPseudoClock can run on another thread

    private final LinkedHashMap<String, EventRecord> processedRecords = new LinkedHashMap<>();
    private final Map<Long, String> factHandleIndex = new HashMap<>();
    private PendingRecord pendingRecord;
    private String currentIdentifier;

    public LinkedHashMap<String, EventRecord> getProcessedRecords() {
        return processedRecords;
    }

    public LinkedHashMap<String, EventRecord> getEventUuidsInMemory() {
        return processedRecords;
    }

    // TODO: consider unifying method names
    public void addEventUuidInMemory(String uuid, EventRecord eventRecord) {
        addRecord(uuid, eventRecord, null);
    }

    public void addRecord(String identifier, EventRecord record, Long factHandleId) {
        if (identifier == null || record == null) {
            return;
        }

        processedRecords.put(identifier, record);
        if (factHandleId != null) {
            factHandleIndex.put(factHandleId, identifier);
        }
        currentIdentifier = identifier;
    }

    public void removeEventUuidInMemory(String uuid) {
        removeRecord(uuid);
    }

    public void removeRecord(String identifier) {
        if (identifier == null) {
            return;
        }
        processedRecords.remove(identifier);
    }

    public void removeRecordByFactHandle(long factHandleId) {
        String identifier = factHandleIndex.remove(factHandleId);
        if (identifier != null) {
            processedRecords.remove(identifier);
        }
    }

    public String getCurrentIdentifier() {
        return currentIdentifier;
    }

    public String getCurrentEventUuid() {
        return currentIdentifier;
    }

    public void preparePendingRecord(String identifier, String json, EventRecord.RecordType type) {
        pendingRecord = new PendingRecord(identifier, json, type);
        currentIdentifier = identifier;
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

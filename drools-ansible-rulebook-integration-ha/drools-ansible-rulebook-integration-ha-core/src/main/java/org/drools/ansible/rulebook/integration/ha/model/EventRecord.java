package org.drools.ansible.rulebook.integration.ha.model;

public class EventRecord {

    public enum RecordType {
        EVENT,
        FACT
    }

    private String eventUuid; // TODO: consider if we exclude this field for persistece for performance

    private String eventJson;

    private long insertedAt;

    private RecordType recordType = RecordType.EVENT;

    // TODO: Probably need to have ID and track the associated event to handle deletions

    public EventRecord() {
    }

    public EventRecord(String eventUuid, String eventJson, long insertedAt) {
        this.eventUuid = eventUuid;
        this.eventJson = eventJson;
        this.insertedAt = insertedAt;
    }

    public EventRecord(String eventUuid, String eventJson, long insertedAt, RecordType recordType) {
        this(eventUuid, eventJson, insertedAt);
        this.recordType = recordType;
    }

    public String getEventUuid() {
        return eventUuid;
    }

    public String getEventJson() {
        return eventJson;
    }

    public long getInsertedAt() {
        return insertedAt;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }
}

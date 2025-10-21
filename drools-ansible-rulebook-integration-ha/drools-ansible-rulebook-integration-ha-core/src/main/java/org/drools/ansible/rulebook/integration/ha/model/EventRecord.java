package org.drools.ansible.rulebook.integration.ha.model;

public class EventRecord {

    public enum RecordType {
        EVENT(false),
        FACT(false),
        CONTROL_ONCE_WITHIN(true);

        private final boolean synthetic;

        RecordType(boolean synthetic) {
            this.synthetic = synthetic;
        }

        public boolean isSynthetic() {
            return synthetic;
        }
    }

    private String eventUuid; // TODO: consider if we exclude this field for persistece for performance

    private String eventJson;

    private long insertedAt;

    private RecordType recordType = RecordType.EVENT;

    // For control events: expiration duration in milliseconds (not absolute time)
    private Long expirationDuration;

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

    public EventRecord(String eventUuid, String eventJson, long insertedAt, RecordType recordType, Long expirationDuration) {
        this(eventUuid, eventJson, insertedAt, recordType);
        this.expirationDuration = expirationDuration;
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

    public Long getExpirationDuration() {
        return expirationDuration;
    }

    public void setExpirationDuration(Long expirationDuration) {
        this.expirationDuration = expirationDuration;
    }
}

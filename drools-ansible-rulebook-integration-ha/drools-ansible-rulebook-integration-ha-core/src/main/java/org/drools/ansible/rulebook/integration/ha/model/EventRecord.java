package org.drools.ansible.rulebook.integration.ha.model;

public class EventRecord {

    private String eventUuid; // TODO: consider if we exclude this field for persistece for performance

    private String eventJson;

    private long insertedAt;

    // TODO: Probably need to have ID and track the associated event to handle deletions

    public EventRecord() {
    }

    public EventRecord(String eventUuid, String eventJson, long insertedAt) {
        this.eventUuid = eventUuid;
        this.eventJson = eventJson;
        this.insertedAt = insertedAt;
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
}

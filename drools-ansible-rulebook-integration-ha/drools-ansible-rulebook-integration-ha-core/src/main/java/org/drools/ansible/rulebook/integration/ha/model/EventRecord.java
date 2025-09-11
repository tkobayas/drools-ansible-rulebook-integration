package org.drools.ansible.rulebook.integration.ha.model;

public class EventRecord {

    private String eventJson;

    private long insertedAt;

    // TODO: Probably need to have ID and track the associated event to handle deletions

    public EventRecord() {
    }

    public EventRecord(String eventJson, long insertedAt) {
        this.eventJson = eventJson;
        this.insertedAt = insertedAt;
    }

    public String getEventJson() {
        return eventJson;
    }

    public void setEventJson(String eventJson) {
        this.eventJson = eventJson;
    }

    public long getInsertedAt() {
        return insertedAt;
    }

    public void setInsertedAt(long insertedAt) {
        this.insertedAt = insertedAt;
    }
}

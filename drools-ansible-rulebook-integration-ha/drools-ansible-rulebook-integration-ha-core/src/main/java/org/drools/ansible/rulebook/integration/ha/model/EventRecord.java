package org.drools.ansible.rulebook.integration.ha.model;

import org.drools.ansible.rulebook.integration.api.domain.temporal.AccumulateWithinDefinition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.OnceAfterDefinition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.OnceWithinDefinition;

public class EventRecord {

    public enum RecordType {
        EVENT(false, null),
        FACT(false, null),
        CONTROL_ONCE_WITHIN(true, OnceWithinDefinition.ONCE_WITHIN_CONTROL),
        CONTROL_ACCUMULATE_WITHIN(true, AccumulateWithinDefinition.ACCUMULATE_WITHIN_CONTROL),
        CONTROL_ONCE_AFTER(true, OnceAfterDefinition.ONCE_AFTER_CONTROL);

        private final boolean synthetic;

        private final String controlName;

        RecordType(boolean synthetic, String controlName) {
            this.synthetic = synthetic;
            this.controlName = controlName;
        }

        public boolean isSynthetic() {
            return synthetic;
        }

        public String getControlName() {
            return controlName;
        }

        public static RecordType getByControlName(String controlName) {
            for (RecordType type : values()) {
                if (type.getControlName() != null && type.getControlName().equals(controlName)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Cannot find RecordType associated with " + controlName);
        }
    }

    private String eventJson;

    private long insertedAt;

    private RecordType recordType = RecordType.EVENT;

    // For control events: expiration duration in milliseconds (not absolute time)
    private Long expirationDuration;

    public EventRecord() {
    }

    public EventRecord(String eventJson, long insertedAt, RecordType recordType, Long expirationDuration) {
        this.eventJson = eventJson;
        this.insertedAt = insertedAt;
        this.recordType = recordType;
        this.expirationDuration = expirationDuration;
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

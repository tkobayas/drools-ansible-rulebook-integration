package org.drools.ansible.rulebook.integration.ha.model;

/**
 * Represents a matching event with its associated metadata
 */
public class MatchingEvent {

    private String haUuid;  // HA instance that this MatchingEvent belongs to
    private String meUuid;
    private String ruleSetName;
    private String ruleName;
    private String eventData;  // JSON string representation of matching facts
    private long createdAt;    // epoch millis when the matching event was created

    public MatchingEvent() {
    }

    public String getHaUuid() {
        return haUuid;
    }

    public void setHaUuid(String haUuid) {
        this.haUuid = haUuid;
    }

    public String getMeUuid() {
        return meUuid;
    }

    public void setMeUuid(String meUuid) {
        this.meUuid = meUuid;
    }

    public String getRuleSetName() {
        return ruleSetName;
    }

    public void setRuleSetName(String ruleSetName) {
        this.ruleSetName = ruleSetName;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getEventData() {
        return eventData;
    }

    public void setEventData(String eventData) {
        this.eventData = eventData;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }
}

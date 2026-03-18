package org.drools.ansible.rulebook.integration.ha.model;

import java.util.HashMap;
import java.util.Map;

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

    // Extensibility columns for future use without schema migration
    private Map<String, Object> metadata = new HashMap<>();
    private Map<String, Object> properties = new HashMap<>();
    private Map<String, Object> settings = new HashMap<>();
    private Map<String, Object> ext = new HashMap<>();

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

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata != null ? metadata : new HashMap<>();
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties != null ? properties : new HashMap<>();
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, Object> settings) {
        this.settings = settings != null ? settings : new HashMap<>();
    }

    public Map<String, Object> getExt() {
        return ext;
    }

    public void setExt(Map<String, Object> ext) {
        this.ext = ext != null ? ext : new HashMap<>();
    }
}

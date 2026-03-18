package org.drools.ansible.rulebook.integration.ha.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a single action info for a matching event
 */
public class ActionInfo {

    private String id;          // UUID primary key
    private String haUuid;       // HA instance UUID
    private String meUuid;       // Foreign key to MatchingEvent
    private int index;           // Action index within the matching event
    private String actionData;   // JSON blob containing action details

    // Extensibility columns for future use without schema migration
    private Map<String, Object> metadata = new HashMap<>();
    private Map<String, Object> properties = new HashMap<>();
    private Map<String, Object> settings = new HashMap<>();
    private Map<String, Object> ext = new HashMap<>();

    public ActionInfo() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getActionData() {
        return actionData;
    }

    public void setActionData(String actionData) {
        this.actionData = actionData;
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
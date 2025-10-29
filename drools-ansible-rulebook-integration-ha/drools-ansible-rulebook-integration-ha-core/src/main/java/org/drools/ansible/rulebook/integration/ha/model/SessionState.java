package org.drools.ansible.rulebook.integration.ha.model;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;

/**
 * Represents the state of events in the HA system.
 * Contains both regular event processing state and in-flight matching events.
 */
public class SessionState {

    private String haUuid;
    private String ruleSetName;
    private String rulebookHash;

    private List<EventRecord> partialEvents;

    private long createdTime;
    private long persistedTime;

    // Metadata
    private int version;
    private String leaderId;

    // For integrity checks
    private String currentStateSHA;      // SHA256 of current state
    private String lastProcessedEventUuid;

    public SessionState() {
        this.createdTime = Instant.now().toEpochMilli();
        this.version = 1;
    }

    public String getHaUuid() {
        return haUuid;
    }

    public void setHaUuid(String haUuid) {
        this.haUuid = haUuid;
    }

    public String getRulebookHash() {
        return rulebookHash;
    }

    public void setRulebookHash(String rulebookHash) {
        this.rulebookHash = rulebookHash;
    }

    public String getRuleSetName() {
        return ruleSetName;
    }

    public void setRuleSetName(String ruleSetName) {
        this.ruleSetName = ruleSetName;
    }

    public List<EventRecord> getPartialEvents() {
        return partialEvents;
    }

    public void setPartialEvents(List<EventRecord> partialEvents) {
        this.partialEvents = partialEvents;
    }

    public long getPersistedTime() {
        return persistedTime;
    }

    public void setPersistedTime(long persistedTime) {
        this.persistedTime = persistedTime;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public String getCurrentStateSHA() {
        return currentStateSHA;
    }

    public void setCurrentStateSHA(String currentStateSHA) {
        this.currentStateSHA = currentStateSHA;
    }

    public String getLastProcessedEventUuid() {
        return lastProcessedEventUuid;
    }

    public void setLastProcessedEventUuid(String lastProcessedEventUuid) {
        this.lastProcessedEventUuid = lastProcessedEventUuid;
    }

    /**
     * Returns a canonical representation of this SessionState for SHA calculation.
     * Excludes fields that are not part of the semantic working memory state:
     * - currentStateSHA: can't hash itself (circular dependency)
     * - version: database-managed persistence version counter, not working memory state
     *
     * @return JSON string with deterministic field ordering for consistent hashing
     */
    public String toHashableContent() {
        // Create a map with fields that represent working memory state
        Map<String, Object> contentMap = new LinkedHashMap<>();

        contentMap.put("haUuid", haUuid);
        contentMap.put("ruleSetName", ruleSetName);
        contentMap.put("rulebookHash", rulebookHash);
        contentMap.put("partialEvents", partialEvents);
        contentMap.put("createdTime", createdTime);
        contentMap.put("persistedTime", persistedTime);
        // version is excluded - it's a database artifact, not working memory state
        contentMap.put("leaderId", leaderId);
        contentMap.put("lastProcessedEventUuid", lastProcessedEventUuid);

        return JsonMapper.toJson(contentMap);
    }
}

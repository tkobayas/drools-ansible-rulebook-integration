package org.drools.ansible.rulebook.integration.ha.model;

import java.time.Instant;
import java.util.List;

import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;

/**
 * Represents the state of events in the HA system.
 * Contains both regular event processing state and in-flight matching events.
 */
public class SessionState {

    private String haUuid;
    private String rulebookHash;

    private List<EventRecord> partialEvents;
    private SessionStats sessionStats;

    private long createdTime;
    private long persistedTime;

    // Metadata
    private int version;
    private boolean isCurrent;
    private String leaderId;

    public SessionState() {
        this.createdTime = Instant.now().toEpochMilli();
        this.version = 1;
        this.isCurrent = false;
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

    public SessionStats getSessionStats() {
        return sessionStats;
    }

    public void setSessionStats(SessionStats sessionStats) {
        this.sessionStats = sessionStats;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public boolean isCurrent() {
        return isCurrent;
    }

    public void setCurrent(boolean current) {
        isCurrent = current;
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
}
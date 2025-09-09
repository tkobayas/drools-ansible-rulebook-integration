package org.drools.ansible.rulebook.integration.ha.model;

import java.time.Instant;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;

/**
 * Represents the state of events in the HA system.
 * Contains both regular event processing state and in-flight matching events.
 */
public class SessionState {

    private String haUuid;
    private String rulebookHash;

    // Regular event processing state
    private Map<String, Object> partialEvents;
    private long clockTimeMillis;
    private SessionStats sessionStats;

    // Metadata
    private int version;
    private boolean isCurrent;
    private String createdAt;
    private String leaderId;

    public SessionState() {
        this.createdAt = Instant.now().toString();
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

    public Map<String, Object> getPartialEvents() {
        return partialEvents;
    }

    public void setPartialEvents(Map<String, Object> partialEvents) {
        this.partialEvents = partialEvents;
    }

    public long getClockTimeMillis() {
        return clockTimeMillis;
    }

    public void setClockTimeMillis(long clockTimeMillis) {
        this.clockTimeMillis = clockTimeMillis;
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

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }
}
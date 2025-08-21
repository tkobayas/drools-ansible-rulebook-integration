package org.drools.ansible.rulebook.integration.ha.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Represents the state of events in the HA system.
 * Contains both regular event processing state and in-flight matching events.
 */
public class EventState {
    
    private String sessionId;
    private String rulebookHash;
    
    // Regular event processing state
    private Map<String, Object> partialMatchingEvents;
    private Map<String, Object> timeWindows;
    private String clockTime;
    private Map<String, Object> sessionStats;
    
    // Metadata
    private int version;
    private boolean isCurrent;
    private String createdAt;
    private String leaderId;
    
    public EventState() {
        this.createdAt = Instant.now().toString();
        this.version = 1;
        this.isCurrent = false;
    }
    
    public String getSessionId() {
        return sessionId;
    }
    
    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
    
    public String getRulebookHash() {
        return rulebookHash;
    }
    
    public void setRulebookHash(String rulebookHash) {
        this.rulebookHash = rulebookHash;
    }
    
    public Map<String, Object> getPartialMatchingEvents() {
        return partialMatchingEvents;
    }
    
    public void setPartialMatchingEvents(Map<String, Object> partialMatchingEvents) {
        this.partialMatchingEvents = partialMatchingEvents;
    }
    
    public Map<String, Object> getTimeWindows() {
        return timeWindows;
    }
    
    public void setTimeWindows(Map<String, Object> timeWindows) {
        this.timeWindows = timeWindows;
    }
    
    public String getClockTime() {
        return clockTime;
    }
    
    public void setClockTime(String clockTime) {
        this.clockTime = clockTime;
    }
    
    public Map<String, Object> getSessionStats() {
        return sessionStats;
    }
    
    public void setSessionStats(Map<String, Object> sessionStats) {
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
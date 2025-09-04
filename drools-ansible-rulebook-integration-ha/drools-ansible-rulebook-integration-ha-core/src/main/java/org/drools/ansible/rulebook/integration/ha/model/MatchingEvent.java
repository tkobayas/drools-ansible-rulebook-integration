package org.drools.ansible.rulebook.integration.ha.model;

/**
 * Represents a matching event with its associated metadata
 */
public class MatchingEvent {
    
    private String meUuid;
    private String sessionId;
    private String ruleSetName;
    private String ruleName;
    private String eventData;  // JSON string representation of matching facts
    
    public MatchingEvent() {
    }
    
    public String getMeUuid() {
        return meUuid;
    }
    
    public void setMeUuid(String meUuid) {
        this.meUuid = meUuid;
    }
    
    public String getSessionId() {
        return sessionId;
    }
    
    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
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
}
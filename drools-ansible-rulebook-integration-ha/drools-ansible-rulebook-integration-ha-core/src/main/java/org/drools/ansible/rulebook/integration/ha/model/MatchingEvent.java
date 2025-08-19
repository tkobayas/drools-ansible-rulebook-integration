package org.drools.ansible.rulebook.integration.ha.model;

import java.time.Instant;
import java.util.Map;

/**
 * Represents a matching event with its associated metadata
 */
public class MatchingEvent {
    
    private String meUuid;
    private String sessionId;
    private String rulesetName;
    private String ruleName;
    private Map<String, Object> matchingFacts;
    private EventState eventState;
    private ActionState actionState;
    private String createdAt;
    private MatchingEventStatus status;
    
    public enum MatchingEventStatus {
        PENDING,
        IN_PROGRESS,
        COMPLETED,
        FAILED
    }
    
    public MatchingEvent() {
        this.createdAt = Instant.now().toString();
        this.status = MatchingEventStatus.PENDING;
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
    
    public String getRulesetName() {
        return rulesetName;
    }
    
    public void setRulesetName(String rulesetName) {
        this.rulesetName = rulesetName;
    }
    
    public String getRuleName() {
        return ruleName;
    }
    
    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }
    
    public Map<String, Object> getMatchingFacts() {
        return matchingFacts;
    }
    
    public void setMatchingFacts(Map<String, Object> matchingFacts) {
        this.matchingFacts = matchingFacts;
    }
    
    public EventState getEventState() {
        return eventState;
    }
    
    public void setEventState(EventState eventState) {
        this.eventState = eventState;
    }
    
    public ActionState getActionState() {
        return actionState;
    }
    
    public void setActionState(ActionState actionState) {
        this.actionState = actionState;
    }
    
    public String getCreatedAt() {
        return createdAt;
    }
    
    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }
    
    public MatchingEventStatus getStatus() {
        return status;
    }
    
    public void setStatus(MatchingEventStatus status) {
        this.status = status;
    }
}
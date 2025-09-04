package org.drools.ansible.rulebook.integration.ha.model;

/**
 * Represents a single action state for a matching event
 */
public class ActionState {
    
    private String id;          // UUID primary key
    private String meUuid;       // Foreign key to MatchingEvent
    private int index;           // Action index within the matching event
    private String actionData;   // JSON blob containing action details
    
    public ActionState() {
    }
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
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
}
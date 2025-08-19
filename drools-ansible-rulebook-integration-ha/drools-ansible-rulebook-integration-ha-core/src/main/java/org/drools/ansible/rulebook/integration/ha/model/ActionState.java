package org.drools.ansible.rulebook.integration.ha.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents the state of actions for a matching event
 */
public class ActionState {
    
    private String meUuid;
    private String rulesetName;
    private String ruleName;
    private List<Action> actions = new ArrayList<>();
    private int version;
    private String updatedAt;
    
    public static class Action {
        private String name;
        private int index;
        private ActionStatus status;
        private String startedAt;
        private String endedAt;
        private String referenceId;
        private String referenceUrl;
        private Map<String, Object> customData;
        
        public enum ActionStatus {
            PENDING,
            STARTED,
            COMPLETED,
            FAILED
        }
        
        public String getName() {
            return name;
        }
        
        public void setName(String name) {
            this.name = name;
        }
        
        public int getIndex() {
            return index;
        }
        
        public void setIndex(int index) {
            this.index = index;
        }
        
        public ActionStatus getStatus() {
            return status;
        }
        
        public void setStatus(ActionStatus status) {
            this.status = status;
        }
        
        public String getStartedAt() {
            return startedAt;
        }
        
        public void setStartedAt(String startedAt) {
            this.startedAt = startedAt;
        }
        
        public String getEndedAt() {
            return endedAt;
        }
        
        public void setEndedAt(String endedAt) {
            this.endedAt = endedAt;
        }
        
        public String getReferenceId() {
            return referenceId;
        }
        
        public void setReferenceId(String referenceId) {
            this.referenceId = referenceId;
        }
        
        public String getReferenceUrl() {
            return referenceUrl;
        }
        
        public void setReferenceUrl(String referenceUrl) {
            this.referenceUrl = referenceUrl;
        }
        
        public Map<String, Object> getCustomData() {
            return customData;
        }
        
        public void setCustomData(Map<String, Object> customData) {
            this.customData = customData;
        }
    }
    
    public ActionState() {
        this.updatedAt = Instant.now().toString();
        this.version = 1;
    }
    
    public String getMeUuid() {
        return meUuid;
    }
    
    public void setMeUuid(String meUuid) {
        this.meUuid = meUuid;
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
    
    public List<Action> getActions() {
        return actions;
    }
    
    public void setActions(List<Action> actions) {
        this.actions = actions;
    }
    
    public int getVersion() {
        return version;
    }
    
    public void setVersion(int version) {
        this.version = version;
    }
    
    public String getUpdatedAt() {
        return updatedAt;
    }
    
    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }
}
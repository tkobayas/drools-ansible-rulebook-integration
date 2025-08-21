package org.drools.ansible.rulebook.integration.ha.api;

import org.drools.ansible.rulebook.integration.ha.model.ActionState;
import org.drools.ansible.rulebook.integration.ha.model.EventState;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;

import java.util.List;
import java.util.Map;

/**
 * Interface for managing High Availability state persistence.
 * Implementations should handle connection management, transactions, and state consistency.
 */
public interface HAStateManager {
    
    /**
     * Initialize the state manager with configuration
     * @param config Configuration map containing connection details
     */
    void initialize(HAConfiguration config);
    
    /**
     * Set the current node as leader
     * @param leaderId Unique identifier for this leader node
     */
    void setLeader(String leaderId);
    
    /**
     * Unset leader status for this node
     */
    void unsetLeader();
    
    /**
     * Check if this node is currently the leader
     * @return true if this node is the leader
     */
    boolean isLeader();
    
    /**
     * Get current event state for a session
     * @param sessionId The ruleset session ID
     * @return The current event state or null if not found
     */
    EventState getEventState(String sessionId);
    
    /**
     * Persist complete event state (includes matching events)
     * @param sessionId The ruleset session ID
     * @param eventState The event state to persist
     */
    void persistEventState(String sessionId, EventState eventState);
    
    /**
     * Add a matching event to the current event state
     * @param matchingEvent The matching event to persist
     * @return UUID for the matching event
     */
    String addMatchingEvent(MatchingEvent matchingEvent);
    
    /**
     * Persist or update action state
     * @param sessionId The ruleset session ID
     * @param meUuid The matching event UUID
     * @param actionState The action state to persist
     */
    void persistActionState(String sessionId, String meUuid, ActionState actionState);
    
    /**
     * Get action state for a matching event
     * @param sessionId The ruleset session ID
     * @param meUuid The matching event UUID
     * @return The action state or null if not found
     */
    ActionState getActionState(String sessionId, String meUuid);
    
    /**
     * Remove a matching event from event state and delete associated action states
     * @param meUuid The matching event UUID
     */
    void removeMatchingEvent(String meUuid);
    
    /**
     * Get all pending matching events for a session
     * @param sessionId The ruleset session ID
     * @return List of pending matching events
     */
    List<MatchingEvent> getPendingMatchingEvents(String sessionId);
    
    /**
     * Commit the current state (two-version protocol)
     * @param sessionId The ruleset session ID
     */
    void commitState(String sessionId);
    
    /**
     * Rollback to previous state (two-version protocol)
     * @param sessionId The ruleset session ID
     */
    void rollbackState(String sessionId);
    
    /**
     * Store session statistics
     * @param sessionId The ruleset session ID
     * @param stats Statistics map
     */
    void persistSessionStats(String sessionId, Map<String, Object> stats);
    
    /**
     * Cleanup resources and close connections
     */
    void shutdown();
}
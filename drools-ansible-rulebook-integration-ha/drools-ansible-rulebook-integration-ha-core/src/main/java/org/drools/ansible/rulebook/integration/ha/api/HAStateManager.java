package org.drools.ansible.rulebook.integration.ha.api;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.model.EventState;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;

/**
 * Interface for managing High Availability state persistence.
 * Implementations should handle connection management, transactions, and state consistency.
 */
public interface HAStateManager {

    /**
     * Initialize HA mode with UUID and database configuration
     *
     * @param uuid           Unique identifier for this HA instance
     * @param postgresParams Database connection parameters
     * @param config         HA configuration parameters
     */
    void initializeHA(String uuid, Map<String, Object> postgresParams, Map<String, Object> config);

    /**
     * Enable leader mode and start writing states to database
     *
     * @param leaderName Name/identifier for this leader
     */
    void enableLeader(String leaderName);

    /**
     * Disable leader mode and stop writing to database
     *
     * @param leaderName Name/identifier for this leader
     */
    void disableLeader(String leaderName);

    /**
     * Check if this node is currently the leader
     *
     * @return true if this node is the leader
     */
    boolean isLeader();

    /**
     * Get current event state for a session
     *
     * @param sessionId The ruleset session ID
     * @return The current event state or null if not found
     */
    EventState getEventState(String sessionId);

    /**
     * Persist complete event state (includes matching events)
     *
     * @param sessionId  The ruleset session ID
     * @param eventState The event state to persist
     */
    void persistEventState(String sessionId, EventState eventState);

    /**
     * Add a matching event to the database
     *
     * @param matchingEvent The matching event to persist
     * @return UUID for the matching event
     */
    String addMatchingEvent(MatchingEvent matchingEvent);

    /**
     * Get all pending matching events for a session
     *
     * @param sessionId The ruleset session ID
     * @return List of pending matching events
     */
    List<MatchingEvent> getPendingMatchingEvents(String sessionId);

    /**
     * Commit the current state (two-version protocol)
     *
     * @param sessionId The ruleset session ID
     */
    void commitState(String sessionId);

    /**
     * Rollback to previous state (two-version protocol)
     *
     * @param sessionId The ruleset session ID
     */
    void rollbackState(String sessionId);

    /**
     * Add an action for a matching event
     *
     * @param sessionId    The ruleset session ID
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @param action       The action data as JSON string
     */
    void addActionState(String sessionId, String matchingUuid, int index, String action);

    /**
     * Update an existing action
     *
     * @param sessionId    The ruleset session ID
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @param action       The updated action data as JSON string
     */
    void updateActionState(String sessionId, String matchingUuid, int index, String action);

    /**
     * Check if an action exists
     *
     * @param sessionId    The ruleset session ID
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @return true if action exists, false otherwise
     */
    boolean actionStateExists(String sessionId, String matchingUuid, int index);

    /**
     * Get an action by index
     *
     * @param sessionId    The ruleset session ID
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @return The action data as JSON string, empty string if not found
     */
    String getActionState(String sessionId, String matchingUuid, int index);

    /**
     * Delete all actions and matching events for a matching UUID
     *
     * @param sessionId    The ruleset session ID
     * @param matchingUuid The matching event UUID
     */
    void deleteActionStates(String sessionId, String matchingUuid);

    /**
     * Get current HA statistics
     *
     * @return HA statistics object
     */
    HAStats getHAStats();

    /**
     * Store session statistics
     *
     * @param sessionId The ruleset session ID
     * @param stats     Statistics map
     */
    void persistSessionStats(String sessionId, Map<String, Object> stats);

    /**
     * Cleanup resources and close connections
     */
    void shutdown();
}
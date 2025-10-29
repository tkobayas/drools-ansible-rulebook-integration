package org.drools.ansible.rulebook.integration.ha.api;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
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
     * Get the UUID of this HA instance
     *
     * @return The HA instance UUID
     */
    String getHaUuid();

    /**
     * Get the leader ID
     *
     * @return The leader ID or null if no leader
     */
    String getLeaderId();

    /**
     * Get current session state for a session
     * This reads from the database (persisted state)
     *
     * @return The current session state or null if not found
     */
    SessionState getPersistedSessionState(String ruleSetName);

    /**
     * Persist complete session state (includes matching events)
     *
     * @param sessionState The session state to persist
     */
    void persistSessionState(SessionState sessionState);

    /**
     * Recover drools session using session state from the database
     * This should be called on startup to restore state
     *
     * @return
     */
    RulesExecutor recoverSession(RulesSet rulesSet, SessionState sessionState);

    /**
     * Add a matching event to the database
     *
     * @param matchingEvent The matching event to persist
     * @return UUID for the matching event
     */
    String addMatchingEvent(MatchingEvent matchingEvent);

    /**
     * Get all pending matching events based on haUuid
     *
     * @return List of pending matching events
     */
    List<MatchingEvent> getPendingMatchingEvents();

    /**
     * Add an action for a matching event
     *
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @param action       The action data as JSON string
     */
    void addActionInfo(String matchingUuid, int index, String action);

    /**
     * Update an existing action
     *
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @param action       The updated action data as JSON string
     */
    void updateActionInfo(String matchingUuid, int index, String action);

    /**
     * Check if an action exists
     *
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @return true if action exists, false otherwise
     */
    boolean actionInfoExists(String matchingUuid, int index);

    /**
     * Get an action by index
     *
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @return The action data as JSON string, empty string if not found
     */
    String getActionInfo(String matchingUuid, int index);

    String getActionStatus(String matchingUuid, int index);

    /**
     * Delete all actions and matching events for a matching UUID
     *
     * @param matchingUuid The matching event UUID
     */
    void deleteActionInfo(String matchingUuid);

    /**
     * Get current HA statistics
     *
     * @return HA statistics object
     */
    HAStats getHAStats();

    /**
     * Persist current HA statistics
     */
    void persistHAStats();

    /**
     * Cleanup resources and close connections
     */
    void shutdown();

    /**
     * Register session state in memory (for both leader and non-leader nodes)
     *
     * @param ruleSetName The name of the ruleset
     * @param sessionState The session state to register in memory
     */
    void registerSessionState(String ruleSetName, SessionState sessionState);

    /**
     * Get the in-memory session state for a given ruleset
     *
     * @param ruleSetName The name of the ruleset
     * @return The in-memory session state or null if not found
     */
    SessionState getInMemorySessionState(String ruleSetName);
}

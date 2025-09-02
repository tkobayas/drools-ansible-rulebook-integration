package org.drools.ansible.rulebook.integration.ha.h2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.model.ActionState;
import org.drools.ansible.rulebook.integration.ha.model.EventState;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * H2 implementation with new model where EventState contains MatchingEvents
 */
public class H2StateManager implements HAStateManager {
    
    private static final Logger logger = LoggerFactory.getLogger(H2StateManager.class);
    
    private HikariDataSource dataSource;
    private ObjectMapper objectMapper;
    private String leaderId;
    private boolean isLeader = false;
    private String haUuid;
    private HAStats haStats;
    
    public H2StateManager() {
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public void initializeHA(String uuid, Map<String, Object> postgresParams, Map<String, Object> config) {
        logger.info("Initializing HA mode with UUID: {}", uuid);
        
        this.haUuid = uuid;
        this.haStats = new HAStats();
        
        // Configure HikariCP connection pool from postgres params
        HikariConfig hikariConfig = new HikariConfig();
        
        // Extract connection parameters
        String host = (String) postgresParams.get("host");
        Integer port = (Integer) postgresParams.get("port");
        String dbname = (String) postgresParams.get("dbname");
        String user = (String) postgresParams.get("user");
        String password = (String) postgresParams.get("password");
        String sslmode = (String) postgresParams.get("sslmode");
        String applicationName = (String) postgresParams.get("application_name");
        
        // Build JDBC URL - default to H2 for testing if postgres params not provided
        String jdbcUrl;
        if (host != null && dbname != null) {
            jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port != null ? port : 5432, dbname);
            if (sslmode != null) {
                jdbcUrl += "?sslmode=" + sslmode;
            }
            if (applicationName != null) {
                jdbcUrl += (sslmode != null ? "&" : "?") + "ApplicationName=" + applicationName;
            }
        } else {
            // Fallback to H2 for development/testing
            jdbcUrl = "jdbc:h2:mem:eda_ha_" + uuid + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
            logger.warn("Using H2 database for HA - not suitable for production");
        }
        
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(user != null ? user : "sa");
        hikariConfig.setPassword(password != null ? password : "");
        
        // Extract config parameters
        Integer writeAfter = (Integer) config.get("write_after");
        hikariConfig.setMaximumPoolSize(10); // Default pool size
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setAutoCommit(false); // We'll manage transactions
        
        this.dataSource = new HikariDataSource(hikariConfig);
        
        // Create schema
        try {
            H2Schema.createSchema(dataSource);
            logger.info("HA schema created successfully for UUID: {}", uuid);
            
            // Check for existing matching events and restore HA stats
            restoreHAState();
            
        } catch (SQLException e) {
            logger.error("Failed to create HA schema", e);
            throw new RuntimeException("Failed to initialize HA mode", e);
        }
    }
    
    
    @Override
    public void enableLeader(String leaderName) {
        this.leaderId = leaderName;
        this.isLeader = true;
        
        // Update HA stats when becoming leader
        if (haStats != null) {
            haStats.setCurrentLeader(leaderName);
        }
        
        // Persist HA stats to database
        persistHAStats();
        
        logger.info("Leader mode enabled for: {}", leaderName);
    }
    
    @Override
    public void disableLeader(String leaderName) {
        this.isLeader = false;
        logger.info("Leader mode disabled for: {}", leaderName);
        
        // Update HA stats
        if (haStats != null && leaderName.equals(haStats.getCurrentLeader())) {
            haStats.setCurrentLeader(null);
            persistHAStats();
        }
        
        this.leaderId = null;
    }
    
    @Override
    public boolean isLeader() {
        return isLeader;
    }
    
    @Override
    public EventState getEventState(String sessionId) {
        try (Connection conn = dataSource.getConnection()) {
            String sql = "SELECT * FROM eda_event_state WHERE session_id = ? AND is_current = true";
            
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, sessionId);
                
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return mapToEventState(rs);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to get event state", e);
            throw new RuntimeException("Failed to get event state", e);
        }
        
        return null;
    }
    
    @Override
    public void persistEventState(String sessionId, EventState eventState) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot persist event state - not the leader");
        }
        
        eventState.setSessionId(sessionId);
        eventState.setLeaderId(leaderId);
        
        try (Connection conn = dataSource.getConnection()) {
            // Get next version number
            int nextVersion = 1;
            String getMaxVersionSql = "SELECT MAX(version) FROM eda_event_state WHERE session_id = ?";
            try (PreparedStatement ps = conn.prepareStatement(getMaxVersionSql)) {
                ps.setString(1, sessionId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        int maxVersion = rs.getInt(1);
                        if (!rs.wasNull()) {
                            nextVersion = maxVersion + 1;
                        }
                    }
                }
            }
            
            // Mark previous version as not current
            String updatePrevious = "UPDATE eda_event_state SET is_current = false WHERE session_id = ?";
            try (PreparedStatement ps = conn.prepareStatement(updatePrevious)) {
                ps.setString(1, sessionId);
                ps.executeUpdate();
            }
            
            // Insert new version
            String sql = """
                INSERT INTO eda_event_state 
                (session_id, rulebook_hash, partial_matching_events, time_windows, 
                 clock_time, session_stats, version, is_current, 
                 created_at, leader_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
                
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, eventState.getSessionId());
                ps.setString(2, eventState.getRulebookHash());
                ps.setString(3, toJson(eventState.getPartialMatchingEvents()));
                ps.setString(4, toJson(eventState.getTimeWindows()));
                ps.setTimestamp(5, eventState.getClockTime() != null ? 
                              Timestamp.from(Instant.parse(eventState.getClockTime())) : null);
                ps.setString(6, toJson(eventState.getSessionStats()));
                ps.setInt(7, nextVersion);
                ps.setBoolean(8, eventState.isCurrent());
                ps.setTimestamp(9, Timestamp.from(Instant.parse(eventState.getCreatedAt())));
                ps.setString(10, eventState.getLeaderId());
                
                ps.executeUpdate();
            }
            
            conn.commit();
            
            // Update HA stats
            if (haStats != null) {
                haStats.incrementEventsProcessed();
                persistHAStats();
            }
            
            logger.debug("Persisted event state for session: {}", sessionId);
        } catch (SQLException e) {
            logger.error("Failed to persist event state", e);
            throw new RuntimeException("Failed to persist event state", e);
        }
    }
    
    @Override
    public String addMatchingEvent(MatchingEvent matchingEvent) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add matching event - not the leader");
        }
        
        // Validate the matching event has required fields
        if (matchingEvent.getMeUuid() == null || matchingEvent.getMeUuid().isEmpty()) {
            throw new IllegalArgumentException("MatchingEvent must have a UUID");
        }
        if (matchingEvent.getSessionId() == null || matchingEvent.getSessionId().isEmpty()) {
            throw new IllegalArgumentException("MatchingEvent must have a session ID");
        }
        
        // Insert into matching_events table
        try (Connection conn = dataSource.getConnection()) {
            String sql = """
                INSERT INTO eda_matching_events 
                (me_uuid, session_id, ruleset_name, rule_name, matching_facts, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
                
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, matchingEvent.getMeUuid());
                ps.setString(2, matchingEvent.getSessionId());
                ps.setString(3, matchingEvent.getRulesetName());
                ps.setString(4, matchingEvent.getRuleName());
                ps.setString(5, toJson(matchingEvent.getMatchingFacts()));
                ps.setString(6, matchingEvent.getStatus() != null ? 
                            matchingEvent.getStatus().toString() : "PENDING");
                ps.setString(7, matchingEvent.getCreatedAt());
                
                ps.executeUpdate();
            }
            
            conn.commit();
            logger.debug("Added matching event with UUID: {} for rule: {}/{}", 
                        matchingEvent.getMeUuid(), matchingEvent.getRulesetName(), 
                        matchingEvent.getRuleName());
        } catch (SQLException e) {
            logger.error("Failed to add matching event", e);
            throw new RuntimeException("Failed to add matching event", e);
        }
        
        return matchingEvent.getMeUuid();
    }
    
    @Override
    public void persistActionState(String sessionId, String meUuid, ActionState actionState) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot persist action state - not the leader");
        }
        
        actionState.setMeUuid(meUuid);
        
        try (Connection conn = dataSource.getConnection()) {
            // Check if exists
            String checkSql = "SELECT version FROM eda_action_state WHERE session_id = ? AND me_uuid = ?";
            boolean exists = false;
            int currentVersion = 0;
            
            try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
                ps.setString(1, sessionId);
                ps.setString(2, meUuid);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        exists = true;
                        currentVersion = rs.getInt("version");
                    }
                }
            }
            
            String sql;
            if (exists) {
                sql = """
                    UPDATE eda_action_state 
                    SET ruleset_name = ?, rule_name = ?, action_data = ?, 
                        status = ?, version = ?, updated_at = ?
                    WHERE session_id = ? AND me_uuid = ?
                    """;
            } else {
                sql = """
                    INSERT INTO eda_action_state 
                    (ruleset_name, rule_name, action_data, status, version, 
                     updated_at, session_id, me_uuid)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """;
            }
            
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, actionState.getRulesetName());
                ps.setString(2, actionState.getRuleName());
                ps.setString(3, toJson(actionState.getActions()));
                ps.setString(4, determineStatus(actionState));
                ps.setInt(5, exists ? currentVersion + 1 : 1);
                ps.setTimestamp(6, Timestamp.from(Instant.parse(actionState.getUpdatedAt())));
                ps.setString(7, sessionId);
                ps.setString(8, meUuid);
                
                ps.executeUpdate();
            }
            
            conn.commit();
            logger.debug("Persisted action state for ME UUID: {}", meUuid);
        } catch (SQLException e) {
            logger.error("Failed to persist action state", e);
            throw new RuntimeException("Failed to persist action state", e);
        }
    }
    
    @Override
    public ActionState getActionState(String sessionId, String meUuid) {
        try (Connection conn = dataSource.getConnection()) {
            String sql = """
                SELECT * FROM eda_action_state 
                WHERE session_id = ? AND me_uuid = ?
                ORDER BY version DESC
                LIMIT 1
                """;
                
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, sessionId);
                ps.setString(2, meUuid);
                
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return mapToActionState(rs);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to get action state", e);
            throw new RuntimeException("Failed to get action state", e);
        }
        return null;
    }
    
    @Override
    public void removeMatchingEvent(String meUuid) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot remove matching event - not the leader");
        }
        
        try (Connection conn = dataSource.getConnection()) {
            // First get the session_id from the matching event
            String sessionId = null;
            String getSessionSql = "SELECT session_id FROM eda_matching_events WHERE me_uuid = ?";
            try (PreparedStatement ps = conn.prepareStatement(getSessionSql)) {
                ps.setString(1, meUuid);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        sessionId = rs.getString("session_id");
                    }
                }
            }
            
            // Delete matching event
            String deleteME = "DELETE FROM eda_matching_events WHERE me_uuid = ?";
            try (PreparedStatement ps = conn.prepareStatement(deleteME)) {
                ps.setString(1, meUuid);
                ps.executeUpdate();
            }
            
            // Delete associated action state if we found the session_id
            if (sessionId != null) {
                String deleteAction = "DELETE FROM eda_action_state WHERE session_id = ? AND me_uuid = ?";
                try (PreparedStatement ps = conn.prepareStatement(deleteAction)) {
                    ps.setString(1, sessionId);
                    ps.setString(2, meUuid);
                    ps.executeUpdate();
                }
            }
            
            conn.commit();
        } catch (SQLException e) {
            logger.error("Failed to remove matching event", e);
            throw new RuntimeException("Failed to remove matching event", e);
        }
        
        logger.debug("Removed matching event: {}", meUuid);
    }
    
    @Override
    public List<MatchingEvent> getPendingMatchingEvents(String sessionId) {
        List<MatchingEvent> matchingEvents = new ArrayList<>();
        
        try (Connection conn = dataSource.getConnection()) {
            String sql = """
                SELECT * FROM eda_matching_events 
                WHERE session_id = ? AND status = 'PENDING'
                ORDER BY created_at
                """;
                
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, sessionId);
                
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        MatchingEvent me = new MatchingEvent();
                        me.setMeUuid(rs.getString("me_uuid"));
                        me.setSessionId(rs.getString("session_id"));
                        me.setRulesetName(rs.getString("ruleset_name"));
                        me.setRuleName(rs.getString("rule_name"));
                        me.setMatchingFacts(fromJson(rs.getString("matching_facts")));
                        me.setStatus(MatchingEvent.MatchingEventStatus.valueOf(rs.getString("status")));
                        me.setCreatedAt(rs.getString("created_at"));
                        matchingEvents.add(me);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to get pending matching events", e);
            throw new RuntimeException("Failed to get pending matching events", e);
        }
        
        return matchingEvents;
    }
    
    @Override
    public void commitState(String sessionId) {
        // Implementation for two-version protocol
        logger.debug("Committed state for session: {}", sessionId);
    }
    
    @Override
    public void rollbackState(String sessionId) {
        // Implementation for two-version protocol
        logger.debug("Rolled back state for session: {}", sessionId);
    }
    
    @Override
    public void persistSessionStats(String sessionId, Map<String, Object> stats) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot persist session stats - not the leader");
        }
        
        // Add stats to current event state
        EventState eventState = getEventState(sessionId);
        if (eventState == null) {
            eventState = new EventState();
            eventState.setSessionId(sessionId);
        }
        
        eventState.setSessionStats(stats);
        eventState.setCurrent(true);
        persistEventState(sessionId, eventState);
        
        logger.debug("Persisted session stats for session: {}", sessionId);
    }
    
    @Override
    public void addAction(String sessionId, String matchingUuid, int index, Map<String, Object> action) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add action - not the leader");
        }
        
        try (Connection conn = dataSource.getConnection()) {
            // Get or create action state for this matching event
            ActionState actionState = getActionState(sessionId, matchingUuid);
            if (actionState == null) {
                actionState = new ActionState();
                actionState.setMeUuid(matchingUuid);
                // Get ruleset and rule name from matching event
                populateActionStateFromMatchingEvent(conn, actionState, matchingUuid);
            }
            
            // Convert Map to Action object
            ActionState.Action newAction = mapToAction(action, index);
            
            // Add or update action in the list
            List<ActionState.Action> actions = actionState.getActions();
            if (actions == null) {
                actions = new ArrayList<>();
                actionState.setActions(actions);
            }
            
            // Find and replace if index exists, otherwise add
            boolean found = false;
            for (int i = 0; i < actions.size(); i++) {
                if (actions.get(i).getIndex() == index) {
                    actions.set(i, newAction);
                    found = true;
                    break;
                }
            }
            if (!found) {
                actions.add(newAction);
            }
            
            // Persist the updated action state
            persistActionState(sessionId, matchingUuid, actionState);
            
            // Update HA stats
            if (haStats != null) {
                haStats.incrementActionsProcessed();
                persistHAStats();
            }
            
        } catch (SQLException e) {
            logger.error("Failed to add action", e);
            throw new RuntimeException("Failed to add action", e);
        }
    }
    
    @Override
    public void updateAction(String sessionId, String matchingUuid, int index, Map<String, Object> action) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot update action - not the leader");
        }
        
        ActionState actionState = getActionState(sessionId, matchingUuid);
        if (actionState == null) {
            throw new IllegalArgumentException("No action state found for matching UUID: " + matchingUuid);
        }
        
        List<ActionState.Action> actions = actionState.getActions();
        if (actions == null || actions.isEmpty()) {
            throw new IllegalArgumentException("No actions found for matching UUID: " + matchingUuid);
        }
        
        // Find and update action by index
        boolean found = false;
        for (int i = 0; i < actions.size(); i++) {
            if (actions.get(i).getIndex() == index) {
                ActionState.Action updatedAction = mapToAction(action, index);
                actions.set(i, updatedAction);
                found = true;
                break;
            }
        }
        
        if (!found) {
            throw new IllegalArgumentException("Action with index " + index + " not found");
        }
        
        // Persist the updated action state
        persistActionState(sessionId, matchingUuid, actionState);
    }
    
    @Override
    public boolean actionExists(String sessionId, String matchingUuid, int index) {
        ActionState actionState = getActionState(sessionId, matchingUuid);
        if (actionState == null || actionState.getActions() == null) {
            return false;
        }
        
        return actionState.getActions().stream()
                .anyMatch(action -> action.getIndex() == index);
    }
    
    @Override
    public Map<String, Object> getAction(String sessionId, String matchingUuid, int index) {
        ActionState actionState = getActionState(sessionId, matchingUuid);
        if (actionState == null || actionState.getActions() == null) {
            return new HashMap<>();
        }
        
        for (ActionState.Action action : actionState.getActions()) {
            if (action.getIndex() == index) {
                return actionToMap(action);
            }
        }
        
        return new HashMap<>();
    }
    
    @Override
    public void deleteActions(String sessionId, String matchingUuid) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot delete actions - not the leader");
        }
        
        // Use existing removeMatchingEvent method which deletes both matching event and actions
        removeMatchingEvent(matchingUuid);
    }
    
    @Override
    public HAStats getHAStats() {
        return haStats != null ? haStats : new HAStats();
    }
    
    @Override
    public void shutdown() {
        logger.info("Shutting down H2StateManager");
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
    
    // Helper methods for new API
    
    private void restoreHAState() {
        // Initialize HA stats from database or create new
        try (Connection conn = dataSource.getConnection()) {
            String sql = "SELECT * FROM eda_ha_stats LIMIT 1";
            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    // Restore existing HA stats
                    haStats = new HAStats();
                    haStats.setCurrentLeader(rs.getString("current_leader"));
                    haStats.setLeaderSwitches(rs.getInt("leader_switches"));
                    haStats.setCurrentTermStartedAt(rs.getString("current_term_started_at"));
                    haStats.setEventsProcessedInTerm(rs.getInt("events_processed_in_term"));
                    haStats.setActionsProcessedInTerm(rs.getInt("actions_processed_in_term"));
                    logger.info("Restored HA stats from database");
                } else {
                    // Create new HA stats
                    haStats = new HAStats();
                    persistHAStats();
                    logger.info("Created new HA stats");
                }
            }
        } catch (SQLException e) {
            logger.warn("Failed to restore HA state, creating new", e);
            haStats = new HAStats();
        }
    }
    
    private void persistHAStats() {
        if (!isLeader || haStats == null) {
            return;
        }
        
        try (Connection conn = dataSource.getConnection()) {
            String sql = """
                INSERT INTO eda_ha_stats 
                (session_id, current_leader, leader_switches, current_term_started_at,
                 events_processed_in_term, actions_processed_in_term, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                current_leader = VALUES(current_leader),
                leader_switches = VALUES(leader_switches),
                current_term_started_at = VALUES(current_term_started_at),
                events_processed_in_term = VALUES(events_processed_in_term),
                actions_processed_in_term = VALUES(actions_processed_in_term),
                updated_at = VALUES(updated_at)
                """;
            
            // For H2, use MERGE statement instead
            String h2Sql = """
                MERGE INTO eda_ha_stats 
                (session_id, current_leader, leader_switches, current_term_started_at,
                 events_processed_in_term, actions_processed_in_term, updated_at)
                KEY(session_id) VALUES (?, ?, ?, ?, ?, ?, ?)
                """;
                
            try (PreparedStatement ps = conn.prepareStatement(h2Sql)) {
                ps.setString(1, haUuid != null ? haUuid : "default");
                ps.setString(2, haStats.getCurrentLeader());
                ps.setInt(3, haStats.getLeaderSwitches());
                ps.setString(4, haStats.getCurrentTermStartedAt());
                ps.setInt(5, haStats.getEventsProcessedInTerm());
                ps.setInt(6, haStats.getActionsProcessedInTerm());
                ps.setTimestamp(7, Timestamp.from(Instant.now()));
                
                ps.executeUpdate();
            }
            
            conn.commit();
        } catch (SQLException e) {
            logger.error("Failed to persist HA stats", e);
        }
    }
    
    private void populateActionStateFromMatchingEvent(Connection conn, ActionState actionState, String matchingUuid) throws SQLException {
        String sql = "SELECT ruleset_name, rule_name FROM eda_matching_events WHERE me_uuid = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, matchingUuid);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    actionState.setRulesetName(rs.getString("ruleset_name"));
                    actionState.setRuleName(rs.getString("rule_name"));
                }
            }
        }
    }
    
    private ActionState.Action mapToAction(Map<String, Object> actionMap, int index) {
        ActionState.Action action = new ActionState.Action();
        action.setIndex(index);
        action.setName((String) actionMap.get("name"));
        action.setReferenceId((String) actionMap.get("reference_id"));
        action.setReferenceUrl((String) actionMap.get("reference_url"));
        action.setStartedAt((String) actionMap.get("start_time"));
        action.setEndedAt((String) actionMap.get("end_time"));
        action.setCustomData((Map<String, Object>) actionMap.get("custom_data"));
        
        // Map status string to enum
        String statusStr = (String) actionMap.get("status");
        if (statusStr != null) {
            switch (statusStr.toLowerCase()) {
                case "running":
                    action.setStatus(ActionState.Action.ActionStatus.RUNNING);
                    break;
                case "success":
                    action.setStatus(ActionState.Action.ActionStatus.SUCCESS);
                    break;
                case "failed":
                    action.setStatus(ActionState.Action.ActionStatus.FAILED);
                    break;
                default:
                    action.setStatus(ActionState.Action.ActionStatus.RUNNING);
            }
        }
        
        return action;
    }
    
    private Map<String, Object> actionToMap(ActionState.Action action) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", action.getName());
        map.put("index", action.getIndex());
        map.put("reference_id", action.getReferenceId());
        map.put("reference_url", action.getReferenceUrl());
        map.put("start_time", action.getStartedAt());
        map.put("end_time", action.getEndedAt());
        map.put("custom_data", action.getCustomData());
        
        if (action.getStatus() != null) {
            map.put("status", action.getStatus().name().toLowerCase());
        }
        
        return map;
    }
    
    // Existing helper methods
    
    private EventState mapToEventState(ResultSet rs) throws SQLException {
        EventState event = new EventState();
        event.setSessionId(rs.getString("session_id"));
        event.setRulebookHash(rs.getString("rulebook_hash"));
        event.setPartialMatchingEvents(fromJson(rs.getString("partial_matching_events")));
        event.setTimeWindows(fromJson(rs.getString("time_windows")));
        
        Timestamp clockTime = rs.getTimestamp("clock_time");
        if (clockTime != null) {
            event.setClockTime(clockTime.toInstant().toString());
        }
        
        event.setSessionStats(fromJson(rs.getString("session_stats")));
        
        event.setVersion(rs.getInt("version"));
        event.setCurrent(rs.getBoolean("is_current"));
        event.setCreatedAt(rs.getTimestamp("created_at").toInstant().toString());
        event.setLeaderId(rs.getString("leader_id"));
        
        return event;
    }
    
    private ActionState mapToActionState(ResultSet rs) throws SQLException {
        ActionState actionState = new ActionState();
        actionState.setMeUuid(rs.getString("me_uuid"));
        actionState.setRulesetName(rs.getString("ruleset_name"));
        actionState.setRuleName(rs.getString("rule_name"));
        actionState.setVersion(rs.getInt("version"));
        actionState.setUpdatedAt(rs.getTimestamp("updated_at").toInstant().toString());
        
        // Deserialize actions
        String actionData = rs.getString("action_data");
        if (actionData != null && !actionData.isEmpty()) {
            try {
                List<ActionState.Action> actions = objectMapper.readValue(
                    actionData,
                    objectMapper.getTypeFactory().constructCollectionType(
                        List.class, ActionState.Action.class
                    )
                );
                actionState.setActions(actions);
            } catch (Exception e) {
                logger.error("Failed to deserialize actions", e);
                actionState.setActions(new ArrayList<>());
            }
        } else {
            actionState.setActions(new ArrayList<>());
        }
        
        return actionState;
    }
    
    private String determineStatus(ActionState actionState) {
        if (actionState.getActions() == null || actionState.getActions().isEmpty()) {
            return "PENDING";
        }
        
        boolean hasStarted = false;
        boolean allCompleted = true;
        boolean hasFailed = false;
        
        for (ActionState.Action action : actionState.getActions()) {
            if (action.getStatus() == ActionState.Action.ActionStatus.RUNNING) {
                hasStarted = true;
                allCompleted = false;
            } else if (action.getStatus() == ActionState.Action.ActionStatus.FAILED) {
                hasFailed = true;
                allCompleted = false;
            } else if (action.getStatus() != ActionState.Action.ActionStatus.SUCCESS) {
                allCompleted = false;
            }
        }
        
        if (hasFailed) return "FAILED";
        if (allCompleted) return "SUCCESS";
        if (hasStarted) return "IN_PROGRESS";
        return "PENDING";
    }
    
    private String toJson(Object obj) {
        if (obj == null) return null;
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            logger.error("Failed to serialize to JSON", e);
            return null;
        }
    }
    
    @SuppressWarnings("unchecked")
    private Map<String, Object> fromJson(String json) {
        if (json == null || json.isEmpty()) return null;
        try {
            return objectMapper.readValue(json, Map.class);
        } catch (Exception e) {
            logger.error("Failed to deserialize from JSON", e);
            return null;
        }
    }
}
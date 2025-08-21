package org.drools.ansible.rulebook.integration.ha.h2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.drools.ansible.rulebook.integration.ha.api.HAConfiguration;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.model.ActionState;
import org.drools.ansible.rulebook.integration.ha.model.EventState;
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
    
    public H2StateManager() {
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public void initialize(HAConfiguration config) {
        logger.info("Initializing H2StateManager with config: {}", config.getDbUrl());
        
        // Configure HikariCP connection pool
        HikariConfig hikariConfig = new HikariConfig();
        
        // Default to in-memory H2 if no URL provided
        String jdbcUrl = config.getDbUrl() != null 
            ? config.getDbUrl() 
            : "jdbc:h2:mem:eda_ha;DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
            
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(config.getUsername() != null ? config.getUsername() : "sa");
        hikariConfig.setPassword(config.getPassword() != null ? config.getPassword() : "");
        hikariConfig.setMaximumPoolSize(config.getConnectionPoolSize());
        hikariConfig.setConnectionTimeout(config.getConnectionTimeout());
        hikariConfig.setAutoCommit(false); // We'll manage transactions
        
        this.dataSource = new HikariDataSource(hikariConfig);
        
        // Create schema
        try {
            H2Schema.createSchema(dataSource);
            logger.info("H2 schema created successfully");
        } catch (SQLException e) {
            logger.error("Failed to create H2 schema", e);
            throw new RuntimeException("Failed to initialize H2StateManager", e);
        }
    }
    
    @Override
    public void setLeader(String leaderId) {
        this.leaderId = leaderId;
        this.isLeader = true;
        logger.info("Node set as leader with ID: {}", leaderId);
    }
    
    @Override
    public void unsetLeader() {
        this.isLeader = false;
        logger.info("Node unset as leader. Previous leader ID: {}", leaderId);
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
    public void shutdown() {
        logger.info("Shutting down H2StateManager");
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
    
    // Helper methods
    
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
            if (action.getStatus() == ActionState.Action.ActionStatus.STARTED) {
                hasStarted = true;
                allCompleted = false;
            } else if (action.getStatus() == ActionState.Action.ActionStatus.FAILED) {
                hasFailed = true;
                allCompleted = false;
            } else if (action.getStatus() != ActionState.Action.ActionStatus.COMPLETED) {
                allCompleted = false;
            }
        }
        
        if (hasFailed) return "FAILED";
        if (allCompleted) return "COMPLETED";
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
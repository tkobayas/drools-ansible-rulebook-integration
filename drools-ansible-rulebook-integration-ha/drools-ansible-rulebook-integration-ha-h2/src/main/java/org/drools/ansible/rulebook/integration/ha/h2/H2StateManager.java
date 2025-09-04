package org.drools.ansible.rulebook.integration.ha.h2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.model.EventState;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * H2 implementation of HAStateManager with simplified domain model
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
        
        // Configure HikariCP connection pool
        HikariConfig hikariConfig = new HikariConfig();
        
        // Check if custom H2 URL is provided in config
        String customH2Url = (String) config.get("db_url");
        String jdbcUrl;
        
        if (customH2Url != null) {
            jdbcUrl = customH2Url;
        } else {
            // Fallback to H2 for development/testing
            jdbcUrl = "jdbc:h2:mem:eda_ha_" + uuid + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        }
        
        logger.warn("Using H2 database for HA - not suitable for production");
        
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername("sa");
        hikariConfig.setPassword("");
        hikariConfig.setMaximumPoolSize(10);
        
        this.dataSource = new HikariDataSource(hikariConfig);
        
        try {
            H2Schema.createSchema(dataSource);
            logger.info("HA schema created successfully for UUID: {}", uuid);
            
            // Initialize or load HA stats
            loadOrCreateHAStats();
            
        } catch (SQLException e) {
            logger.error("Failed to initialize HA schema", e);
            throw new RuntimeException("Failed to initialize HA schema", e);
        }
    }
    
    @Override
    public void enableLeader(String leaderName) {
        this.leaderId = leaderName;
        this.isLeader = true;
        
        if (haStats != null) {
            haStats.setCurrentLeader(leaderName);
            persistHAStats();
        }
        
        logger.info("Leader mode enabled for: {}", leaderName);
    }
    
    @Override
    public void disableLeader(String leaderName) {
        if (leaderId != null && leaderId.equals(leaderName)) {
            this.isLeader = false;
            this.leaderId = null;
        }
        
        logger.info("Leader mode disabled for: {}", leaderName);
    }
    
    @Override
    public boolean isLeader() {
        return isLeader;
    }
    
    @Override
    public EventState getEventState(String sessionId) {
        String sql = "SELECT * FROM eda_event_state WHERE session_id = ? AND is_current = true";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, sessionId);
            ResultSet rs = ps.executeQuery();
            
            if (rs.next()) {
                EventState eventState = new EventState();
                eventState.setSessionId(rs.getString("session_id"));
                eventState.setRulebookHash(rs.getString("rulebook_hash"));
                
                String sessionStatsJson = rs.getString("session_stats");
                if (sessionStatsJson != null) {
                    try {
                        Map<String, Object> sessionStats = objectMapper.readValue(
                            sessionStatsJson, new TypeReference<Map<String, Object>>() {});
                        eventState.setSessionStats(sessionStats);
                    } catch (JsonProcessingException e) {
                        logger.warn("Failed to parse session stats JSON", e);
                    }
                }
                
                return eventState;
            }
            
        } catch (SQLException e) {
            logger.error("Failed to get event state", e);
        }
        
        return null;
    }
    
    @Override
    public void persistEventState(String sessionId, EventState eventState) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot persist event state - not leader");
        }
        
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            
            // Insert new version
            String sql = """
                INSERT INTO eda_event_state (session_id, rulebook_hash, session_stats, version, is_current, leader_id)
                VALUES (?, ?, ?, 
                    COALESCE((SELECT MAX(version) FROM eda_event_state WHERE session_id = ?), 0) + 1,
                    false, ?)
                """;
            
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, sessionId);
                ps.setString(2, eventState.getRulebookHash());
                
                String sessionStatsJson = null;
                if (eventState.getSessionStats() != null) {
                    sessionStatsJson = objectMapper.writeValueAsString(eventState.getSessionStats());
                }
                ps.setString(3, sessionStatsJson);
                ps.setString(4, sessionId);
                ps.setString(5, leaderId);
                
                ps.executeUpdate();
            }
            
            conn.commit();
            
            // Update HA stats
            if (haStats != null) {
                haStats.incrementEventsProcessed();
                persistHAStats();
            }
            
            logger.debug("Persisted event state for session: {}", sessionId);
            
        } catch (SQLException | JsonProcessingException e) {
            logger.error("Failed to persist event state", e);
            throw new RuntimeException("Failed to persist event state", e);
        }
    }
    
    @Override
    public String addMatchingEvent(MatchingEvent matchingEvent) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add matching event - not leader");
        }
        
        String meUuid = UUID.randomUUID().toString();
        matchingEvent.setMeUuid(meUuid);
        
        String sql = """
            INSERT INTO MatchingEvent (me_uuid, session_id, rule_set_name, rule_name, event_data)
            VALUES (?, ?, ?, ?, ?)
            """;
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, meUuid);
            ps.setString(2, matchingEvent.getSessionId());
            ps.setString(3, matchingEvent.getRuleSetName());
            ps.setString(4, matchingEvent.getRuleName());
            ps.setString(5, matchingEvent.getEventData());
            
            ps.executeUpdate();
            
            logger.debug("Added matching event with UUID: {} for rule: {}/{}", 
                meUuid, matchingEvent.getRuleSetName(), matchingEvent.getRuleName());
            
            return meUuid;
            
        } catch (SQLException e) {
            logger.error("Failed to add matching event", e);
            throw new RuntimeException("Failed to add matching event", e);
        }
    }
    
    @Override
    public List<MatchingEvent> getPendingMatchingEvents(String sessionId) {
        String sql = "SELECT * FROM MatchingEvent WHERE session_id = ?";
        List<MatchingEvent> events = new ArrayList<>();
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, sessionId);
            ResultSet rs = ps.executeQuery();
            
            while (rs.next()) {
                MatchingEvent event = new MatchingEvent();
                event.setMeUuid(rs.getString("me_uuid"));
                event.setSessionId(rs.getString("session_id"));
                event.setRuleSetName(rs.getString("rule_set_name"));
                event.setRuleName(rs.getString("rule_name"));
                event.setEventData(rs.getString("event_data"));
                events.add(event);
            }
            
        } catch (SQLException e) {
            logger.error("Failed to get pending matching events", e);
        }
        
        return events;
    }
    
    @Override
    public void commitState(String sessionId) {
        if (!isLeader) {
            return;
        }
        
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            
            // Mark current version as not current
            String sql1 = "UPDATE eda_event_state SET is_current = false WHERE session_id = ? AND is_current = true";
            try (PreparedStatement ps1 = conn.prepareStatement(sql1)) {
                ps1.setString(1, sessionId);
                ps1.executeUpdate();
            }
            
            // Mark latest version as current
            String sql2 = """
                UPDATE eda_event_state 
                SET is_current = true 
                WHERE session_id = ? AND version = (SELECT MAX(version) FROM eda_event_state WHERE session_id = ?)
                """;
            try (PreparedStatement ps2 = conn.prepareStatement(sql2)) {
                ps2.setString(1, sessionId);
                ps2.setString(2, sessionId);
                ps2.executeUpdate();
            }
            
            conn.commit();
            logger.debug("Committed state for session: {}", sessionId);
            
        } catch (SQLException e) {
            logger.error("Failed to commit state", e);
        }
    }
    
    @Override
    public void rollbackState(String sessionId) {
        if (!isLeader) {
            return;
        }
        
        String sql = "DELETE FROM eda_event_state WHERE session_id = ? AND is_current = false";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, sessionId);
            ps.executeUpdate();
            
            logger.debug("Rolled back state for session: {}", sessionId);
            
        } catch (SQLException e) {
            logger.error("Failed to rollback state", e);
        }
    }
    
    @Override
    public void addAction(String sessionId, String matchingUuid, int index, Map<String, Object> action) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add action - not leader");
        }
        
        String actionId = UUID.randomUUID().toString();
        
        String sql = """
            INSERT INTO ActionState (id, me_uuid, index, action_data)
            VALUES (?, ?, ?, ?)
            """;
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, actionId);
            ps.setString(2, matchingUuid);
            ps.setInt(3, index);
            ps.setString(4, objectMapper.writeValueAsString(action));
            
            ps.executeUpdate();
            
            // Update HA stats
            if (haStats != null) {
                haStats.incrementActionsProcessed();
                persistHAStats();
            }
            
            logger.debug("Added action for ME UUID: {}, index: {}", matchingUuid, index);
            
        } catch (SQLException | JsonProcessingException e) {
            logger.error("Failed to add action", e);
            throw new RuntimeException("Failed to add action", e);
        }
    }
    
    @Override
    public void updateAction(String sessionId, String matchingUuid, int index, Map<String, Object> action) {
        if (!isLeader) {
            logger.debug("Not leader - skipping action update");
            return;
        }
        
        String sql = "UPDATE ActionState SET action_data = ? WHERE me_uuid = ? AND index = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, objectMapper.writeValueAsString(action));
            ps.setString(2, matchingUuid);
            ps.setInt(3, index);
            
            int updated = ps.executeUpdate();
            if (updated > 0) {
                logger.debug("Updated action for ME UUID: {}, index: {}", matchingUuid, index);
            } else {
                logger.warn("No action found to update for ME UUID: {}, index: {}", matchingUuid, index);
            }
            
        } catch (SQLException | JsonProcessingException e) {
            logger.error("Failed to update action", e);
            throw new RuntimeException("Failed to update action", e);
        }
    }
    
    @Override
    public boolean actionExists(String sessionId, String matchingUuid, int index) {
        String sql = "SELECT COUNT(*) FROM ActionState WHERE me_uuid = ? AND index = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, matchingUuid);
            ps.setInt(2, index);
            
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
            
        } catch (SQLException e) {
            logger.error("Failed to check action existence", e);
        }
        
        return false;
    }
    
    @Override
    public Map<String, Object> getAction(String sessionId, String matchingUuid, int index) {
        String sql = "SELECT action_data FROM ActionState WHERE me_uuid = ? AND index = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, matchingUuid);
            ps.setInt(2, index);
            
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String actionData = rs.getString("action_data");
                if (actionData != null) {
                    return objectMapper.readValue(actionData, new TypeReference<Map<String, Object>>() {});
                }
            }
            
        } catch (SQLException | JsonProcessingException e) {
            logger.error("Failed to get action", e);
        }
        
        return new HashMap<>();
    }
    
    @Override
    public void deleteActions(String sessionId, String matchingUuid) {
        if (!isLeader) {
            logger.debug("Not leader - skipping action deletion");
            return;
        }
        
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            
            // Delete actions first (due to foreign key)
            String sqlActions = "DELETE FROM ActionState WHERE me_uuid = ?";
            try (PreparedStatement ps1 = conn.prepareStatement(sqlActions)) {
                ps1.setString(1, matchingUuid);
                ps1.executeUpdate();
            }
            
            // Delete matching event
            String sqlME = "DELETE FROM MatchingEvent WHERE me_uuid = ?";
            try (PreparedStatement ps2 = conn.prepareStatement(sqlME)) {
                ps2.setString(1, matchingUuid);
                ps2.executeUpdate();
            }
            
            conn.commit();
            logger.debug("Deleted matching event and actions: {}", matchingUuid);
            
        } catch (SQLException e) {
            logger.error("Failed to delete actions", e);
            throw new RuntimeException("Failed to delete actions", e);
        }
    }
    
    @Override
    public HAStats getHAStats() {
        return haStats;
    }
    
    @Override
    public void persistSessionStats(String sessionId, Map<String, Object> stats) {
        String sql = """
            INSERT INTO eda_session_stats (session_id, stats_data)
            VALUES (?, ?)
            ON DUPLICATE KEY UPDATE stats_data = VALUES(stats_data)
            """;
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, sessionId);
            ps.setString(2, objectMapper.writeValueAsString(stats));
            
            ps.executeUpdate();
            
        } catch (SQLException | JsonProcessingException e) {
            logger.error("Failed to persist session stats", e);
        }
    }
    
    @Override
    public void shutdown() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("Shutting down H2StateManager");
        }
    }
    
    // Private helper methods
    
    private void loadOrCreateHAStats() throws SQLException {
        String sql = "SELECT * FROM eda_ha_stats WHERE session_id = ?";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setString(1, haUuid != null ? haUuid : "default");
            ResultSet rs = ps.executeQuery();
            
            if (rs.next()) {
                haStats.setCurrentLeader(rs.getString("current_leader"));
                haStats.setLeaderSwitches(rs.getInt("leader_switches"));
                haStats.setCurrentTermStartedAt(rs.getString("current_term_started_at"));
                haStats.setEventsProcessedInTerm(rs.getInt("events_processed_in_term"));
                haStats.setActionsProcessedInTerm(rs.getInt("actions_processed_in_term"));
                
                logger.info("Restored HA stats from database");
            } else {
                // Create initial stats
                persistHAStats();
                logger.info("Created new HA stats");
            }
        }
    }
    
    private void persistHAStats() {
        if (haStats == null) {
            return;
        }
        
        // For H2, use MERGE statement
        String h2Sql = """
            MERGE INTO eda_ha_stats 
            (session_id, current_leader, leader_switches, current_term_started_at,
             events_processed_in_term, actions_processed_in_term, updated_at)
            KEY(session_id) VALUES (?, ?, ?, ?, ?, ?, ?)
            """;
            
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(h2Sql)) {
            
            ps.setString(1, haUuid != null ? haUuid : "default");
            ps.setString(2, haStats.getCurrentLeader());
            ps.setInt(3, haStats.getLeaderSwitches());
            ps.setString(4, haStats.getCurrentTermStartedAt());
            ps.setInt(5, haStats.getEventsProcessedInTerm());
            ps.setInt(6, haStats.getActionsProcessedInTerm());
            ps.setTimestamp(7, Timestamp.from(Instant.now()));
            
            ps.executeUpdate();
            
        } catch (SQLException e) {
            logger.error("Failed to persist HA stats", e);
        }
    }
}
package org.drools.ansible.rulebook.integration.ha.h2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.drools.ansible.rulebook.integration.ha.api.AbstractHAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

/**
 * H2 implementation of HAStateManager with simplified domain model
 */
public class H2StateManager extends AbstractHAStateManager {

    private static final Logger logger = LoggerFactory.getLogger(H2StateManager.class);

    private HikariDataSource dataSource;
    private String leaderId;
    private boolean isLeader = false;
    private String haUuid;
    private String workerName;
    private HAStats haStats;

    public H2StateManager() {
    }

    @Override
    public void initializeHA(String uuid, String workerName, Map<String, Object> postgresParams, Map<String, Object> config) {
        logger.info("Initializing HA mode with UUID: {}, workerName: {}", uuid, workerName);

        this.haUuid = uuid;
        this.workerName = workerName;
        this.haStats = new HAStats(uuid);

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

            // Initialize or load HA stats
            loadOrCreateHAStats();
        } catch (SQLException e) {
            logger.error("Failed to initialize HA schema", e);
            throw new RuntimeException("Failed to initialize HA schema", e);
        }
    }

    @Override
    public void enableLeader() {
        this.leaderId = this.workerName;
        this.isLeader = true;

        if (haStats != null) {
            haStats.setCurrentLeader(this.workerName);
            persistHAStats();
        }

        logger.info("Leader mode enabled for: {}", this.workerName);
    }

    @Override
    public void disableLeader() {
        if (leaderId != null && leaderId.equals(this.workerName)) {
            this.isLeader = false;
            this.leaderId = null;
        }

        logger.info("Leader mode disabled for: {}", this.workerName);
    }

    @Override
    public boolean isLeader() {
        return isLeader;
    }

    @Override
    public String getHaUuid() {
        return haUuid;
    }

    @Override
    public String getLeaderId() {
        return leaderId;
    }

    @Override
    public String getWorkerName() {
        return workerName;
    }

    @Override
    public SessionState getPersistedSessionState(String ruleSetName) {
        String sql = """
                SELECT *
                FROM SessionState
                WHERE ha_uuid = ? AND rule_set_name = ?
                ORDER BY version DESC
                LIMIT 1
                """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ps.setString(2, ruleSetName);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                SessionState sessionState = new SessionState();
                sessionState.setHaUuid(rs.getString("ha_uuid"));
                sessionState.setRuleSetName(rs.getString("rule_set_name"));
                sessionState.setRulebookHash(rs.getString("rulebook_hash"));

                // Handle partial events
                String partialEventsJson = rs.getString("partial_matching_events");
                if (partialEventsJson != null) {
                    try {
                        ObjectMapper objectMapper = new ObjectMapper();
                        List<EventRecord> partialEvents = objectMapper.readValue(partialEventsJson, 
                            new TypeReference<List<EventRecord>>() {});
                        sessionState.setPartialEvents(partialEvents);
                    } catch (Exception e) {
                        logger.error("Failed to deserialize partial events", e);
                    }
                }

                // Handle persisted time
                Timestamp persistedTime = rs.getTimestamp("persisted_time");
                if (persistedTime != null) {
                    sessionState.setPersistedTime(persistedTime.getTime());
                }

                // Handle metadata
                sessionState.setVersion(rs.getInt("version"));
                sessionState.setLeaderId(rs.getString("leader_id"));

                // Handle SHA tracking fields
                sessionState.setCurrentStateSHA(rs.getString("current_state_sha"));

                // Handle created_time
                Timestamp createdTime = rs.getTimestamp("created_time");
                if (createdTime != null) {
                    sessionState.setCreatedTime(createdTime.getTime());
                }

                logger.info("Loaded SessionState from database: {}", ruleSetName);

                return sessionState;
            }
        } catch (SQLException e) {
            logger.error("Failed to get SessionState", e);
        }

        return null;
    }

    @Override
    public void persistSessionState(SessionState sessionState) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot persist SessionState - not leader");
        }

        if (sessionState.getRuleSetName() == null) {
            throw new IllegalArgumentException("SessionState.ruleSetName must be set");
        }

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // Insert new version as current
            // Note: SHA is already calculated in updateInMemorySessionState() before this is called
            String sql = """
                    INSERT INTO SessionState (ha_uuid, rule_set_name, rulebook_hash, partial_matching_events, persisted_time, current_state_sha, version, created_time, leader_id)
                    VALUES (?, ?, ?, ?, ?, ?,
                        COALESCE((SELECT MAX(version) FROM SessionState WHERE ha_uuid = ? AND rule_set_name = ?), 0) + 1,
                        ?, ?)
                    """;

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, sessionState.getHaUuid());
                ps.setString(2, sessionState.getRuleSetName());
                ps.setString(3, sessionState.getRulebookHash());

                // Handle partial events
                String partialEventsJson = null;
                if (sessionState.getPartialEvents() != null) {
                    partialEventsJson = toJson(sessionState.getPartialEvents());
                }
                ps.setString(4, partialEventsJson);

                // Handle persisted time
                if (sessionState.getPersistedTime() > 0) {
                    ps.setTimestamp(5, new Timestamp(sessionState.getPersistedTime()));
                } else {
                    ps.setTimestamp(5, null);
                }

                // Handle SHA tracking fields
                ps.setString(6, sessionState.getCurrentStateSHA());

                ps.setString(7, sessionState.getHaUuid());
                ps.setString(8, sessionState.getRuleSetName());

                // Handle created_time
                if (sessionState.getCreatedTime() > 0) {
                    ps.setTimestamp(9, new Timestamp(sessionState.getCreatedTime()));
                } else {
                    ps.setTimestamp(9, new Timestamp(System.currentTimeMillis()));
                }

                ps.setString(10, sessionState.getLeaderId());

                ps.executeUpdate();
            }

            conn.commit();

            logger.debug("Persisted SessionState for haUuid: {}", haUuid);
        } catch (SQLException e) {
            logger.error("Failed to persist SessionState", e);
            throw new RuntimeException("Failed to persist SessionState", e);
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
                INSERT INTO MatchingEvent (me_uuid, ha_uuid, rule_set_name, rule_name, event_data)
                VALUES (?, ?, ?, ?, ?)
                """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, meUuid);
            ps.setString(2, matchingEvent.getHaUuid());
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
    public List<MatchingEvent> getPendingMatchingEvents() {
        String sql = "SELECT * FROM MatchingEvent WHERE ha_uuid = ?";
        List<MatchingEvent> events = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                MatchingEvent event = new MatchingEvent();
                event.setMeUuid(rs.getString("me_uuid"));
                event.setHaUuid(rs.getString("ha_uuid"));
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
    public void addActionInfo(String matchingUuid, int index, String action) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add action - not leader");
        }

        String actionId = UUID.randomUUID().toString();

        String sql = """
                INSERT INTO ActionInfo (id, me_uuid, index, action_data)
                VALUES (?, ?, ?, ?)
                """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, actionId);
            ps.setString(2, matchingUuid);
            ps.setInt(3, index);
            ps.setString(4, action);

            ps.executeUpdate();

            // Update HA stats
            if (haStats != null) {
                haStats.incrementActionsProcessed();
                persistHAStats();
            }

            logger.debug("Added action for ME UUID: {}, index: {}", matchingUuid, index);
        } catch (SQLException e) {
            logger.error("Failed to add action", e);
            throw new RuntimeException("Failed to add action", e);
        }
    }

    @Override
    public void updateActionInfo(String matchingUuid, int index, String action) {
        if (!isLeader) {
            logger.debug("Not leader - skipping action update");
            return;
        }

        String sql = "UPDATE ActionInfo SET action_data = ? WHERE me_uuid = ? AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, action);
            ps.setString(2, matchingUuid);
            ps.setInt(3, index);

            int updated = ps.executeUpdate();
            if (updated > 0) {
                logger.debug("Updated action for ME UUID: {}, index: {}", matchingUuid, index);
            } else {
                logger.warn("No action found to update for ME UUID: {}, index: {}", matchingUuid, index);
            }
        } catch (SQLException e) {
            logger.error("Failed to update action", e);
            throw new RuntimeException("Failed to update action", e);
        }
    }

    @Override
    public String getActionStatus(String matchingUuid, int index) {
        Integer status = fetchActionStatusFromDatabase(matchingUuid, index);
        return status == null ? "" : Integer.toString(status);
    }

    @Override
    public boolean actionInfoExists(String matchingUuid, int index) {
        String sql = "SELECT COUNT(*) FROM ActionInfo WHERE me_uuid = ? AND index = ?";

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
    public String getActionInfo(String matchingUuid, int index) {
        String sql = "SELECT action_data FROM ActionInfo WHERE me_uuid = ? AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, matchingUuid);
            ps.setInt(2, index);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String actionData = rs.getString("action_data");
                if (actionData != null) {
                    return actionData;
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to get action", e);
        }

        return "";
    }

    @Override
    public void deleteActionInfo(String matchingUuid) {
        if (!isLeader) {
            logger.debug("Not leader - skipping action deletion");
            return;
        }

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // Delete actions first (due to foreign key)
            String sqlActions = "DELETE FROM ActionInfo WHERE me_uuid = ?";
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
    public void shutdown() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("Shutting down H2StateManager");
        }
    }

    // Private helper methods

    private void loadOrCreateHAStats() throws SQLException {
        String sql = "SELECT * FROM HAStats WHERE ha_uuid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                haStats.setHaUuid(rs.getString("ha_uuid"));
                haStats.setCurrentLeader(rs.getString("current_leader"));
                haStats.setLeaderSwitches(rs.getInt("leader_switches"));
                haStats.setCurrentTermStartedAt(rs.getString("current_term_started_at"));
                haStats.setEventsProcessedInTerm(rs.getInt("events_processed_in_term"));
                haStats.setActionsProcessedInTerm(rs.getInt("actions_processed_in_term"));

                logger.info("Restored HA stats from database");
            } else {
                // Create initial stats
                persistHAStats();
            }
        }
    }

    @Override
    public void persistHAStats() {
        if (haStats == null) {
            return;
        }

        // Ensure haUuid is set
        if (haStats.getHaUuid() == null) {
            haStats.setHaUuid(haUuid);
        }

        // For H2, use MERGE statement
        String h2Sql = """
                MERGE INTO HAStats
                (ha_uuid, current_leader, leader_switches, current_term_started_at,
                 events_processed_in_term, actions_processed_in_term, updated_at)
                KEY(ha_uuid) VALUES (?, ?, ?, ?, ?, ?, ?)
                """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(h2Sql)) {

            ps.setString(1, haStats.getHaUuid());
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

    private Integer fetchActionStatusFromDatabase(String matchingUuid, int index) {
        String sql = "SELECT action_data FROM ActionInfo WHERE me_uuid = ? AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, matchingUuid);
            ps.setInt(2, index);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return extractStatus(rs.getString("action_data"));
            }
        } catch (SQLException e) {
            logger.error("Failed to fetch action status", e);
        }
        return null;
    }

    private Integer extractStatus(String actionJson) {
        if (actionJson == null) {
            return null;
        }
        try {
            Object statusValue = readValueAsMapOfStringAndObject(actionJson).get("status");
            if (statusValue instanceof Number number) {
                return number.intValue();
            }
            if (statusValue instanceof String str && !str.isBlank()) {
                try {
                    return Integer.parseInt(str.trim());
                } catch (NumberFormatException nfe) {
                    logger.debug("Ignoring non-numeric status value: {}", str);
                }
            }
        } catch (Exception e) {
            logger.warn("Unable to parse action status from JSON", e);
        }
        return null;
    }

}

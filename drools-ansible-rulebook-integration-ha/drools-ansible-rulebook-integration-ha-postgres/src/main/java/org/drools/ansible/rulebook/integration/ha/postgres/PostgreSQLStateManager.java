package org.drools.ansible.rulebook.integration.ha.postgres;

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
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValue;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.ACTION_INFO;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.HA_STATS;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.MATCHING_EVENT;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.SESSION_STATE;

/**
 * PostgreSQL implementation of HAStateManager with production-ready persistence
 */
public class PostgreSQLStateManager extends AbstractHAStateManager {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLStateManager.class);

    private HikariDataSource dataSource;
    private String leaderId;
    private boolean isLeader = false;
    private String haUuid;
    private String workerName;
    private HAStats haStats;

    public PostgreSQLStateManager() {
    }

    @Override
    public void initializeHA(String uuid, String workerName, Map<String, Object> dbParams, Map<String, Object> config) {
        logger.info("Initializing PostgreSQL HA mode with UUID: {}, workerName: {}", uuid, workerName);

        this.haUuid = uuid;
        this.workerName = workerName;
        this.haStats = new HAStats(uuid);

        // Parse PostgreSQL connection parameters
        String host = (String) dbParams.getOrDefault("host", "localhost");
        Object portObj = dbParams.getOrDefault("port", 5432);
        Integer port = (portObj instanceof Integer) ? (Integer) portObj : Integer.parseInt(portObj.toString());
        String database = (String) dbParams.getOrDefault("database", "eda_ha");
        String username = (String) dbParams.getOrDefault("user", "postgres");
        String password = (String) dbParams.getOrDefault("password", "");
        String sslmode = (String) dbParams.getOrDefault("sslmode", "prefer");
        String applicationName = (String) dbParams.getOrDefault("application_name", "drools-eda-ha");

        // Allow custom JDBC URL override
        String jdbcUrl;
        if (config != null && config.containsKey("db_url")) {
            jdbcUrl = (String) config.get("db_url");
            logger.info("Using custom JDBC URL from config");
        } else {
            jdbcUrl = String.format(
                "jdbc:postgresql://%s:%d/%s?sslmode=%s&ApplicationName=%s",
                host, port, database, sslmode, applicationName
            );
            logger.info("Connecting to PostgreSQL at {}:{}/{}", host, port, database);
        }

        // Configure HikariCP connection pool
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(3);  // Reduced from 10 to avoid "too many clients" in tests
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setDriverClassName("org.postgresql.Driver");

        logger.debug("PostgreSQL HAStateManager connecting to database with parameters: jdbcUrl={}, username={}, sslmode={}, applicationName={}",
            jdbcUrl, username, sslmode, applicationName);

        // PostgreSQL-specific optimizations
        hikariConfig.addDataSourceProperty("prepareThreshold", 3);
        hikariConfig.addDataSourceProperty("preparedStatementCacheQueries", 256);

        this.dataSource = new HikariDataSource(hikariConfig);

        try {
            PostgreSQLSchema.createSchema(dataSource);

            // Initialize or load HA stats
            loadOrCreateHAStats();

            logger.info("PostgreSQL HA initialization completed successfully");
        } catch (SQLException e) {
            logger.error("Failed to initialize PostgreSQL HA schema", e);
            throw new RuntimeException("Failed to initialize PostgreSQL HA schema", e);
        }
    }

    @Override
    public void enableLeader() {
        this.leaderId = this.workerName;
        this.isLeader = true;

        // HAStats should be overwritten by the persisted one. Then, adjusted by AstRulesEngine.updateGlobalSessionStats
        loadOrCreateHAStats();
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
        String sql = "SELECT * FROM " + SESSION_STATE
                + " WHERE ha_uuid = ? AND rule_set_name = ?"
                + " ORDER BY version DESC LIMIT 1";

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

                logger.info("Loaded SessionState from PostgreSQL database: {}", ruleSetName);

                return sessionState;
            }
        } catch (SQLException e) {
            logger.error("Failed to get SessionState from PostgreSQL", e);
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
            String sql = "INSERT INTO " + SESSION_STATE
                    + " (ha_uuid, rule_set_name, rulebook_hash, partial_matching_events, persisted_time, current_state_sha, version, created_time, leader_id)"
                    + " VALUES (?, ?, ?, ?, ?, ?,"
                    + " COALESCE((SELECT MAX(version) FROM " + SESSION_STATE + " WHERE ha_uuid = ? AND rule_set_name = ?), 0) + 1,"
                    + " ?, ?)";

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

            logger.debug("Persisted SessionState to PostgreSQL for haUuid: {}", haUuid);
        } catch (SQLException e) {
            logger.error("Failed to persist SessionState to PostgreSQL", e);
            throw new RuntimeException("Failed to persist SessionState to PostgreSQL", e);
        }
    }

    @Override
    public String addMatchingEvent(MatchingEvent matchingEvent) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add matching event - not leader");
        }

        // Generate UUID
        UUID meUuid = UUID.randomUUID();
        String meUuidString = meUuid.toString();
        matchingEvent.setMeUuid(meUuidString);
        if (matchingEvent.getCreatedAt() == 0L) {
            matchingEvent.setCreatedAt(System.currentTimeMillis());
        }

        String sql = "INSERT INTO " + MATCHING_EVENT
                + " (me_uuid, ha_uuid, rule_set_name, rule_name, event_data, created_at)"
                + " VALUES (?::uuid, ?, ?, ?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            // PostgreSQL: Use native UUID type
            ps.setObject(1, meUuid);
            ps.setString(2, matchingEvent.getHaUuid());
            ps.setString(3, matchingEvent.getRuleSetName());
            ps.setString(4, matchingEvent.getRuleName());
            ps.setString(5, matchingEvent.getEventData());
            ps.setLong(6, matchingEvent.getCreatedAt());

            ps.executeUpdate();

            logger.debug("Added matching event with UUID: {} for rule: {}/{}",
                         meUuidString, matchingEvent.getRuleSetName(), matchingEvent.getRuleName());

            return meUuidString;
        } catch (SQLException e) {
            logger.error("Failed to add matching event to PostgreSQL", e);
            throw new RuntimeException("Failed to add matching event to PostgreSQL", e);
        }
    }

    @Override
    public List<MatchingEvent> getPendingMatchingEvents() {
        String sql = "SELECT * FROM " + MATCHING_EVENT + " WHERE ha_uuid = ? ORDER BY created_at";
        List<MatchingEvent> events = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                MatchingEvent me = new MatchingEvent();
                // PostgreSQL: Read UUID and convert to string
                UUID meUuid = (UUID) rs.getObject("me_uuid");
                me.setMeUuid(meUuid.toString());
                me.setHaUuid(rs.getString("ha_uuid"));
                me.setRuleSetName(rs.getString("rule_set_name"));
                me.setRuleName(rs.getString("rule_name"));
                me.setEventData(rs.getString("event_data"));
                me.setCreatedAt(rs.getLong("created_at"));
                events.add(me);
            }

            logger.debug("Found {} pending matching events for haUuid: {}", events.size(), haUuid);
        } catch (SQLException e) {
            logger.error("Failed to get pending matching events from PostgreSQL", e);
        }

        return events;
    }

    @Override
    public void addActionInfo(String matchingUuid, int index, String actionData) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add action info - not leader");
        }

        // Generate UUID for ActionInfo
        UUID actionId = UUID.randomUUID();

        String sql = "INSERT INTO " + ACTION_INFO
                + " (id, ha_uuid, me_uuid, index, action_data)"
                + " VALUES (?::uuid, ?, ?::uuid, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            // PostgreSQL: Use native UUID type
            ps.setObject(1, actionId);
            ps.setString(2, haUuid);
            ps.setObject(3, UUID.fromString(matchingUuid));
            ps.setInt(4, index);
            ps.setString(5, actionData);

            ps.executeUpdate();

            if (haStats != null) {
                haStats.incrementActionsProcessed();
            }

            logger.debug("Added action info for matching event: {}, index: {}", matchingUuid, index);
        } catch (SQLException e) {
            logger.error("Failed to add action info to PostgreSQL", e);
            throw new RuntimeException("Failed to add action info to PostgreSQL", e);
        }
    }

    @Override
    public void updateActionInfo(String matchingUuid, int index, String actionData) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot update action info - not leader");
        }

        String sql = "UPDATE " + ACTION_INFO
                + " SET action_data = ?"
                + " WHERE me_uuid = ?::uuid AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, actionData);
            ps.setObject(2, UUID.fromString(matchingUuid));
            ps.setInt(3, index);

            int updated = ps.executeUpdate();

            if (updated > 0) {
                logger.debug("Updated action info for matching event: {}, index: {}", matchingUuid, index);
            } else {
                logger.warn("No action info found to update for matching event: {}, index: {}", matchingUuid, index);
            }
        } catch (SQLException e) {
            logger.error("Failed to update action info in PostgreSQL", e);
            throw new RuntimeException("Failed to update action info in PostgreSQL", e);
        }
    }

    @Override
    public boolean actionInfoExists(String matchingUuid, int index) {
        String sql = "SELECT COUNT(*) FROM " + ACTION_INFO + " WHERE me_uuid = ?::uuid AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setObject(1, UUID.fromString(matchingUuid));
            ps.setInt(2, index);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                boolean exists = rs.getInt(1) > 0;
                logger.debug("Action info exists check for {}/{}: {}", matchingUuid, index, exists);
                return exists;
            }
        } catch (SQLException e) {
            logger.error("Failed to check if action info exists in PostgreSQL", e);
        }

        return false;
    }

    @Override
    public String getActionInfo(String matchingUuid, int index) {
        String sql = "SELECT action_data FROM " + ACTION_INFO + " WHERE me_uuid = ?::uuid AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setObject(1, UUID.fromString(matchingUuid));
            ps.setInt(2, index);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String actionData = rs.getString("action_data");
                logger.debug("Retrieved action info for matching event: {}, index: {}", matchingUuid, index);
                return actionData;
            }
        } catch (SQLException e) {
            logger.error("Failed to get action info from PostgreSQL", e);
        }

        return null;
    }

    @Override
    public String getActionStatus(String matchingUuid, int index) {
        Integer status = fetchActionStatusFromDatabase(matchingUuid, index);
        return status != null ? String.valueOf(status) : null;
    }

    @Override
    public void deleteActionInfo(String matchingUuid) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot delete action info - not leader");
        }

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // Delete ActionInfo records (will cascade due to FK)
            String deleteActionInfo = "DELETE FROM " + ACTION_INFO + " WHERE me_uuid = ?::uuid";
            try (PreparedStatement ps = conn.prepareStatement(deleteActionInfo)) {
                ps.setObject(1, UUID.fromString(matchingUuid));
                ps.executeUpdate();
            }

            // Delete MatchingEvent
            String deleteMatchingEvent = "DELETE FROM " + MATCHING_EVENT + " WHERE me_uuid = ?::uuid";
            try (PreparedStatement ps = conn.prepareStatement(deleteMatchingEvent)) {
                ps.setObject(1, UUID.fromString(matchingUuid));
                ps.executeUpdate();
            }

            conn.commit();

            logger.debug("Deleted action info and matching event for UUID: {}", matchingUuid);
        } catch (SQLException e) {
            logger.error("Failed to delete action info from PostgreSQL", e);
            throw new RuntimeException("Failed to delete action info from PostgreSQL", e);
        }
    }

    @Override
    public HAStats getHAStats() {
        if (haStats != null) {
            haStats.setIncompleteMatchingEvents(countIncompleteMatchingEvents());
            haStats.setPartialEventsInMemory(countPartialEventsInMemory());
        }
        return haStats;
    }

    @Override
    public void shutdown() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("PostgreSQL HA state manager shut down");
        }
    }

    public HAStats loadOrCreateHAStats() {
        String sql = "SELECT * FROM " + HA_STATS + " WHERE ha_uuid = ?";

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
                haStats.setIncompleteMatchingEvents(rs.getInt("incomplete_matching_events"));
                haStats.setPartialEventsInMemory(rs.getInt("partial_events_in_memory"));
                String globalSessionStatsJson = rs.getString("global_session_stats");
                if (globalSessionStatsJson != null && !globalSessionStatsJson.isBlank()) {
                    haStats.setGlobalSessionStats(readValue(globalSessionStatsJson, org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats.class));
                }
                haStats.setPartialFulfilledRules(rs.getInt("partial_fulfilled_rules"));
                haStats.setSessionStateSize(rs.getLong("session_state_size"));

                logger.info("Restored HA stats from PostgreSQL database");
            } else {
                // Create initial stats
                persistHAStats();
            }
        } catch (SQLException e) {
            logger.error("Failed to load HA stats from PostgreSQL", e);
            throw new RuntimeException("Failed to load HA stats", e);
        }
        return haStats;
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

        // Calculate session state size before persisting
        Long sessionStateSize = calculateSessionStateSize();
        haStats.setSessionStateSize(sessionStateSize);
        haStats.setIncompleteMatchingEvents(countIncompleteMatchingEvents());
        haStats.setPartialEventsInMemory(countPartialEventsInMemory());
        // partialFulfilledRules is computed live in AstRulesEngine.getHAStats()
        String globalSessionStatsJson = haStats.getGlobalSessionStats() == null ? null : toJson(haStats.getGlobalSessionStats());

        // PostgreSQL: Use INSERT ... ON CONFLICT instead of MERGE
        String sql = "INSERT INTO " + HA_STATS
                + " (ha_uuid, current_leader, leader_switches, current_term_started_at,"
                + " events_processed_in_term, actions_processed_in_term, incomplete_matching_events,"
                + " partial_events_in_memory, global_session_stats, partial_fulfilled_rules, session_state_size, updated_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT (ha_uuid) DO UPDATE SET"
                + " current_leader = EXCLUDED.current_leader,"
                + " leader_switches = EXCLUDED.leader_switches,"
                + " current_term_started_at = EXCLUDED.current_term_started_at,"
                + " events_processed_in_term = EXCLUDED.events_processed_in_term,"
                + " actions_processed_in_term = EXCLUDED.actions_processed_in_term,"
                + " partial_events_in_memory = EXCLUDED.partial_events_in_memory,"
                + " incomplete_matching_events = EXCLUDED.incomplete_matching_events,"
                + " global_session_stats = EXCLUDED.global_session_stats,"
                + " partial_fulfilled_rules = EXCLUDED.partial_fulfilled_rules,"
                + " session_state_size = EXCLUDED.session_state_size,"
                + " updated_at = EXCLUDED.updated_at";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haStats.getHaUuid());
            ps.setString(2, haStats.getCurrentLeader());
            ps.setInt(3, haStats.getLeaderSwitches());
            ps.setString(4, haStats.getCurrentTermStartedAt());
            ps.setInt(5, haStats.getEventsProcessedInTerm());
            ps.setInt(6, haStats.getActionsProcessedInTerm());
            ps.setInt(7, haStats.getIncompleteMatchingEvents());
            ps.setInt(8, haStats.getPartialEventsInMemory());
            ps.setString(9, globalSessionStatsJson);
            ps.setInt(10, haStats.getPartialFulfilledRules());
            ps.setLong(11, haStats.getSessionStateSize());
            ps.setTimestamp(12, Timestamp.from(Instant.now()));

            ps.executeUpdate();

            logger.debug("Persisted HA stats to PostgreSQL");
        } catch (SQLException e) {
            logger.error("Failed to persist HA stats to PostgreSQL", e);
        }
    }

    /**
     * Calculate the size of the latest SessionState record using PostgreSQL's pg_column_size
     * @return size in bytes, or 0 if no session state exists
     */
    private Long calculateSessionStateSize() {
        String sql = "SELECT"
                + " pg_column_size(ha_uuid) +"
                + " pg_column_size(rule_set_name) +"
                + " pg_column_size(rulebook_hash) +"
                + " pg_column_size(partial_matching_events) +"
                + " pg_column_size(persisted_time) +"
                + " pg_column_size(current_state_sha) +"
                + " pg_column_size(version) +"
                + " pg_column_size(created_time) +"
                + " pg_column_size(leader_id) AS total_size"
                + " FROM " + SESSION_STATE
                + " WHERE ha_uuid = ?"
                + " ORDER BY version DESC LIMIT 1";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getLong("total_size");
            }
        } catch (SQLException e) {
            logger.warn("Failed to calculate session state size: {}", e.getMessage());
        }

        return 0L;
    }

    @Override
    public void logStartupSummary() {
        int pendingMEs = countIncompleteMatchingEvents();
        int pendingActions = countActionInfo();
        int sessionCount = countSessionStates();
        int partialEvents = countPartialEventsInMemory();
        String leader = haStats != null ? haStats.getCurrentLeader() : "unknown";
        int switches = haStats != null ? haStats.getLeaderSwitches() : 0;

        logger.info("HA startup summary [ha_uuid={}, leader={}]: {} session(s), {} partial event(s), {} pending matching event(s), {} pending action(s), leader switches: {}",
                     haUuid, leader, sessionCount, partialEvents, pendingMEs, pendingActions, switches);
    }

    private int countSessionStates() {
        String sql = "SELECT COUNT(DISTINCT rule_set_name) AS cnt FROM " + SESSION_STATE + " WHERE ha_uuid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getInt("cnt");
            }
        } catch (SQLException e) {
            logger.warn("Failed to count session states: {}", e.getMessage());
        }
        return 0;
    }

    private int countActionInfo() {
        String sql = "SELECT COUNT(*) AS cnt FROM " + ACTION_INFO + " WHERE ha_uuid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getInt("cnt");
            }
        } catch (SQLException e) {
            logger.warn("Failed to count action info: {}", e.getMessage());
        }
        return 0;
    }

    private int countIncompleteMatchingEvents() {
        String sql = "SELECT COUNT(*) AS pending FROM " + MATCHING_EVENT + " WHERE ha_uuid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getInt("pending");
            }
        } catch (SQLException e) {
            logger.warn("Failed to count incomplete matching events: {}", e.getMessage());
        }
        return 0;
    }

    private Integer fetchActionStatusFromDatabase(String matchingUuid, int index) {
        String sql = "SELECT action_data FROM " + ACTION_INFO + " WHERE me_uuid = ?::uuid AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setObject(1, UUID.fromString(matchingUuid));
            ps.setInt(2, index);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return extractStatus(rs.getString("action_data"));
            }
        } catch (SQLException e) {
            logger.error("Failed to fetch action status from PostgreSQL", e);
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

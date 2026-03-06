package org.drools.ansible.rulebook.integration.ha.h2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValue;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.ACTION_INFO;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.HA_STATS;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.MATCHING_EVENT;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.SESSION_STATE;

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

    // For debugging purposes
    public void printDatabaseContents() {
        try (var conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            try (var rs = stmt.executeQuery("SELECT ha_uuid, properties FROM " + HA_STATS)) {
                while (rs.next()) {
                    logger.info("#### HAStats row: ha_uuid=" + rs.getString("ha_uuid")
                                               + ", properties=" + rs.getString("properties"));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initializeHA(String uuid, String workerName, Map<String, Object> dbParams, Map<String, Object> config) {
        logger.info("Initializing HA mode with UUID: {}, workerName: {}", uuid, workerName);

        this.haUuid = uuid;
        this.workerName = workerName;
        this.haStats = new HAStats(uuid);

        // Configure HikariCP connection pool
        HikariConfig hikariConfig = new HikariConfig();

        // db_file_path in dbParams is required (default ./eda_ha)
        String dbFilePath = dbParams != null ? (String) dbParams.get("db_file_path") : null;
        if (dbFilePath == null || dbFilePath.isEmpty()) {
            dbFilePath = "./eda_ha";
        }
        String jdbcUrl = "jdbc:h2:file:" + dbFilePath + ";MODE=PostgreSQL";
        logger.info("Using file-backed H2 database from db_file_path: {}", dbFilePath);
        logger.warn("Using H2 database for HA - not suitable for production");

        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername("sa");
        hikariConfig.setPassword("");
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setDriverClassName("org.h2.Driver");

        this.dataSource = new HikariDataSource(hikariConfig);

        try {
            H2Schema.createSchema(dataSource);
            H2Schema.migrateSchema(dataSource);

            // Initialize or load HA stats
            loadOrCreateHAStats();
        } catch (SQLException e) {
            logger.error("Failed to initialize HA schema", e);
            throw new RuntimeException("Failed to initialize HA schema", e);
        }

        commonInit(config);
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
                + " WHERE ha_uuid = ? AND rule_set_name = ?";

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
                String partialEventsJson = decryptIfEnabled(rs.getString("partial_matching_events"));
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

                // Handle processed event IDs
                String processedEventIdsJson = rs.getString("processed_event_ids");
                if (processedEventIdsJson != null) {
                    try {
                        ObjectMapper objectMapper = new ObjectMapper();
                        List<String> processedEventIds = objectMapper.readValue(processedEventIdsJson,
                            new TypeReference<List<String>>() {});
                        sessionState.setProcessedEventIds(processedEventIds);
                    } catch (Exception e) {
                        logger.error("Failed to deserialize processed event IDs", e);
                    }
                }

                // Handle persisted time
                Timestamp persistedTime = rs.getTimestamp("persisted_time");
                if (persistedTime != null) {
                    sessionState.setPersistedTime(persistedTime.getTime());
                }

                // Handle metadata
                sessionState.setLeaderId(rs.getString("leader_id"));

                // Handle SHA tracking fields
                sessionState.setCurrentStateSHA(rs.getString("current_state_sha"));

                // Handle created_time
                Timestamp createdTime = rs.getTimestamp("created_time");
                if (createdTime != null) {
                    sessionState.setCreatedTime(createdTime.getTime());
                }

                // Handle extensibility columns
                sessionState.setMetadata(jsonToMap(rs.getString("metadata")));
                sessionState.setProperties(jsonToMap(rs.getString("properties")));
                sessionState.setSettings(jsonToMap(rs.getString("settings")));
                sessionState.setExt(jsonToMap(rs.getString("ext")));

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

        ensureVersionInMetadata(sessionState.getMetadata());

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            doSessionStateUpsert(conn, sessionState);
            conn.commit();

            logger.debug("Persisted SessionState for haUuid: {}", haUuid);
        } catch (SQLException e) {
            logger.error("Failed to persist SessionState", e);
            throw new RuntimeException("Failed to persist SessionState", e);
        }
    }

    @Override
    public void persistSessionStateAndStats(SessionState sessionState) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot persist SessionState - not leader");
        }

        if (sessionState.getRuleSetName() == null) {
            throw new IllegalArgumentException("SessionState.ruleSetName must be set");
        }

        if (haStats == null) {
            // Defensive: haStats should always be initialized before this is called on the hot path
            persistSessionState(sessionState);
            return;
        }

        ensureVersionInMetadata(sessionState.getMetadata());

        // Ensure haUuid is set on stats
        if (haStats.getHaUuid() == null) {
            haStats.setHaUuid(haUuid);
        }
        ensureVersionInMetadata(haStats.getMetadata());

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // 1. Upsert session state
            doSessionStateUpsert(conn, sessionState);

            // 2. Calculate session state size (reads the row we just wrote)
            haStats.setSessionStateSize(doCalculateSessionStateSize(conn));

            // 3. Upsert HA stats
            doHAStatsUpsert(conn);

            // Single commit for both operations
            conn.commit();

            logger.debug("Persisted SessionState and HAStats in single transaction for haUuid: {}", haUuid);
        } catch (SQLException e) {
            logger.error("Failed to persist SessionState and HAStats", e);
            throw new RuntimeException("Failed to persist SessionState and HAStats", e);
        }
    }

    @Override
    public void persistSessionStateStatsAndMatchingEvents(SessionState sessionState, List<MatchingEvent> matchingEvents) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot persist SessionState - not leader");
        }

        if (sessionState.getRuleSetName() == null) {
            throw new IllegalArgumentException("SessionState.ruleSetName must be set");
        }

        if (haStats == null) {
            // Defensive: haStats should always be initialized before this is called on the hot path
            persistSessionState(sessionState);
            for (MatchingEvent me : matchingEvents) {
                addMatchingEvent(me);
            }
            return;
        }

        ensureVersionInMetadata(sessionState.getMetadata());

        if (haStats.getHaUuid() == null) {
            haStats.setHaUuid(haUuid);
        }
        ensureVersionInMetadata(haStats.getMetadata());

        // Pre-compute encrypted data outside the transaction to minimize lock duration
        String encryptedPartialEvents = null;
        if (sessionState.getPartialEvents() != null) {
            encryptedPartialEvents = encryptIfEnabled(toJson(sessionState.getPartialEvents()));
        }

        // Pre-compute encrypted matching event data and ensure metadata versions
        List<String> encryptedEventDataList = new ArrayList<>();
        if (!matchingEvents.isEmpty()) {
            for (MatchingEvent me : matchingEvents) {
                ensureVersionInMetadata(me.getMetadata());
                encryptedEventDataList.add(encryptIfEnabled(me.getEventData()));
            }
        }

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // 1. Upsert session state (pass pre-computed encrypted data)
            doSessionStateUpsert(conn, sessionState, encryptedPartialEvents);

            // 2. Calculate session state size
            haStats.setSessionStateSize(doCalculateSessionStateSize(conn));

            // 3. Upsert HA stats
            doHAStatsUpsert(conn);

            // 4. Insert matching events
            if (!matchingEvents.isEmpty()) {
                for (int i = 0; i < matchingEvents.size(); i++) {
                    doMatchingEventInsert(conn, matchingEvents.get(i), encryptedEventDataList.get(i));
                }
            }

            // Single commit for all operations
            conn.commit();

            logger.debug("Persisted SessionState, HAStats, and {} matching events in single transaction for haUuid: {}",
                         matchingEvents.size(), haUuid);
        } catch (SQLException e) {
            logger.error("Failed to persist SessionState, HAStats, and matching events", e);
            throw new RuntimeException("Failed to persist SessionState, HAStats, and matching events", e);
        }
    }

    private void doSessionStateUpsert(Connection conn, SessionState sessionState) throws SQLException {
        String encryptedPartialEvents = null;
        if (sessionState.getPartialEvents() != null) {
            encryptedPartialEvents = encryptIfEnabled(toJson(sessionState.getPartialEvents()));
        }
        doSessionStateUpsert(conn, sessionState, encryptedPartialEvents);
    }

    private void doSessionStateUpsert(Connection conn, SessionState sessionState, String encryptedPartialEvents) throws SQLException {
        // Upsert session state (single-row-per-session design)
        // Note: SHA is already calculated in updateInMemorySessionState() before this is called
        String sql = "MERGE INTO " + SESSION_STATE
                + " (ha_uuid, rule_set_name, rulebook_hash, partial_matching_events, processed_event_ids, persisted_time, current_state_sha,"
                + " created_time, leader_id, metadata, properties, settings, ext)"
                + " KEY(ha_uuid, rule_set_name)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?,"
                + " COALESCE((SELECT created_time FROM " + SESSION_STATE + " WHERE ha_uuid = ? AND rule_set_name = ?), ?),"
                + " ?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sessionState.getHaUuid());
            ps.setString(2, sessionState.getRuleSetName());
            ps.setString(3, sessionState.getRulebookHash());

            // Handle partial events (already encrypted if applicable)
            ps.setString(4, encryptedPartialEvents);

            // Handle processed event IDs
            String processedEventIdsJson = null;
            if (sessionState.getProcessedEventIds() != null) {
                processedEventIdsJson = toJson(sessionState.getProcessedEventIds());
            }
            ps.setString(5, processedEventIdsJson);

            // Handle persisted time
            if (sessionState.getPersistedTime() > 0) {
                ps.setTimestamp(6, new Timestamp(sessionState.getPersistedTime()));
            } else {
                ps.setTimestamp(6, null);
            }

            // Handle SHA tracking fields
            ps.setString(7, sessionState.getCurrentStateSHA());

            // Subquery params for COALESCE(created_time) — preserves original on update
            ps.setString(8, sessionState.getHaUuid());
            ps.setString(9, sessionState.getRuleSetName());

            // Fallback created_time for first insert
            if (sessionState.getCreatedTime() > 0) {
                ps.setTimestamp(10, new Timestamp(sessionState.getCreatedTime()));
            } else {
                ps.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
            }

            ps.setString(11, sessionState.getLeaderId());

            // Extensibility columns
            ps.setString(12, mapToJson(sessionState.getMetadata()));
            ps.setString(13, mapToJson(sessionState.getProperties()));
            ps.setString(14, mapToJson(sessionState.getSettings()));
            ps.setString(15, mapToJson(sessionState.getExt()));

            ps.executeUpdate();
        }
    }

    @Override
    public String addMatchingEvent(MatchingEvent matchingEvent) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add matching event - not leader");
        }

        String meUuid = UUID.randomUUID().toString();
        matchingEvent.setMeUuid(meUuid);
        if (matchingEvent.getCreatedAt() == 0L) {
            matchingEvent.setCreatedAt(System.currentTimeMillis());
        }

        ensureVersionInMetadata(matchingEvent.getMetadata());

        // Pre-compute encrypted event data outside the transaction
        String encryptedEventData = encryptIfEnabled(matchingEvent.getEventData());

        try (Connection conn = dataSource.getConnection()) {
            doMatchingEventInsert(conn, matchingEvent, encryptedEventData);

            logger.debug("Added matching event with UUID: {} for rule: {}/{}",
                         meUuid, matchingEvent.getRuleSetName(), matchingEvent.getRuleName());

            return meUuid;
        } catch (SQLException e) {
            logger.error("Failed to add matching event", e);
            throw new RuntimeException("Failed to add matching event", e);
        }
    }

    @Override
    public List<String> addMatchingEvents(List<MatchingEvent> matchingEvents) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add matching events - not leader");
        }
        if (matchingEvents.isEmpty()) {
            return List.of();
        }

        // Prepare all matching events: assign UUIDs, timestamps, metadata, and encrypt outside transaction
        List<String> meUuids = new ArrayList<>();
        List<String> encryptedEventDataList = new ArrayList<>();
        for (MatchingEvent me : matchingEvents) {
            String meUuid = UUID.randomUUID().toString();
            meUuids.add(meUuid);
            me.setMeUuid(meUuid);
            if (me.getCreatedAt() == 0L) {
                me.setCreatedAt(System.currentTimeMillis());
            }
            ensureVersionInMetadata(me.getMetadata());
            encryptedEventDataList.add(encryptIfEnabled(me.getEventData()));
        }

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            for (int i = 0; i < matchingEvents.size(); i++) {
                doMatchingEventInsert(conn, matchingEvents.get(i), encryptedEventDataList.get(i));
            }
            conn.commit();

            for (String meUuid : meUuids) {
                logger.debug("Added matching event with UUID: {}", meUuid);
            }
            return meUuids;
        } catch (SQLException e) {
            logger.error("Failed to add matching events", e);
            throw new RuntimeException("Failed to add matching events", e);
        }
    }

    private void doMatchingEventInsert(Connection conn, MatchingEvent matchingEvent, String encryptedEventData) throws SQLException {
        String sql = "INSERT INTO " + MATCHING_EVENT
                + " (me_uuid, ha_uuid, rule_set_name, rule_name, event_data, created_at,"
                + " metadata, properties, settings, ext)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, matchingEvent.getMeUuid());
            ps.setString(2, matchingEvent.getHaUuid());
            ps.setString(3, matchingEvent.getRuleSetName());
            ps.setString(4, matchingEvent.getRuleName());
            ps.setString(5, encryptedEventData);
            ps.setLong(6, matchingEvent.getCreatedAt());
            ps.setString(7, mapToJson(matchingEvent.getMetadata()));
            ps.setString(8, mapToJson(matchingEvent.getProperties()));
            ps.setString(9, mapToJson(matchingEvent.getSettings()));
            ps.setString(10, mapToJson(matchingEvent.getExt()));

            ps.executeUpdate();
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
                MatchingEvent event = new MatchingEvent();
                event.setMeUuid(rs.getString("me_uuid"));
                event.setHaUuid(rs.getString("ha_uuid"));
                event.setRuleSetName(rs.getString("rule_set_name"));
                event.setRuleName(rs.getString("rule_name"));
                event.setEventData(decryptIfEnabled(rs.getString("event_data")));
                event.setCreatedAt(rs.getLong("created_at"));
                event.setMetadata(jsonToMap(rs.getString("metadata")));
                event.setProperties(jsonToMap(rs.getString("properties")));
                event.setSettings(jsonToMap(rs.getString("settings")));
                event.setExt(jsonToMap(rs.getString("ext")));
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

        // Pre-compute encryption outside the transaction
        String encryptedAction = encryptIfEnabled(action);
        String metadataJson = mapToJson(Map.of(DROOLS_VERSION_KEY, DROOLS_VERSION));

        String sql = "INSERT INTO " + ACTION_INFO
                + " (id, ha_uuid, me_uuid, index, action_data,"
                + " metadata, properties, settings, ext)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, actionId);
                ps.setString(2, haUuid);
                ps.setString(3, matchingUuid);
                ps.setInt(4, index);
                ps.setString(5, encryptedAction);
                ps.setString(6, metadataJson);
                ps.setString(7, "{}");
                ps.setString(8, "{}");
                ps.setString(9, "{}");

                ps.executeUpdate();
            }

            if (haStats != null) {
                haStats.incrementActionsProcessed();
                haStats.setSessionStateSize(doCalculateSessionStateSize(conn));
                ensureVersionInMetadata(haStats.getMetadata());
                doHAStatsUpsert(conn);
            }

            conn.commit();
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

        String sql = "UPDATE " + ACTION_INFO + " SET action_data = ? WHERE me_uuid = ? AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, encryptIfEnabled(action));
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
        String sql = "SELECT COUNT(*) FROM " + ACTION_INFO + " WHERE me_uuid = ? AND index = ?";

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
        String sql = "SELECT action_data FROM " + ACTION_INFO + " WHERE me_uuid = ? AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, matchingUuid);
            ps.setInt(2, index);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String actionData = decryptIfEnabled(rs.getString("action_data"));
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
            String sqlActions = "DELETE FROM " + ACTION_INFO + " WHERE me_uuid = ?";
            try (PreparedStatement ps1 = conn.prepareStatement(sqlActions)) {
                ps1.setString(1, matchingUuid);
                ps1.executeUpdate();
            }

            // Delete matching event
            String sqlME = "DELETE FROM " + MATCHING_EVENT + " WHERE me_uuid = ?";
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
            logger.info("Shutting down H2StateManager");
        }
    }

    /**
     * Force H2 to fully close the database and release all JVM-level caches.
     * This executes the H2 SHUTDOWN command before closing the connection pool.
     * Useful in tests to ensure file-backed databases are completely released
     * between test runs, preventing stale data from H2's internal database cache.
     */
    public void shutdownCompletely() {
        if (dataSource != null && !dataSource.isClosed()) {
            try (Connection conn = dataSource.getConnection();
                 var stmt = conn.createStatement()) {
                stmt.execute("SHUTDOWN");
            } catch (SQLException e) {
                logger.debug("H2 SHUTDOWN command failed (may already be closed): {}", e.getMessage());
            }
            dataSource.close();
            logger.info("Shutting down H2StateManager completely");
        }
    }

    // Private helper methods

    public HAStats loadOrCreateHAStats() {
        String sql = "SELECT * FROM " + HA_STATS + " WHERE ha_uuid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                haStats.setHaUuid(rs.getString("ha_uuid"));
                populateHAStatsFromJson(rs.getString("properties"), haStats);
                haStats.setMetadata(jsonToMap(rs.getString("metadata")));
                haStats.setSettings(jsonToMap(rs.getString("settings")));
                haStats.setExt(jsonToMap(rs.getString("ext")));
                logger.info("Restored HA stats from database");
            } else {
                // Create initial stats
                persistHAStats();
            }
        } catch (SQLException e) {
            logger.error("Failed to load HA stats", e);
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
        // partialFulfilledRules is computed live in AstRulesEngine.getHAStats()

        ensureVersionInMetadata(haStats.getMetadata());

        try (Connection conn = dataSource.getConnection()) {
            doHAStatsUpsert(conn);
        } catch (SQLException e) {
            logger.error("Failed to persist HA stats", e);
        }
    }

    /**
     * Calculate the size of the latest SessionState record using H2's OCTET_LENGTH
     * @return size in bytes, or 0 if no session state exists
     */
    private Long calculateSessionStateSize() {
        try (Connection conn = dataSource.getConnection()) {
            return doCalculateSessionStateSize(conn);
        } catch (SQLException e) {
            logger.warn("Failed to calculate session state size: {}", e.getMessage());
        }
        return 0L;
    }

    private Long doCalculateSessionStateSize(Connection conn) throws SQLException {
        String sql = "SELECT"
                + " OCTET_LENGTH(ha_uuid) +"
                + " OCTET_LENGTH(rule_set_name) +"
                + " COALESCE(OCTET_LENGTH(rulebook_hash), 0) +"
                + " COALESCE(OCTET_LENGTH(partial_matching_events), 0) +"
                + " COALESCE(OCTET_LENGTH(metadata), 0) +"
                + " COALESCE(OCTET_LENGTH(properties), 0) +"
                + " COALESCE(OCTET_LENGTH(settings), 0) +"
                + " COALESCE(OCTET_LENGTH(ext), 0) +"
                + " 8 + 8 + 8 + 8 AS total_size"
                + " FROM " + SESSION_STATE
                + " WHERE ha_uuid = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getLong("total_size");
            }
        }
        return 0L;
    }

    private void doHAStatsUpsert(Connection conn) throws SQLException {
        String propertiesJson = haStatsToJson(haStats);

        String h2Sql = "MERGE INTO " + HA_STATS
                + " (ha_uuid, properties, updated_at, metadata, settings, ext)"
                + " KEY(ha_uuid) VALUES (?, ?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(h2Sql)) {
            ps.setString(1, haStats.getHaUuid());
            ps.setString(2, propertiesJson);
            ps.setTimestamp(3, Timestamp.from(Instant.now()));
            ps.setString(4, mapToJson(haStats.getMetadata()));
            ps.setString(5, mapToJson(haStats.getSettings()));
            ps.setString(6, mapToJson(haStats.getExt()));

            ps.executeUpdate();
        }
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
        String sql = "SELECT action_data FROM " + ACTION_INFO + " WHERE me_uuid = ? AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, matchingUuid);
            ps.setInt(2, index);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return extractStatus(decryptIfEnabled(rs.getString("action_data")));
            }
        } catch (SQLException e) {
            logger.error("Failed to fetch action status", e);
        }
        return null;
    }

    private String haStatsToJson(HAStats stats) {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("current_leader", stats.getCurrentLeader());
        props.put("leader_switches", stats.getLeaderSwitches());
        props.put("current_term_started_at", stats.getCurrentTermStartedAt());
        props.put("events_processed_in_term", stats.getEventsProcessedInTerm());
        props.put("actions_processed_in_term", stats.getActionsProcessedInTerm());
        props.put("incomplete_matching_events", stats.getIncompleteMatchingEvents());
        props.put("partial_events_in_memory", stats.getPartialEventsInMemory());
        props.put("global_session_stats", stats.getGlobalSessionStats());
        props.put("partial_fulfilled_rules", stats.getPartialFulfilledRules());
        props.put("session_state_size", stats.getSessionStateSize());
        // metadata, settings, ext are stored in separate DB columns
        return toJson(props);
    }

    private void populateHAStatsFromJson(String json, HAStats stats) {
        if (json == null || json.isBlank() || "{}".equals(json)) {
            return;
        }
        Map<String, Object> props = jsonToMap(json);
        stats.setCurrentLeader((String) props.get("current_leader"));
        stats.setLeaderSwitches(getIntFromMap(props, "leader_switches"));
        stats.setCurrentTermStartedAt((String) props.get("current_term_started_at"));
        stats.setEventsProcessedInTerm(getIntFromMap(props, "events_processed_in_term"));
        stats.setActionsProcessedInTerm(getIntFromMap(props, "actions_processed_in_term"));
        stats.setIncompleteMatchingEvents(getIntFromMap(props, "incomplete_matching_events"));
        stats.setPartialEventsInMemory(getIntFromMap(props, "partial_events_in_memory"));
        Object gss = props.get("global_session_stats");
        if (gss != null) {
            stats.setGlobalSessionStats(readValue(toJson(gss),
                    org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats.class));
        }
        stats.setPartialFulfilledRules(getIntFromMap(props, "partial_fulfilled_rules"));
        Object ssSize = props.get("session_state_size");
        stats.setSessionStateSize(ssSize instanceof Number ? ((Number) ssSize).longValue() : 0L);
        // metadata, settings, ext are loaded from separate DB columns
    }

    private int getIntFromMap(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value instanceof Number ? ((Number) value).intValue() : 0;
    }

    private String mapToJson(Map<String, Object> map) {
        if (map == null || map.isEmpty()) return "{}";
        return toJson(map);
    }

    private Map<String, Object> jsonToMap(String json) {
        if (json == null || json.isBlank() || "{}".equals(json)) return new HashMap<>();
        try {
            return readValueAsMapOfStringAndObject(json);
        } catch (Exception e) {
            logger.warn("Failed to parse JSON map, returning empty: {}", e.getMessage());
            return new HashMap<>();
        }
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

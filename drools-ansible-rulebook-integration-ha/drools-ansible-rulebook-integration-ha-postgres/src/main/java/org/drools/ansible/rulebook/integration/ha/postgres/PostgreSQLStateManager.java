package org.drools.ansible.rulebook.integration.ha.postgres;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<List<EventRecord>> EVENT_RECORD_LIST_TYPE = new TypeReference<>() {};
    private static final TypeReference<List<String>> STRING_LIST_TYPE = new TypeReference<>() {};

    /**
     * SSL key format classification. Determines how the key file is handled:
     * <ul>
     *   <li>{@code PKCS12} — .p12/.pfx files, passed through to pgjdbc as-is</li>
     *   <li>{@code DER} — .pk8/.der files, passed through to pgjdbc as-is</li>
     *   <li>{@code PEM} — PEM files (encrypted or unencrypted), converted to temporary PKCS#12</li>
     * </ul>
     */
    public enum SslKeyFormat {
        PKCS12,
        DER,
        PEM
    }

    @FunctionalInterface
    private interface SqlWork {
        void execute(Connection conn) throws SQLException;
    }

    @FunctionalInterface
    private interface SqlWorkWithResult<T> {
        T execute(Connection conn) throws SQLException;
    }

    private HikariDataSource dataSource;
    private String leaderId;
    private boolean isLeader = false;
    private String haUuid;
    private String workerName;
    private HAStats haStats;
    private Path tempP12KeystorePath;

    public PostgreSQLStateManager() {
    }

    /**
     * Returns the path to the temporary PKCS#12 keystore file created during SSL key conversion,
     * or {@code null} if no conversion was performed.
     */
    public Path getTempP12KeystorePath() {
        return tempP12KeystorePath;
    }

    // ── Transaction helpers ─────────────────────────────────────────────

    private void executeInTransaction(String errorMessage, SqlWork work) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                work.execute(conn);
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            logger.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    private <T> T executeInTransactionWithResult(String errorMessage, SqlWorkWithResult<T> work) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                T result = work.execute(conn);
                conn.commit();
                return result;
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            logger.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    // ── Initialization ──────────────────────────────────────────────────

    @Override
    public void initializeHA(String uuid, String workerName, Map<String, Object> dbParams, Map<String, Object> config) {
        logger.info("Initializing PostgreSQL HA mode with UUID: {}, workerName: {}", uuid, workerName);

        this.haUuid = uuid;
        this.workerName = workerName;
        this.haStats = new HAStats(uuid);

        // buildDataSource may throw UnsupportedOperationException/IllegalArgumentException
        // for invalid SSL config — let those propagate directly without wrapping.
        this.dataSource = buildDataSource(dbParams);

        try {
            PostgreSQLSchema.createSchema(dataSource);
            PostgreSQLSchema.migrateSchema(dataSource);

            loadOrCreateHAStats();

            logger.info("PostgreSQL HA initialization completed successfully");

            commonInit(config);
        } catch (Exception e) {
            if (tempP12KeystorePath != null) {
                PemToKeyStoreConverter.cleanup(tempP12KeystorePath);
                tempP12KeystorePath = null;
            }
            if (dataSource != null && !dataSource.isClosed()) {
                dataSource.close();
            }
            logger.error("Failed to initialize PostgreSQL HA", e);
            throw new RuntimeException("Failed to initialize PostgreSQL HA", e);
        }
    }

    private HikariDataSource buildDataSource(Map<String, Object> dbParams) {
        String host = (String) dbParams.getOrDefault("host", "localhost");
        Object portObj = dbParams.getOrDefault("port", 5432);
        Integer port = (portObj instanceof Integer) ? (Integer) portObj : Integer.parseInt(portObj.toString());
        String database = (String) dbParams.getOrDefault("database", "eda_ha");
        String username = (String) dbParams.getOrDefault("user", "postgres");
        String password = (String) dbParams.getOrDefault("password", "");
        String sslmode = (String) dbParams.getOrDefault("sslmode", "prefer");
        String applicationName = (String) dbParams.getOrDefault("application_name", "drools-eda-ha");

        String sslkey = (String) dbParams.get("sslkey");
        String sslcert = (String) dbParams.get("sslcert");
        String sslrootcert = (String) dbParams.get("sslrootcert");
        String sslpassword = (String) dbParams.get("sslpassword");

        if (sslkey != null && !sslkey.isEmpty()) {
            SslKeyFormat format = detectSslKeyFormat(sslkey);
            logger.info("SSL key format detected: {} for {}", format, sslkey);

            switch (format) {
                case PKCS12:
                    throw new UnsupportedOperationException(
                            "PKCS#12 format (.p12/.pfx) is not supported. Please use PEM format instead.");
                case DER:
                    throw new UnsupportedOperationException(
                            "DER format (.pk8/.der) is not supported. Please use PEM format instead.");
                case PEM:
                    if (sslcert == null || sslcert.isEmpty()) {
                        throw new IllegalArgumentException(
                                "sslcert is required when converting a PEM key to PKCS#12");
                    }
                    char[] passphrase;
                    if (sslpassword != null && !sslpassword.isEmpty()) {
                        passphrase = sslpassword.toCharArray();
                    } else {
                        sslpassword = UUID.randomUUID().toString();
                        passphrase = sslpassword.toCharArray();
                    }
                    tempP12KeystorePath = PemToKeyStoreConverter.convertPemToP12(
                            sslkey, sslcert, passphrase);
                    sslkey = tempP12KeystorePath.toString();
                    logger.info("Converted PEM key to PKCS#12 keystore");
                    break;
            }
        }

        StringBuilder jdbcUrlBuilder = new StringBuilder(
            String.format("jdbc:postgresql://%s:%d/%s?sslmode=%s&ApplicationName=%s",
                host, port, database, sslmode, applicationName));

        if (sslkey != null && !sslkey.isEmpty()) {
            jdbcUrlBuilder.append("&sslkey=").append(URLEncoder.encode(sslkey, StandardCharsets.UTF_8));
        }
        if (sslpassword != null) {
            jdbcUrlBuilder.append("&sslpassword=").append(URLEncoder.encode(sslpassword, StandardCharsets.UTF_8));
        }
        if (sslcert != null && !sslcert.isEmpty()) {
            jdbcUrlBuilder.append("&sslcert=").append(URLEncoder.encode(sslcert, StandardCharsets.UTF_8));
        }
        if (sslrootcert != null && !sslrootcert.isEmpty()) {
            jdbcUrlBuilder.append("&sslrootcert=").append(URLEncoder.encode(sslrootcert, StandardCharsets.UTF_8));
        }

        String jdbcUrl = jdbcUrlBuilder.toString();
        logger.info("Connecting to PostgreSQL at {}:{}/{}", host, port, database);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(3);
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setDriverClassName("org.postgresql.Driver");

        logger.debug("PostgreSQL HAStateManager connecting to database with parameters: jdbcUrl={}, username={}, sslmode={}, applicationName={}",
            maskSensitiveParams(jdbcUrl), username, sslmode, applicationName);

        hikariConfig.addDataSourceProperty("prepareThreshold", 3);
        hikariConfig.addDataSourceProperty("preparedStatementCacheQueries", 256);

        return new HikariDataSource(hikariConfig);
    }

    // ── Leader management ───────────────────────────────────────────────

    @Override
    public void enableLeader() {
        this.leaderId = this.workerName;
        this.isLeader = true;

        String selectSql = "SELECT * FROM " + HA_STATS + " WHERE ha_uuid = ?";

        executeInTransaction("Failed to enable leader", conn -> {
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setString(1, haUuid);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    populateHAStatsFromResultSet(rs);
                    logger.info("Restored HA stats from PostgreSQL database");
                }
            }

            haStats.setCurrentLeader(this.workerName);
            if (haStats.getHaUuid() == null) {
                haStats.setHaUuid(haUuid);
            }
            ensureVersionInMetadata(haStats.getMetadata());
            doHAStatsUpsert(conn);
        });

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

    // ── SessionState operations ─────────────────────────────────────────

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

                String partialEventsJson = decryptIfEnabled(rs.getString("partial_matching_events"));
                if (partialEventsJson != null) {
                    try {
                        List<EventRecord> partialEvents = OBJECT_MAPPER.readValue(partialEventsJson, EVENT_RECORD_LIST_TYPE);
                        sessionState.setPartialEvents(partialEvents);
                    } catch (Exception e) {
                        logger.error("Failed to deserialize partial events", e);
                    }
                }

                String processedEventIdsJson = rs.getString("processed_event_ids");
                if (processedEventIdsJson != null) {
                    try {
                        List<String> processedEventIds = OBJECT_MAPPER.readValue(processedEventIdsJson, STRING_LIST_TYPE);
                        sessionState.setProcessedEventIds(processedEventIds);
                    } catch (Exception e) {
                        logger.error("Failed to deserialize processed event IDs", e);
                    }
                }

                Timestamp persistedTime = rs.getTimestamp("persisted_time");
                if (persistedTime != null) {
                    sessionState.setPersistedTime(persistedTime.getTime());
                }

                sessionState.setLeaderId(rs.getString("leader_id"));
                sessionState.setCurrentStateSHA(rs.getString("current_state_sha"));

                Timestamp createdTime = rs.getTimestamp("created_time");
                if (createdTime != null) {
                    sessionState.setCreatedTime(createdTime.getTime());
                }

                sessionState.setMetadata(jsonToMap(rs.getString("metadata")));
                sessionState.setProperties(jsonToMap(rs.getString("properties")));
                sessionState.setSettings(jsonToMap(rs.getString("settings")));
                sessionState.setExt(jsonToMap(rs.getString("ext")));

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
        validateForPersist(sessionState);
        ensureVersionInMetadata(sessionState.getMetadata());

        executeInTransaction("Failed to persist SessionState to PostgreSQL", conn -> {
            doSessionStateUpsert(conn, sessionState);
        });

        logger.debug("Persisted SessionState to PostgreSQL for haUuid: {}", haUuid);
    }

    @Override
    public void persistSessionStateAndStats(SessionState sessionState) {
        validateForPersist(sessionState);

        if (haStats == null) {
            // Defensive: haStats should always be initialized before this is called on the hot path
            persistSessionState(sessionState);
            return;
        }

        ensureVersionInMetadata(sessionState.getMetadata());
        prepareHAStatsForPersist();

        executeInTransaction("Failed to persist SessionState and HAStats to PostgreSQL", conn -> {
            doSessionStateUpsert(conn, sessionState);
            doHAStatsUpsert(conn);
        });

        logger.debug("Persisted SessionState and HAStats to PostgreSQL in single transaction for haUuid: {}", haUuid);
    }

    @Override
    public void persistSessionStateStatsAndMatchingEvents(SessionState sessionState, List<MatchingEvent> matchingEvents) {
        validateForPersist(sessionState);

        if (haStats == null) {
            // Defensive: haStats should always be initialized before this is called on the hot path
            persistSessionState(sessionState);
            for (MatchingEvent me : matchingEvents) {
                addMatchingEvent(me);
            }
            return;
        }

        ensureVersionInMetadata(sessionState.getMetadata());
        prepareHAStatsForPersist();

        // Pre-compute encrypted data outside the transaction to minimize lock duration
        String encryptedPartialEvents = null;
        if (sessionState.getPartialEvents() != null) {
            encryptedPartialEvents = encryptIfEnabled(toJson(sessionState.getPartialEvents()));
        }

        List<String> encryptedEventDataList = new ArrayList<>();
        List<UUID> meUuids = new ArrayList<>();
        if (!matchingEvents.isEmpty()) {
            for (MatchingEvent me : matchingEvents) {
                ensureVersionInMetadata(me.getMetadata());
                encryptedEventDataList.add(encryptIfEnabled(me.getEventData()));
                meUuids.add(UUID.fromString(me.getMeUuid()));
            }
        }

        final String finalEncryptedPartialEvents = encryptedPartialEvents;
        executeInTransaction("Failed to persist SessionState, HAStats, and matching events to PostgreSQL", conn -> {
            doSessionStateUpsert(conn, sessionState, finalEncryptedPartialEvents);
            doHAStatsUpsert(conn);

            for (int i = 0; i < matchingEvents.size(); i++) {
                doMatchingEventInsert(conn, matchingEvents.get(i), meUuids.get(i), encryptedEventDataList.get(i));
            }
        });

        logger.debug("Persisted SessionState, HAStats, and {} matching events to PostgreSQL in single transaction for haUuid: {}",
                     matchingEvents.size(), haUuid);
    }

    private void validateForPersist(SessionState sessionState) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot persist SessionState - not leader");
        }
        if (sessionState.getRuleSetName() == null) {
            throw new IllegalArgumentException("SessionState.ruleSetName must be set");
        }
    }

    private void prepareHAStatsForPersist() {
        if (haStats.getHaUuid() == null) {
            haStats.setHaUuid(haUuid);
        }
        ensureVersionInMetadata(haStats.getMetadata());
    }

    private void doSessionStateUpsert(Connection conn, SessionState sessionState) throws SQLException {
        String encryptedPartialEvents = null;
        if (sessionState.getPartialEvents() != null) {
            encryptedPartialEvents = encryptIfEnabled(toJson(sessionState.getPartialEvents()));
        }
        doSessionStateUpsert(conn, sessionState, encryptedPartialEvents);
    }

    private void doSessionStateUpsert(Connection conn, SessionState sessionState, String encryptedPartialEvents) throws SQLException {
        String sql = "INSERT INTO " + SESSION_STATE
                + " (ha_uuid, rule_set_name, rulebook_hash, partial_matching_events, processed_event_ids, persisted_time, current_state_sha, created_time, leader_id,"
                + " metadata, properties, settings, ext)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,"
                + " ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb)"
                + " ON CONFLICT (ha_uuid, rule_set_name) DO UPDATE SET"
                + " rulebook_hash = EXCLUDED.rulebook_hash,"
                + " partial_matching_events = EXCLUDED.partial_matching_events,"
                + " processed_event_ids = EXCLUDED.processed_event_ids,"
                + " persisted_time = EXCLUDED.persisted_time,"
                + " current_state_sha = EXCLUDED.current_state_sha,"
                + " leader_id = EXCLUDED.leader_id,"
                + " metadata = EXCLUDED.metadata,"
                + " properties = EXCLUDED.properties,"
                + " settings = EXCLUDED.settings,"
                + " ext = EXCLUDED.ext";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sessionState.getHaUuid());
            ps.setString(2, sessionState.getRuleSetName());
            ps.setString(3, sessionState.getRulebookHash());
            ps.setString(4, encryptedPartialEvents);

            String processedEventIdsJson = null;
            if (sessionState.getProcessedEventIds() != null) {
                processedEventIdsJson = toJson(sessionState.getProcessedEventIds());
            }
            ps.setString(5, processedEventIdsJson);

            if (sessionState.getPersistedTime() > 0) {
                ps.setTimestamp(6, new Timestamp(sessionState.getPersistedTime()));
            } else {
                ps.setTimestamp(6, null);
            }

            ps.setString(7, sessionState.getCurrentStateSHA());

            if (sessionState.getCreatedTime() > 0) {
                ps.setTimestamp(8, new Timestamp(sessionState.getCreatedTime()));
            } else {
                ps.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
            }

            ps.setString(9, sessionState.getLeaderId());

            ps.setString(10, mapToJson(sessionState.getMetadata()));
            ps.setString(11, mapToJson(sessionState.getProperties()));
            ps.setString(12, mapToJson(sessionState.getSettings()));
            ps.setString(13, mapToJson(sessionState.getExt()));

            ps.executeUpdate();
        }
    }

    // ── MatchingEvent operations ────────────────────────────────────────

    @Override
    public String addMatchingEvent(MatchingEvent matchingEvent) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add matching event - not leader");
        }

        UUID meUuid = UUID.randomUUID();
        String meUuidString = meUuid.toString();
        matchingEvent.setMeUuid(meUuidString);
        if (matchingEvent.getCreatedAt() == 0L) {
            matchingEvent.setCreatedAt(System.currentTimeMillis());
        }

        ensureVersionInMetadata(matchingEvent.getMetadata());

        String encryptedEventData = encryptIfEnabled(matchingEvent.getEventData());

        executeInTransaction("Failed to add matching event to PostgreSQL", conn -> {
            doMatchingEventInsert(conn, matchingEvent, meUuid, encryptedEventData);
        });

        logger.debug("Added matching event with UUID: {} for rule: {}/{}",
                     meUuidString, matchingEvent.getRuleSetName(), matchingEvent.getRuleName());

        return meUuidString;
    }

    @Override
    public List<String> addMatchingEvents(List<MatchingEvent> matchingEvents) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add matching events - not leader");
        }
        if (matchingEvents.isEmpty()) {
            return List.of();
        }

        List<UUID> meUuids = new ArrayList<>();
        List<String> encryptedEventDataList = new ArrayList<>();
        for (MatchingEvent me : matchingEvents) {
            UUID meUuid = UUID.randomUUID();
            meUuids.add(meUuid);
            me.setMeUuid(meUuid.toString());
            if (me.getCreatedAt() == 0L) {
                me.setCreatedAt(System.currentTimeMillis());
            }
            ensureVersionInMetadata(me.getMetadata());
            encryptedEventDataList.add(encryptIfEnabled(me.getEventData()));
        }

        executeInTransaction("Failed to add matching events to PostgreSQL", conn -> {
            for (int i = 0; i < matchingEvents.size(); i++) {
                doMatchingEventInsert(conn, matchingEvents.get(i), meUuids.get(i), encryptedEventDataList.get(i));
            }
        });

        List<String> uuids = new ArrayList<>();
        for (UUID meUuid : meUuids) {
            String uuidStr = meUuid.toString();
            uuids.add(uuidStr);
            logger.debug("Added matching event with UUID: {}", uuidStr);
        }
        return uuids;
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
                UUID meUuid = (UUID) rs.getObject("me_uuid");
                me.setMeUuid(meUuid.toString());
                me.setHaUuid(rs.getString("ha_uuid"));
                me.setRuleSetName(rs.getString("rule_set_name"));
                me.setRuleName(rs.getString("rule_name"));
                me.setEventData(decryptIfEnabled(rs.getString("event_data")));
                me.setCreatedAt(rs.getLong("created_at"));
                me.setMetadata(jsonToMap(rs.getString("metadata")));
                me.setProperties(jsonToMap(rs.getString("properties")));
                me.setSettings(jsonToMap(rs.getString("settings")));
                me.setExt(jsonToMap(rs.getString("ext")));
                events.add(me);
            }

            logger.debug("Found {} pending matching events for haUuid: {}", events.size(), haUuid);
        } catch (SQLException e) {
            logger.error("Failed to get pending matching events from PostgreSQL", e);
        }

        return events;
    }

    private void doMatchingEventInsert(Connection conn, MatchingEvent matchingEvent, UUID meUuid, String encryptedEventData) throws SQLException {
        String sql = "INSERT INTO " + MATCHING_EVENT
                + " (me_uuid, ha_uuid, rule_set_name, rule_name, event_data, created_at,"
                + " metadata, properties, settings, ext)"
                + " VALUES (?::uuid, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?::jsonb, ?::jsonb)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, meUuid);
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

    // ── ActionInfo operations ───────────────────────────────────────────

    @Override
    public void addActionInfo(String matchingUuid, int index, String actionData) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add action info - not leader");
        }

        UUID actionId = UUID.randomUUID();

        String encryptedActionData = encryptIfEnabled(actionData);
        String metadataJson = mapToJson(Map.of(DROOLS_VERSION_KEY, DROOLS_VERSION));

        String sql = "INSERT INTO " + ACTION_INFO
                + " (id, ha_uuid, me_uuid, index, action_data,"
                + " metadata, properties, settings, ext)"
                + " VALUES (?::uuid, ?, ?::uuid, ?, ?, ?::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb)";

        executeInTransaction("Failed to add action info to PostgreSQL", conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setObject(1, actionId);
                ps.setString(2, haUuid);
                ps.setObject(3, UUID.fromString(matchingUuid));
                ps.setInt(4, index);
                ps.setString(5, encryptedActionData);
                ps.setString(6, metadataJson);

                ps.executeUpdate();
            }

            if (haStats != null) {
                haStats.incrementActionsProcessed();
                ensureVersionInMetadata(haStats.getMetadata());
                doHAStatsUpsert(conn);
            }
        });

        logger.debug("Added action info for matching event: {}, index: {}", matchingUuid, index);
    }

    @Override
    public void updateActionInfo(String matchingUuid, int index, String actionData) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot update action info - not leader");
        }

        String sql = "UPDATE " + ACTION_INFO
                + " SET action_data = ?"
                + " WHERE me_uuid = ?::uuid AND index = ?";

        executeInTransaction("Failed to update action info in PostgreSQL", conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, encryptIfEnabled(actionData));
                ps.setObject(2, UUID.fromString(matchingUuid));
                ps.setInt(3, index);

                int updated = ps.executeUpdate();

                if (updated > 0) {
                    logger.debug("Updated action info for matching event: {}, index: {}", matchingUuid, index);
                } else {
                    logger.warn("No action info found to update for matching event: {}, index: {}", matchingUuid, index);
                }
            }
        });
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
                String actionData = decryptIfEnabled(rs.getString("action_data"));
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

        executeInTransaction("Failed to delete action info from PostgreSQL", conn -> {
            String deleteActionInfo = "DELETE FROM " + ACTION_INFO + " WHERE me_uuid = ?::uuid";
            try (PreparedStatement ps = conn.prepareStatement(deleteActionInfo)) {
                ps.setObject(1, UUID.fromString(matchingUuid));
                ps.executeUpdate();
            }

            String deleteMatchingEvent = "DELETE FROM " + MATCHING_EVENT + " WHERE me_uuid = ?::uuid";
            try (PreparedStatement ps = conn.prepareStatement(deleteMatchingEvent)) {
                ps.setObject(1, UUID.fromString(matchingUuid));
                ps.executeUpdate();
            }
        });

        logger.debug("Deleted action info and matching event for UUID: {}", matchingUuid);
    }

    // ── HAStats operations ──────────────────────────────────────────────

    @Override
    public HAStats getHAStats() {
        if (haStats != null) {
            haStats.setPartialEventsInMemory(countPartialEventsInMemory());
        }
        return haStats;
    }

    @Override
    public void refreshHAStats() {
        if (haStats != null) {
            haStats.setIncompleteMatchingEvents(countRows("SELECT COUNT(*) AS cnt FROM " + MATCHING_EVENT + " WHERE ha_uuid = ?"));
            haStats.setSessionStateSize(calculateSessionStateSize());
        }
    }

    public HAStats loadOrCreateHAStats() {
        String sql = "SELECT * FROM " + HA_STATS + " WHERE ha_uuid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                populateHAStatsFromResultSet(rs);
                logger.info("Restored HA stats from PostgreSQL database");
            } else {
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

        prepareHAStatsForPersist();

        executeInTransaction("Failed to persist HA stats to PostgreSQL", conn -> {
            doHAStatsUpsert(conn);
        });

        logger.debug("Persisted HA stats to PostgreSQL");
    }

    private void populateHAStatsFromResultSet(ResultSet rs) throws SQLException {
        haStats.setHaUuid(rs.getString("ha_uuid"));
        populateHAStatsFromJson(rs.getString("properties"), haStats);
        haStats.setMetadata(jsonToMap(rs.getString("metadata")));
        haStats.setSettings(jsonToMap(rs.getString("settings")));
        haStats.setExt(jsonToMap(rs.getString("ext")));
    }

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
                + " pg_column_size(ha_uuid) +"
                + " pg_column_size(rule_set_name) +"
                + " pg_column_size(rulebook_hash) +"
                + " pg_column_size(partial_matching_events) +"
                + " COALESCE(pg_column_size(metadata), 0) +"
                + " COALESCE(pg_column_size(properties), 0) +"
                + " COALESCE(pg_column_size(settings), 0) +"
                + " COALESCE(pg_column_size(ext), 0) +"
                + " pg_column_size(persisted_time) +"
                + " pg_column_size(current_state_sha) +"
                + " pg_column_size(created_time) +"
                + " pg_column_size(leader_id) AS total_size"
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

        String sql = "INSERT INTO " + HA_STATS
                + " (ha_uuid, properties, updated_at, metadata, settings, ext)"
                + " VALUES (?, ?::jsonb, ?, ?::jsonb, ?::jsonb, ?::jsonb)"
                + " ON CONFLICT (ha_uuid) DO UPDATE SET"
                + " properties = EXCLUDED.properties,"
                + " updated_at = EXCLUDED.updated_at,"
                + " metadata = EXCLUDED.metadata,"
                + " settings = EXCLUDED.settings,"
                + " ext = EXCLUDED.ext";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, haStats.getHaUuid());
            ps.setString(2, propertiesJson);
            ps.setTimestamp(3, Timestamp.from(Instant.now()));
            ps.setString(4, mapToJson(haStats.getMetadata()));
            ps.setString(5, mapToJson(haStats.getSettings()));
            ps.setString(6, mapToJson(haStats.getExt()));

            ps.executeUpdate();
        }
    }

    // ── Startup / Shutdown ──────────────────────────────────────────────

    @Override
    public void logStartupSummary() {
        int pendingMEs = countRows("SELECT COUNT(*) AS cnt FROM " + MATCHING_EVENT + " WHERE ha_uuid = ?");
        int pendingActions = countRows("SELECT COUNT(*) AS cnt FROM " + ACTION_INFO + " WHERE ha_uuid = ?");
        int sessionCount = countRows("SELECT COUNT(DISTINCT rule_set_name) AS cnt FROM " + SESSION_STATE + " WHERE ha_uuid = ?");
        int partialEvents = countPartialEventsInMemory();
        String leader = haStats != null ? haStats.getCurrentLeader() : "unknown";
        int switches = haStats != null ? haStats.getLeaderSwitches() : 0;

        logger.info("HA startup summary [ha_uuid={}, leader={}]: {} session(s), {} partial event(s), {} pending matching event(s), {} pending action(s), leader switches: {}",
                     haUuid, leader, sessionCount, partialEvents, pendingMEs, pendingActions, switches);
    }

    @Override
    public void shutdown() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("PostgreSQL HA state manager shut down");
        }
        if (tempP12KeystorePath != null) {
            PemToKeyStoreConverter.cleanup(tempP12KeystorePath);
            tempP12KeystorePath = null;
        }
    }

    // ── Counting / query helpers ────────────────────────────────────────

    private int countRows(String sql) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.warn("Failed to execute count query: {}", e.getMessage());
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
                return extractStatus(decryptIfEnabled(rs.getString("action_data")));
            }
        } catch (SQLException e) {
            logger.error("Failed to fetch action status from PostgreSQL", e);
        }
        return null;
    }

    // ── SSL helpers ─────────────────────────────────────────────────────

    /**
     * Detect the SSL key format based on file extension.
     * PKCS#12 and DER are identified by extension; everything else is treated as PEM.
     */
    public static SslKeyFormat detectSslKeyFormat(String sslkeyPath) {
        String lower = sslkeyPath.toLowerCase();
        if (lower.endsWith(".p12") || lower.endsWith(".pfx")) {
            return SslKeyFormat.PKCS12;
        }
        if (lower.endsWith(".pk8") || lower.endsWith(".der")) {
            return SslKeyFormat.DER;
        }
        return SslKeyFormat.PEM;
    }

    private static String maskSensitiveParams(String url) {
        if (url == null) {
            return null;
        }
        return url.replaceAll("(sslpassword=)[^&]*", "$1***");
    }

    // ── JSON serialization helpers ──────────────────────────────────────

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

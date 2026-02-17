package org.drools.ansible.rulebook.integration.ha.tests;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.ha.postgres.PostgreSQLSchema;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class TestUtils {

    // H2 file-backed database path for tests (shared between nodes via same file)
    public static final String TEST_H2_FILE_PATH = "./target/h2-test/eda_ha";

    // PostgreSQL Configuration (for Testcontainers)
    // These will be populated by PostgreSQLTestBase when container starts
    private static volatile Map<String, Object> dbParams = null;
    private static volatile Map<String, Object> dbHAConfig = null;

    public static void setDbTestConfig(Map<String, Object> params, Map<String, Object> config) {
        dbParams = params;
        dbHAConfig = config;
    }

    public static Map<String, Object> getDbParams() {
        return dbParams != null ? dbParams : new HashMap<>();
    }

    public static Map<String, Object> getDbHAConfig() {
        return dbHAConfig != null ? dbHAConfig : Map.of("write_after", 1);
    }

    private TestUtils() {
    }

    public static MatchingEvent createMatchingEvent(String haUuid, String rulesetName,
                                              String ruleName, Map<String, Object> matchingFacts) {
        MatchingEvent me = new MatchingEvent();
        me.setHaUuid(haUuid);
        me.setRuleSetName(rulesetName);
        me.setRuleName(ruleName);

        // Serialize matching facts to JSON
        String eventDataJson = toJson(matchingFacts);
        me.setEventData(eventDataJson);
        return me;
    }

    /**
     * Force H2 to fully close the database and remove it from the JVM-level cache.
     * H2 maintains an internal static map of open databases. Without an explicit SHUTDOWN,
     * reopening the same file path may return cached (stale) data even after file deletion.
     * Uses IFEXISTS=TRUE to avoid creating a new database if none exists.
     */
    public static void shutdownH2Database() {
        String jdbcUrl = "jdbc:h2:file:" + TEST_H2_FILE_PATH + ";MODE=PostgreSQL;IFEXISTS=TRUE";
        try (var conn = DriverManager.getConnection(jdbcUrl, "sa", "");
             var stmt = conn.createStatement()) {
            stmt.execute("SHUTDOWN");
        } catch (Exception e) {
            // Database might already be closed or file doesn't exist - that's OK
        }
    }

    /**
     * Delete H2 database files for the test file path.
     * H2 creates .mv.db and optionally .trace.db files.
     */
    public static void deleteH2Files() {
        Path dir = Path.of(TEST_H2_FILE_PATH).getParent();
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        String baseName = Path.of(TEST_H2_FILE_PATH).getFileName().toString();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, baseName + ".*")) {
            for (Path file : stream) {
                Files.deleteIfExists(file);
            }
        } catch (IOException e) {
            System.err.println("Failed to delete H2 files: " + e.getMessage());
        }
    }

    public static void dropPostgresTables() {
        if (dbParams == null || dbParams.isEmpty()) {
            return; // No PostgreSQL configured
        }

        String host = (String) dbParams.get("host");
        Integer port = (Integer) dbParams.get("port");
        String database = (String) dbParams.get("database");
        String username = (String) dbParams.get("user");
        String password = (String) dbParams.get("password");

        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(1);  // Only need 1 connection for dropping tables

        try (HikariDataSource dataSource = new HikariDataSource(hikariConfig)) {
            PostgreSQLSchema.dropSchema(dataSource);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop PostgreSQL tables", e);
        }
    }

    // Add meta/uuid to event body
    public static String createEvent(String eventBody) {
        String eventUuid = UUID.randomUUID().toString();
        return eventBody.replaceFirst("\\{", "{\"meta\": {\"uuid\": \"" + eventUuid + "\"}, ");
    }

    /**
     * Extract matching_uuid from HA mode response JSON.
     * HA response format: [{"name": "rule_name", "events": {...}, "matching_uuid": "uuid-here"}]
     *
     * @param response JSON response from assertEvent/assertFact in HA mode
     * @return matching_uuid string, or null if not found or response is empty
     */
    public static String extractMatchingUuidFromResponse(String response) {
        try {
            List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(response);
            if (matchList.isEmpty()) {
                return null;
            }
            Map<String, Object> firstMatch = matchList.get(0);
            return (String) firstMatch.get("matching_uuid");
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract matching_uuid from response: " + response, e);
        }
    }

    /**
     * Extract matching_uuid from async recovery message JSON.
     * Async recovery message format:
     * {
     *   "session_id": 1,
     *   "result": {
     *     "matching_uuid": "uuid-here",
     *     "name": "rule_name",
     *     "type": "MATCHING_EVENT_RECOVERY",
     *     "ruleset_name": "ruleset_name",
     *     "events": {...}
     *   }
     * }
     *
     * @param asyncMessage JSON async recovery message
     * @return matching_uuid string, or null if not found
     */
    public static String extractMatchingUuidFromAsyncRecoveryMessage(String asyncMessage) {
        try {
            Map<String, Object> message = JsonMapper.readValueAsMapOfStringAndObject(asyncMessage);
            Map<String, Object> result = (Map<String, Object>) message.get("result");
            if (result == null) {
                return null;
            }
            return (String) result.get("matching_uuid");
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract matching_uuid from async recovery message: " + asyncMessage, e);
        }
    }
}

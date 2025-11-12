package org.drools.ansible.rulebook.integration.ha.tests;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.ha.h2.H2Schema;
import org.drools.ansible.rulebook.integration.ha.postgres.PostgreSQLSchema;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class TestUtils {

    // H2 Configuration (default for tests)
    public static final Map<String, Object> TEST_PG_CONFIG = new HashMap<>(); // Empty for H2
    public static final String TEST_H2_URL = "jdbc:h2:mem:eda_ha_test;DB_CLOSE_DELAY=-1";
    public static final Map<String, Object> TEST_HA_CONFIG = Map.of( // DB is shared between nodes
                                                                      "db_url", TEST_H2_URL, // Shared H2 database
                                                                      "write_after", 1 // Immediate persistence
    );

    // PostgreSQL Configuration (for Testcontainers)
    // These will be populated by PostgreSQLTestBase when container starts
    private static volatile Map<String, Object> postgresParams = null;
    private static volatile Map<String, Object> postgresHAConfig = null;

    public static void setPostgresTestConfig(Map<String, Object> params, Map<String, Object> config) {
        postgresParams = params;
        postgresHAConfig = config;
    }

    public static Map<String, Object> getPostgresParams() {
        return postgresParams != null ? postgresParams : new HashMap<>();
    }

    public static Map<String, Object> getPostgresHAConfig() {
        return postgresHAConfig != null ? postgresHAConfig : Map.of("write_after", 1);
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

    public static void dropTables() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(TEST_H2_URL);
        hikariConfig.setUsername("sa");
        hikariConfig.setPassword("");
        hikariConfig.setMaximumPoolSize(10);

        try (HikariDataSource dataSource = new HikariDataSource(hikariConfig)) {
            H2Schema.dropSchema(dataSource);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void dropPostgresTables() {
        if (postgresParams == null || postgresParams.isEmpty()) {
            return; // No PostgreSQL configured
        }

        String host = (String) postgresParams.get("host");
        Integer port = (Integer) postgresParams.get("port");
        String database = (String) postgresParams.get("database");
        String username = (String) postgresParams.get("user");
        String password = (String) postgresParams.get("password");

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

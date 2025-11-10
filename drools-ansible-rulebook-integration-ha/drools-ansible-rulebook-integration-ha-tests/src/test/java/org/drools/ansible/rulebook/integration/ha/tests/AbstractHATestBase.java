package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.Collections;
import java.util.Map;

import org.testcontainers.containers.PostgreSQLContainer;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

/**
 * Abstract base class for all HA tests.
 * Provides common database setup logic for both H2 and PostgreSQL.
 * Supports switching databases via system property 'test.db.type'.
 *
 * Subclasses should call initializePostgres() or initializeH2() in their static block
 * with appropriate database name.
 *
 * As long as HA tests involve HAStateManager or AstRulesEngine, they should extend this class.
 *
 * Usage:
 * - Default (H2): mvn test
 * - PostgreSQL: mvn test -Dtest.db.type=postgres
 */
abstract class AbstractHATestBase {

    // Determine database type from system property
    protected static final String TEST_DB_TYPE = System.getProperty("test.db.type", "h2");
    protected static final boolean USE_POSTGRES = "postgres".equalsIgnoreCase(TEST_DB_TYPE) ||
                                                "postgresql".equalsIgnoreCase(TEST_DB_TYPE);

    // PostgreSQL container (only initialized if USE_POSTGRES is true)
    protected static PostgreSQLContainer<?> postgres;

    // Database configuration (populated based on TEST_DB_TYPE)
    protected static Map<String, Object> dbParams;
    protected static Map<String, Object> dbHAConfig;

    // JSON strings for AstRulesEngine API (converted from Maps)
    protected static String dbParamsJson;
    protected static String dbHAConfigJson;

    /**
     * Initialize PostgreSQL Testcontainer with specified database name.
     * This allows different test suites to use different databases for isolation.
     *
     * @param databaseName The name for the PostgreSQL database (e.g., "eda_ha_test", "eda_ha_unit_test")
     * @param testType Description for logging (e.g., "HA integration tests", "HAStateManager tests")
     */
    protected static void initializePostgres(String databaseName, String testType) {
        System.out.println("Initializing PostgreSQL Testcontainer for " + testType + "...");

        // Start Testcontainer
        postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName(databaseName)
            .withUsername("test")
            .withPassword("test")
            .withReuse(true); // Reuse across test classes for performance

        postgres.start();

        // Set system property for HAStateManagerFactory
        System.setProperty("ha.db.type", "postgres");

        // Configure parameters
        dbParams = Map.of(
            "host", postgres.getHost(),
            "port", postgres.getMappedPort(5432),
            "database", postgres.getDatabaseName(),
            "username", postgres.getUsername(),
            "password", postgres.getPassword(),
            "sslmode", "disable"
        );

        dbHAConfig = Map.of("write_after", 1);

        // Convert to JSON for AstRulesEngine API
        dbParamsJson = toJson(dbParams);
        dbHAConfigJson = toJson(dbHAConfig);

        // Configure TestUtils with PostgreSQL params
        TestUtils.setPostgresTestConfig(dbParams, dbHAConfig);

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (postgres != null && postgres.isRunning()) {
                System.out.println("Stopping PostgreSQL Testcontainer...");
                postgres.stop();
            }
            System.clearProperty("ha.db.type");
        }));

        System.out.println("PostgreSQL Testcontainer started at " +
            postgres.getHost() + ":" + postgres.getMappedPort(5432));
    }

    /**
     * Initialize H2 in-memory database configuration.
     */
    protected static void initializeH2() {
        System.out.println("Using H2 in-memory database");

        // H2 configuration
        dbParams = Collections.emptyMap(); // H2 doesn't need postgres params
        dbHAConfig = Map.of(
            "db_url", TestUtils.TEST_H2_URL,
            "write_after", 1
        );

        // Convert to JSON for AstRulesEngine API
        dbParamsJson = toJson(dbParams);
        dbHAConfigJson = toJson(dbHAConfig);
    }

    /**
     * Clean up database after each test based on database type.
     * Subclasses can call this in their @AfterEach methods.
     */
    protected void cleanupDatabase() {
        if (USE_POSTGRES) {
            TestUtils.dropPostgresTables();
        } else {
            TestUtils.dropTables();
        }
    }
}

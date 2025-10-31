package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.Collections;
import java.util.Map;

import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base class for HAStateManager unit/component tests.
 * Supports both H2 and PostgreSQL based on system property 'test.db.type'.
 *
 * Usage:
 * - Default (H2): mvn test
 * - PostgreSQL: mvn test -Dtest.db.type=postgres
 */
abstract class HAStateManagerTestBase {

    // Determine database type from system property
    protected static final String TEST_DB_TYPE = System.getProperty("test.db.type", "h2");
    protected static final boolean USE_POSTGRES = "postgres".equalsIgnoreCase(TEST_DB_TYPE) ||
                                                "postgresql".equalsIgnoreCase(TEST_DB_TYPE);

    // PostgreSQL container (only initialized if USE_POSTGRES is true)
    protected static PostgreSQLContainer<?> postgres;

    // Database configuration (populated based on TEST_DB_TYPE)
    protected static Map<String, Object> dbParams;
    protected static Map<String, Object> dbHAConfig;

    // Static initialization - runs once for all test classes
    static {
        if (USE_POSTGRES) {
            initializePostgres();
        } else {
            initializeH2();
        }
    }

    private static void initializePostgres() {
        System.out.println("Initializing PostgreSQL Testcontainer for HAStateManager tests...");

        // Start Testcontainer with different database name to avoid conflicts with HAIntegrationTestBase
        postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("eda_ha_unit_test")  // Different DB name for unit tests
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

    private static void initializeH2() {
        System.out.println("Using H2 in-memory database for HAStateManager tests");

        // H2 configuration
        dbParams = Collections.emptyMap(); // H2 doesn't need postgres params
        dbHAConfig = Map.of(
            "db_url", TestUtils.TEST_H2_URL,
            "write_after", 1
        );
    }

    /**
     * Clean up database after each test based on database type
     */
    protected void cleanupDatabase() {
        if (USE_POSTGRES) {
            TestUtils.dropPostgresTables();
        } else {
            TestUtils.dropTables();
        }
    }
}

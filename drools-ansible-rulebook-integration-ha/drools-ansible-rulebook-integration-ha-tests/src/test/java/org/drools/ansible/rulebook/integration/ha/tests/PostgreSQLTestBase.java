package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for PostgreSQL integration tests using Testcontainers
 *
 * This class automatically provisions a PostgreSQL container for tests.
 * Tests extending this class will use PostgreSQL instead of H2.
 */
@Testcontainers
public abstract class PostgreSQLTestBase {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
        .withDatabaseName("eda_ha_test")
        .withUsername("test")
        .withPassword("test")
        .withReuse(false); // Don't reuse between test classes for isolation

    @BeforeAll
    static void setupPostgres() {
        postgres.start();

        // Set system property to use PostgreSQL
        System.setProperty("ha.db.type", "postgres");

        // Configure TestUtils with PostgreSQL connection parameters
        Map<String, Object> postgresParams = Map.of(
            "host", postgres.getHost(),
            "port", postgres.getMappedPort(5432),
            "database", postgres.getDatabaseName(),
            "username", postgres.getUsername(),
            "password", postgres.getPassword(),
            "sslmode", "disable"
        );

        Map<String, Object> postgresHAConfig = Map.of(
            "write_after", 1
        );

        TestUtils.setPostgresTestConfig(postgresParams, postgresHAConfig);

        System.out.println("PostgreSQL Testcontainer started at " +
            postgres.getHost() + ":" + postgres.getMappedPort(5432));
    }

    @AfterAll
    static void teardownPostgres() {
        // Clean up system property
        System.clearProperty("ha.db.type");

        // Container will be stopped automatically by Testcontainers
        System.out.println("PostgreSQL Testcontainer stopped");
    }

    /**
     * Get PostgreSQL connection parameters for tests
     */
    protected static Map<String, Object> getPostgresParams() {
        return TestUtils.getPostgresParams();
    }

    /**
     * Get HA configuration for PostgreSQL tests
     */
    protected static Map<String, Object> getPostgresHAConfig() {
        return TestUtils.getPostgresHAConfig();
    }

    /**
     * Drop all tables in PostgreSQL (for cleanup between tests)
     */
    protected static void dropPostgresTables() {
        TestUtils.dropPostgresTables();
    }
}

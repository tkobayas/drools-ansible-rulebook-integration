package org.drools.ansible.rulebook.integration.ha.tests;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;

/**
 * Base class for AstRulesEngine HA integration tests.
 * Supports both H2 and PostgreSQL based on system property 'test.db.type'.
 *
 * Usage:
 * - Default (H2): mvn test
 * - PostgreSQL: mvn test -Dtest.db.type=postgres
 */
abstract class HAIntegrationTestBase {

    protected static final String HA_UUID = "integration-ha-1";

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

    protected AstRulesEngine rulesEngine1; // node 1
    protected AstRulesEngine rulesEngine2; // node 2

    protected long sessionId1; // node1
    protected long sessionId2; // node2

    protected AsyncConsumer consumer1; // node1
    protected AsyncConsumer consumer2; // node2

    abstract String getRuleSet();

    private static void initializePostgres() {
        System.out.println("Initializing PostgreSQL Testcontainer for HA integration tests...");

        // Start Testcontainer
        postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("eda_ha_test")
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
        System.out.println("Using H2 in-memory database for HA integration tests");

        // H2 configuration
        dbParams = Collections.emptyMap(); // H2 doesn't need postgres params
        dbHAConfig = Map.of(
            "db_url", TestUtils.TEST_H2_URL,
            "write_after", 1
        );
    }

    @BeforeEach
    void setUp() {
        System.out.println("Running test with database: " + TEST_DB_TYPE);

        rulesEngine1 = new AstRulesEngine();
        rulesEngine1.initializeHA(HA_UUID, dbParams, dbHAConfig); // The same cluster. Both nodes share same DB
        sessionId1 = rulesEngine1.createRuleset(getRuleSet(), RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new AsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());

        rulesEngine2 = new AstRulesEngine();
        rulesEngine2.initializeHA(HA_UUID, dbParams, dbHAConfig); // The same cluster. Both nodes share same DB
        sessionId2 = rulesEngine2.createRuleset(getRuleSet(), RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer2 = new AsyncConsumer("consumer2");
        consumer2.startConsuming(rulesEngine2.port());
    }

    @AfterEach
    void tearDown() {
        if (consumer1 != null) {
            consumer1.stop();
        }
        if (consumer2 != null) {
            consumer2.stop();
        }

        if (rulesEngine1 != null) {
            rulesEngine1.dispose(sessionId1);
            rulesEngine1.close(); // Close connection pools
        }
        if (rulesEngine2 != null) {
            rulesEngine2.dispose(sessionId2);
            rulesEngine2.close(); // Close connection pools
        }

        // Clean up database based on type
        if (USE_POSTGRES) {
            TestUtils.dropPostgresTables();
        } else {
            TestUtils.dropTables();
        }
    }

    // Simulate a python client that consumes async responses
    public static class AsyncConsumer {
        private volatile boolean keepReading = true;
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final String name;
        private final List<String> receivedMessages = new ArrayList<>();

        public AsyncConsumer(String name) {
            this.name = name;
        }

        public void startConsuming(int port) {
            executor.submit(() -> {
                try (Socket socket = new Socket("localhost", port)) {
                    socket.setSoTimeout(1000);
                    DataInputStream stream = new DataInputStream(socket.getInputStream());

                    while (keepReading && !Thread.currentThread().isInterrupted()) {
                        try {
                            if (stream.available() > 0) {
                                int l = stream.readInt();
                                byte[] bytes = stream.readNBytes(l);
                                String result = new String(bytes, StandardCharsets.UTF_8);
                                System.out.println(name + " - Async result: " + result);
                                receivedMessages.add(result);
                            }
                        } catch (SocketTimeoutException e) {
                            continue;
                        }
                    }
                } catch (IOException e) {
                    if (keepReading) {
                        System.err.println(name + " error: " + e.getMessage());
                    }
                }
            });
        }

        public void stop() {
            keepReading = false;
            executor.shutdown();
            try {
                if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }

        public List<String> getReceivedMessages() {
            return receivedMessages;
        }
    }


    // Helper method to create HAStateManager to assert database
    protected HAStateManager createHAStateManagerForAssertion() {
        HAStateManager manager = HAStateManagerFactory.create();
        manager.initializeHA(HA_UUID, dbParams, dbHAConfig);
        return manager;
    }

    protected String getRuleSetNameValue() {
        return (String) readValueAsMapOfStringAndObject(getRuleSet()).get("name");
    }
}

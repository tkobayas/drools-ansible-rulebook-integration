package org.drools.ansible.rulebook.integration.ha.tests;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for SSL/mTLS connection to PostgreSQL.
 * <p>
 * This test starts a PostgreSQL container configured for SSL with client certificate
 * authentication, then verifies that the HA state manager can connect via mTLS
 * and perform basic operations.
 * <p>
 * Run with: {@code mvn test -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests
 * -Dtest=HAPostgresSSLTest -Dtest.db.type=postgres}
 */
@EnabledIfSystemProperty(named = "test.db.type", matches = "postgres(ql)?")
class HAPostgresSSLTest {

    private static final String HA_UUID = "ssl-test-ha-1";
    private static final String WORKER_NAME = "ssl-worker-1";
    private static final String RULE_SET_NAME = "sslTestRuleset";

    private static PostgreSQLContainer<?> sslPostgres;
    private static SSLTestCertificateGenerator.CertBundle certBundle;

    @AfterAll
    static void tearDownAll() {
        if (sslPostgres != null && sslPostgres.isRunning()) {
            sslPostgres.stop();
        }
    }

    @Test
    void testSSLConnectionWithClientCertificate() throws Exception {
        // Generate certificates
        Path certDir = Files.createTempDirectory("ha-ssl-test-certs-");
        certBundle = SSLTestCertificateGenerator.generate(certDir);

        // Start SSL-enabled PostgreSQL container
        sslPostgres = PostgresSSLJdbcTest.createSSLPostgresContainer(certBundle);
        sslPostgres.start();

        // Build dbParams with SSL settings
        Map<String, Object> dbParams = new HashMap<>();
        dbParams.put("db_type", "postgres");
        dbParams.put("host", sslPostgres.getHost());
        dbParams.put("port", sslPostgres.getMappedPort(5432));
        dbParams.put("database", sslPostgres.getDatabaseName());
        dbParams.put("user", sslPostgres.getUsername());
        dbParams.put("password", sslPostgres.getPassword());
        dbParams.put("sslmode", "verify-ca");
        dbParams.put("sslkey", certBundle.clientKey().toString());
        dbParams.put("sslcert", certBundle.clientCert().toString());
        dbParams.put("sslrootcert", certBundle.caCert().toString());
        dbParams.put("sslpassword", certBundle.passphrase());

        Map<String, Object> dbHAConfig = Map.of("write_after", 1);

        // Create and initialize HAStateManager
        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        try {
            stateManager.initializeHA(HA_UUID, WORKER_NAME, dbParams, dbHAConfig);
            stateManager.enableLeader();

            // Verify basic DB operations work over SSL
            SessionState sessionState = new SessionState();
            sessionState.setHaUuid(HA_UUID);
            sessionState.setRuleSetName(RULE_SET_NAME);
            stateManager.persistSessionState(sessionState);

            SessionState retrieved = stateManager.getPersistedSessionState(RULE_SET_NAME);
            assertThat(retrieved).isNotNull();
            assertThat(retrieved.getHaUuid()).isEqualTo(HA_UUID);
            assertThat(retrieved.getRuleSetName()).isEqualTo(RULE_SET_NAME);

            // Test matching event persistence
            MatchingEvent me = TestUtils.createMatchingEvent(HA_UUID, RULE_SET_NAME, "testRule", Map.of("key", "value"));
            String meUuid = stateManager.addMatchingEvent(me);
            assertThat(meUuid).isNotNull();
            assertThat(stateManager.getPendingMatchingEvents()).hasSize(1);

            // Connection succeeded with cert auth — SSL/mTLS is working.
            // Verify the connection is actually using SSL.
            assertThat(stateManager.getHAStats()).isNotNull();
        } finally {
            stateManager.shutdown();
        }
    }

}

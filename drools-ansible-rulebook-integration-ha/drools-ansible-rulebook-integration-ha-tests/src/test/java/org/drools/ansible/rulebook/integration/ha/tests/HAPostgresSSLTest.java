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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for SSL/mTLS connection to PostgreSQL via HAStateManager.
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

    private static final String WORKER_NAME = "ssl-worker-1";
    private static final String RULE_SET_NAME = "sslTestRuleset";

    private static PostgreSQLContainer<?> sslPostgres;
    private static SSLTestCertificateGenerator.CertBundle baseCertBundle;
    private static SSLTestCertificateGenerator.CertBundle derKeyBundle;
    private static SSLTestCertificateGenerator.CertBundle unencryptedPkcs8PemKeyBundle;

    @BeforeAll
    static void setUp() throws Exception {
        Path tempDir = Files.createTempDirectory("ha-ssl-test-");
        baseCertBundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        // Derive bundles with client key in different formats
        derKeyBundle = SSLTestCertificateGenerator.withDerUnencryptedKey(baseCertBundle, tempDir.resolve("client.der"));
        unencryptedPkcs8PemKeyBundle = SSLTestCertificateGenerator.withUnencryptedPkcs8PemKey(baseCertBundle, tempDir.resolve("client-pkcs8.pem"));

        sslPostgres = PostgresSSLJdbcTest.createSSLPostgresContainer(baseCertBundle);
        sslPostgres.start();
    }

    @AfterAll
    static void tearDown() {
        if (sslPostgres != null && sslPostgres.isRunning()) {
            sslPostgres.stop();
        }
    }

    @Test
    void testSSLConnectionWithEncryptedPemKey() throws Exception {
        String haUuid = "ssl-test-encrypted-pem";
        Map<String, Object> dbParams = buildBaseDbParams(baseCertBundle);
        dbParams.put("sslkey", baseCertBundle.clientKey().toString());
        dbParams.put("sslcert", baseCertBundle.clientCert().toString());
        dbParams.put("sslpassword", baseCertBundle.passphrase());

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        try {
            stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1));
            stateManager.enableLeader();

            verifyBasicOperations(stateManager, haUuid);
        } finally {
            stateManager.shutdown();
        }
    }

    @Test
    void testSSLConnectionWithDerKeyNoPassword() throws Exception {
        String haUuid = "ssl-test-der-no-password";
        Map<String, Object> dbParams = buildBaseDbParams(derKeyBundle);
        dbParams.put("sslkey", derKeyBundle.clientKey().toString());
        dbParams.put("sslcert", derKeyBundle.clientCert().toString());
        // No sslpassword — DER key is unencrypted

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        try {
            stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1));
            stateManager.enableLeader();

            verifyBasicOperations(stateManager, haUuid);
        } finally {
            stateManager.shutdown();
        }
    }

    @Test
    void testSSLConnectionWithUnencryptedPkcs8PemKey() throws Exception {
        String haUuid = "ssl-test-unencrypted-pkcs8-pem";
        Map<String, Object> dbParams = buildBaseDbParams(unencryptedPkcs8PemKeyBundle);
        dbParams.put("sslkey", unencryptedPkcs8PemKeyBundle.clientKey().toString());
        dbParams.put("sslcert", unencryptedPkcs8PemKeyBundle.clientCert().toString());
        // No sslpassword — unencrypted PEM key

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        try {
            stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1));
            stateManager.enableLeader();

            verifyBasicOperations(stateManager, haUuid);
        } finally {
            stateManager.shutdown();
        }
    }

    private Map<String, Object> buildBaseDbParams(SSLTestCertificateGenerator.CertBundle bundle) {
        Map<String, Object> dbParams = new HashMap<>();
        dbParams.put("db_type", "postgres");
        dbParams.put("host", sslPostgres.getHost());
        dbParams.put("port", sslPostgres.getMappedPort(5432));
        dbParams.put("database", sslPostgres.getDatabaseName());
        dbParams.put("user", sslPostgres.getUsername());
        dbParams.put("password", sslPostgres.getPassword());
        dbParams.put("sslmode", "verify-ca");
        dbParams.put("sslrootcert", bundle.caCert().toString());
        return dbParams;
    }

    private void verifyBasicOperations(HAStateManager stateManager, String haUuid) {
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(haUuid);
        sessionState.setRuleSetName(RULE_SET_NAME);
        stateManager.persistSessionState(sessionState);

        SessionState retrieved = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.getHaUuid()).isEqualTo(haUuid);
        assertThat(retrieved.getRuleSetName()).isEqualTo(RULE_SET_NAME);

        MatchingEvent me = TestUtils.createMatchingEvent(haUuid, RULE_SET_NAME, "testRule", Map.of("key", "value"));
        String meUuid = stateManager.addMatchingEvent(me);
        assertThat(meUuid).isNotNull();
        assertThat(stateManager.getPendingMatchingEvents()).hasSize(1);

        assertThat(stateManager.getHAStats()).isNotNull();
    }
}

package org.drools.ansible.rulebook.integration.ha.tests;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.drools.ansible.rulebook.integration.ha.postgres.PemToKeyStoreConverter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SSL/mTLS integration tests for direct JDBC connections to PostgreSQL.
 * <p>
 * Tests multiple SSL key formats (DER, PKCS#12) via both Properties and URL parameters
 * against a real PostgreSQL container configured for client certificate authentication.
 * <p>
 * Run with: {@code mvn test -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests
 * -Dtest=PostgresSSLJdbcTest -Dtest.db.type=postgres}
 */
@EnabledIfSystemProperty(named = "test.db.type", matches = "postgres(ql)?")
class PostgresSSLJdbcTest {

    private static PostgreSQLContainer<?> postgres;
    private static SSLTestCertificateGenerator.CertBundle bundle;
    private static Path p12Path;
    private static Path derKeyPath;
    private static String jdbcUrl;

    @BeforeAll
    static void setUp() throws Exception {
        Path tempDir = Files.createTempDirectory("ssl-jdbc-test-");
        bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        // Convert encrypted PEM to P12
        p12Path = PemToKeyStoreConverter.convertPemToP12(
                bundle.clientKey().toString(),
                bundle.clientCert().toString(),
                bundle.passphrase().toCharArray());

        // Extract unencrypted PKCS#8 DER key from the P12
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var is = Files.newInputStream(p12Path)) {
            ks.load(is, bundle.passphrase().toCharArray());
        }
        String alias = ks.aliases().nextElement();
        PrivateKey clientPrivateKey = (PrivateKey) ks.getKey(alias, bundle.passphrase().toCharArray());
        derKeyPath = tempDir.resolve("client.der");
        Files.write(derKeyPath, clientPrivateKey.getEncoded());

        // Start SSL PostgreSQL container
        postgres = createSSLPostgresContainer(bundle);
        postgres.start();

        jdbcUrl = String.format("jdbc:postgresql://%s:%d/eda_ha_ssl_test",
                postgres.getHost(), postgres.getMappedPort(5432));
    }

    @AfterAll
    static void tearDown() {
        if (postgres != null && postgres.isRunning()) {
            postgres.stop();
        }
        if (p12Path != null) {
            PemToKeyStoreConverter.cleanup(p12Path);
        }
    }

    @Test
    void testNonSSLConnection() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "test");
        props.setProperty("password", "test");
        props.setProperty("sslmode", "disable");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 AS result")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("result")).isEqualTo(1);
        }
    }

    @Test
    void testSSLWithDerKeyViaProperties() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "test");
        props.setProperty("password", "test");
        props.setProperty("sslmode", "require");
        props.setProperty("sslkey", derKeyPath.toString());
        props.setProperty("sslcert", bundle.clientCert().toString());
        props.setProperty("sslrootcert", bundle.caCert().toString());

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 AS result")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("result")).isEqualTo(1);
        }
    }

    @Test
    void testSSLWithP12KeystoreViaProperties() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "test");
        props.setProperty("password", "test");
        props.setProperty("sslmode", "require");
        props.setProperty("sslkey", p12Path.toString());
        props.setProperty("sslpassword", bundle.passphrase());
        props.setProperty("sslrootcert", bundle.caCert().toString());

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 AS result")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("result")).isEqualTo(1);
        }
    }

    @Test
    void testSSLWithP12KeystoreViaUrlParams() throws Exception {
        String url = String.format(
                "%s?sslmode=require&sslrootcert=%s&sslkey=%s&sslpassword=%s",
                jdbcUrl, bundle.caCert(), p12Path, bundle.passphrase());

        Properties props = new Properties();
        props.setProperty("user", "test");
        props.setProperty("password", "test");

        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 AS result")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("result")).isEqualTo(1);
        }
    }

    /**
     * Creates a PostgreSQL container configured for SSL with client certificate authentication.
     * Shared by multiple SSL test classes.
     */
    static PostgreSQLContainer<?> createSSLPostgresContainer(SSLTestCertificateGenerator.CertBundle certs) throws Exception {
        byte[] caCertBytes = Files.readAllBytes(certs.caCert());
        byte[] serverKeyBytes = Files.readAllBytes(certs.serverKey());
        byte[] serverCertBytes = Files.readAllBytes(certs.serverCert());

        String initScript = "#!/bin/bash\n"
                + "set -e\n"
                + "\n"
                + "# Copy certs to PGDATA\n"
                + "cp /tmp/ssl/server.key \"$PGDATA/server.key\"\n"
                + "cp /tmp/ssl/server.crt \"$PGDATA/server.crt\"\n"
                + "cp /tmp/ssl/ca.crt \"$PGDATA/ca.crt\"\n"
                + "\n"
                + "# PostgreSQL requires strict permissions on the key file\n"
                + "chmod 600 \"$PGDATA/server.key\"\n"
                + "chown postgres:postgres \"$PGDATA/server.key\" \"$PGDATA/server.crt\" \"$PGDATA/ca.crt\"\n"
                + "\n"
                + "# Enable SSL in postgresql.conf\n"
                + "echo \"ssl = on\" >> \"$PGDATA/postgresql.conf\"\n"
                + "echo \"ssl_cert_file = 'server.crt'\" >> \"$PGDATA/postgresql.conf\"\n"
                + "echo \"ssl_key_file = 'server.key'\" >> \"$PGDATA/postgresql.conf\"\n"
                + "echo \"ssl_ca_file = 'ca.crt'\" >> \"$PGDATA/postgresql.conf\"\n"
                + "\n"
                + "# Write pg_hba.conf:\n"
                + "# - local connections use trust (for init scripts)\n"
                + "# - SSL connections from 'test' user require client cert\n"
                + "# - non-SSL connections use scram-sha-256 (for Testcontainers healthcheck)\n"
                + "cat > \"$PGDATA/pg_hba.conf\" << 'PGEOF'\n"
                + "local   all   all                     trust\n"
                + "hostssl all   test   0.0.0.0/0        cert\n"
                + "hostssl all   test   ::/0             cert\n"
                + "host    all   all    0.0.0.0/0        scram-sha-256\n"
                + "host    all   all    ::/0             scram-sha-256\n"
                + "PGEOF\n"
                + "\n"
                + "# Reload PostgreSQL to apply changes\n"
                + "pg_ctl reload -D \"$PGDATA\"\n";

        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
                .withDatabaseName("eda_ha_ssl_test")
                .withUsername("test")
                .withPassword("test")
                .withCopyToContainer(Transferable.of(serverKeyBytes), "/tmp/ssl/server.key")
                .withCopyToContainer(Transferable.of(serverCertBytes), "/tmp/ssl/server.crt")
                .withCopyToContainer(Transferable.of(caCertBytes), "/tmp/ssl/ca.crt")
                .withCopyToContainer(Transferable.of(initScript), "/docker-entrypoint-initdb.d/00-ssl-setup.sh");
    }
}

package org.drools.ansible.rulebook.integration.ha.tests;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

import org.drools.ansible.rulebook.integration.ha.postgres.PemToKeyStoreConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PEM-to-PKCS#12 key conversion.
 * <p>
 * For SSL integration tests against a real PostgreSQL container, see {@link PostgresSSLJdbcTest}.
 */
class PemToKeyStoreConverterTest {

    @TempDir
    Path tempDir;

    // Key format: Traditional OpenSSL PEM encryption (Proc-Type / DEK-Info)
    @Test
    void testConvertTraditionalEncryptedPemToP12() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        Path p12Path = PemToKeyStoreConverter.convertPemToP12(
                bundle.clientKey().toString(),
                bundle.clientCert().toString(),
                bundle.passphrase().toCharArray());

        assertThat(p12Path).exists();
        assertThat(p12Path.toString()).endsWith(".p12");

        // Load and verify the PKCS#12 keystore
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var is = Files.newInputStream(p12Path)) {
            ks.load(is, bundle.passphrase().toCharArray());
        }

        Enumeration<String> aliases = ks.aliases();
        assertThat(aliases.hasMoreElements()).isTrue();

        String alias = aliases.nextElement();
        assertThat(ks.isKeyEntry(alias)).isTrue();

        PrivateKey key = (PrivateKey) ks.getKey(alias, bundle.passphrase().toCharArray());
        assertThat(key).isNotNull();
        assertThat(key.getAlgorithm()).isEqualTo("RSA");

        Certificate[] chain = ks.getCertificateChain(alias);
        assertThat(chain).isNotNull().hasSize(1);
        X509Certificate cert = (X509Certificate) chain[0];
        assertThat(cert.getSubjectX500Principal().getName()).contains("CN=test");

        PemToKeyStoreConverter.cleanup(p12Path);
        assertThat(p12Path).doesNotExist();
    }

    // Key format: unencrypted PEM PKCS#8
    @Test
    void testConvertUnencryptedPkcs8PemToP12() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        // Write client key as unencrypted PKCS#8 PEM (BEGIN PRIVATE KEY)
        SSLTestCertificateGenerator.CertBundle pkcs8Bundle =
                SSLTestCertificateGenerator.withUnencryptedPkcs8PemKey(bundle, tempDir.resolve("client-pkcs8.pem"));

        // Verify the PEM file has the expected header
        String pemContent = Files.readString(pkcs8Bundle.clientKey());
        assertThat(pemContent).contains("-----BEGIN PRIVATE KEY-----");
        assertThat(pemContent).doesNotContain("ENCRYPTED");

        // Convert to P12 with a generated passphrase (simulates what PostgreSQLStateManager will do)
        Path p12Path = PemToKeyStoreConverter.convertPemToP12(
                pkcs8Bundle.clientKey().toString(),
                pkcs8Bundle.clientCert().toString(),
                "generated-passphrase".toCharArray());

        assertThat(p12Path).exists();
        assertThat(p12Path.toString()).endsWith(".p12");

        // Verify the P12 keystore contains the correct key and certificate
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var is = Files.newInputStream(p12Path)) {
            ks.load(is, "generated-passphrase".toCharArray());
        }

        String alias = ks.aliases().nextElement();
        assertThat(ks.isKeyEntry(alias)).isTrue();

        PrivateKey key = (PrivateKey) ks.getKey(alias, "generated-passphrase".toCharArray());
        assertThat(key).isNotNull();
        assertThat(key.getAlgorithm()).isEqualTo("RSA");

        Certificate[] chain = ks.getCertificateChain(alias);
        assertThat(chain).isNotNull().hasSize(1);
        X509Certificate cert = (X509Certificate) chain[0];
        assertThat(cert.getSubjectX500Principal().getName()).contains("CN=test");

        PemToKeyStoreConverter.cleanup(p12Path);
    }
}

package org.drools.ansible.rulebook.integration.ha.tests;

import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.drools.ansible.rulebook.integration.ha.postgres.PemToKeyStoreConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PEM-to-PKCS#12 key conversion.
 * <p>
 * These tests validate:
 * <ol>
 *   <li>Certificate generation produces valid PEM files</li>
 *   <li>PemToKeyStoreConverter can read traditional encrypted PEM keys</li>
 *   <li>The resulting PKCS#12 keystore contains the correct key and certificate</li>
 *   <li>Unencrypted PEM keys can also be converted</li>
 * </ol>
 * <p>
 * For SSL integration tests against a real PostgreSQL container, see {@link PostgresSSLJdbcTest}.
 */
class PemToKeyStoreConverterTest {

    @TempDir
    Path tempDir;

    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    @Test
    void testCertificateGenerationProducesValidPemFiles() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        assertThat(bundle.caCert()).exists();
        assertThat(bundle.serverKey()).exists();
        assertThat(bundle.serverCert()).exists();
        assertThat(bundle.clientKey()).exists();
        assertThat(bundle.clientCert()).exists();

        // Verify PEM headers
        String caCertContent = Files.readString(bundle.caCert());
        assertThat(caCertContent).contains("-----BEGIN CERTIFICATE-----");

        String clientKeyContent = Files.readString(bundle.clientKey());
        // Traditional encrypted PEM has "BEGIN RSA PRIVATE KEY" with Proc-Type/DEK-Info
        assertThat(clientKeyContent).contains("-----BEGIN RSA PRIVATE KEY-----");
        assertThat(clientKeyContent).contains("Proc-Type: 4,ENCRYPTED");
        assertThat(clientKeyContent).contains("DEK-Info:");

        String clientCertContent = Files.readString(bundle.clientCert());
        assertThat(clientCertContent).contains("-----BEGIN CERTIFICATE-----");
    }

    @Test
    void testPemParserRecognizesEncryptedKey() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        try (PEMParser parser = new PEMParser(new FileReader(bundle.clientKey().toFile()))) {
            Object obj = parser.readObject();
            assertThat(obj).isNotNull();
            assertThat(obj.getClass().getSimpleName()).isEqualTo("PEMEncryptedKeyPair");
        }
    }

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

    @Test
    void testIsPemEncrypted() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        // Encrypted PEM key should be detected as encrypted
        assertThat(PemToKeyStoreConverter.isPemEncrypted(bundle.clientKey().toString())).isTrue();

        // Unencrypted PEM key should be detected as not encrypted
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048, new SecureRandom());
        KeyPair keyPair = keyGen.generateKeyPair();

        Path unencryptedKeyPath = tempDir.resolve("unencrypted.pem");
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(unencryptedKeyPath.toFile()))) {
            writer.writeObject(keyPair.getPrivate());
        }

        assertThat(PemToKeyStoreConverter.isPemEncrypted(unencryptedKeyPath.toString())).isFalse();
    }

    @Test
    void testConvertUnencryptedPemToP12() throws Exception {
        SSLTestCertificateGenerator.CertBundle bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        // Write an unencrypted version of a key
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048, new SecureRandom());
        KeyPair keyPair = keyGen.generateKeyPair();

        Path unencryptedKeyPath = tempDir.resolve("unencrypted.key");
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(unencryptedKeyPath.toFile()))) {
            writer.writeObject(keyPair.getPrivate());
        }

        // Use a dummy passphrase (for the P12 keystore password)
        Path p12Path = PemToKeyStoreConverter.convertPemToP12(
                unencryptedKeyPath.toString(),
                bundle.clientCert().toString(),
                "dummypass".toCharArray());

        assertThat(p12Path).exists();

        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var is = Files.newInputStream(p12Path)) {
            ks.load(is, "dummypass".toCharArray());
        }
        assertThat(ks.size()).isEqualTo(1);

        PemToKeyStoreConverter.cleanup(p12Path);
    }

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

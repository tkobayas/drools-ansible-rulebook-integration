package org.drools.ansible.rulebook.integration.ha.tests;

import java.io.FileWriter;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Generates SSL certificates at test runtime for mTLS testing with PostgreSQL.
 * <p>
 * Produces:
 * <ul>
 *   <li>CA key pair + self-signed certificate (BasicConstraints CA:TRUE)</li>
 *   <li>Server key pair + certificate (SAN: DNS:localhost, IP:127.0.0.1; signed by CA)</li>
 *   <li>Client key pair + certificate (CN=test matching PostgreSQL role; signed by CA)</li>
 * </ul>
 */
final class SSLTestCertificateGenerator {

    static final String TEST_PASSPHRASE = "testpassphrase";

    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    private SSLTestCertificateGenerator() {
    }

    record CertBundle(
            Path caCert,
            Path serverKey,
            Path serverCert,
            Path clientKey,
            Path clientCert,
            String passphrase,
            Path baseDir
    ) {}

    /**
     * Generate a full set of SSL certificates for testing.
     *
     * @param baseDir directory to write certificate files into
     * @return CertBundle with paths to all generated files
     */
    static CertBundle generate(Path baseDir) throws Exception {
        Files.createDirectories(baseDir);

        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048, new SecureRandom());

        // --- CA ---
        KeyPair caKeyPair = keyGen.generateKeyPair();
        X509Certificate caCert = buildCACertificate(caKeyPair);

        // --- Server ---
        KeyPair serverKeyPair = keyGen.generateKeyPair();
        X509Certificate serverCert = buildServerCertificate(serverKeyPair, caKeyPair, caCert);

        // --- Client ---
        KeyPair clientKeyPair = keyGen.generateKeyPair();
        X509Certificate clientCert = buildClientCertificate(clientKeyPair, caKeyPair, caCert);

        // Write files
        Path caCertPath = baseDir.resolve("ca.crt");
        Path serverKeyPath = baseDir.resolve("server.key");
        Path serverCertPath = baseDir.resolve("server.crt");
        Path clientKeyPath = baseDir.resolve("client.key");
        Path clientCertPath = baseDir.resolve("client.crt");

        writeCertPem(caCertPath, caCert);
        writeUnencryptedKeyPem(serverKeyPath, serverKeyPair);
        writeCertPem(serverCertPath, serverCert);
        writeEncryptedKeyPem(clientKeyPath, clientKeyPair, TEST_PASSPHRASE.toCharArray());
        writeCertPem(clientCertPath, clientCert);

        return new CertBundle(caCertPath, serverKeyPath, serverCertPath, clientKeyPath, clientCertPath, TEST_PASSPHRASE, baseDir);
    }

    private static X509Certificate buildCACertificate(KeyPair caKeyPair) throws Exception {
        X500Name issuer = new X500Name("CN=Test CA");
        Instant now = Instant.now();
        Date notBefore = Date.from(now);
        Date notAfter = Date.from(now.plus(Duration.ofDays(365)));

        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                issuer,
                BigInteger.valueOf(1),
                notBefore, notAfter,
                issuer,
                caKeyPair.getPublic()
        );
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(caKeyPair.getPrivate());
        X509CertificateHolder holder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(holder);
    }

    private static X509Certificate buildServerCertificate(KeyPair serverKeyPair, KeyPair caKeyPair, X509Certificate caCert) throws Exception {
        X500Name issuer = new X500Name("CN=Test CA");
        X500Name subject = new X500Name("CN=localhost");
        Instant now = Instant.now();
        Date notBefore = Date.from(now);
        Date notAfter = Date.from(now.plus(Duration.ofDays(365)));

        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                issuer,
                BigInteger.valueOf(2),
                notBefore, notAfter,
                subject,
                serverKeyPair.getPublic()
        );

        // SAN: DNS:localhost, IP:127.0.0.1
        GeneralNames san = new GeneralNames(new GeneralName[]{
                new GeneralName(GeneralName.dNSName, "localhost"),
                new GeneralName(GeneralName.iPAddress, "127.0.0.1")
        });
        builder.addExtension(Extension.subjectAlternativeName, false, san);
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(caKeyPair.getPrivate());
        X509CertificateHolder holder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(holder);
    }

    private static X509Certificate buildClientCertificate(KeyPair clientKeyPair, KeyPair caKeyPair, X509Certificate caCert) throws Exception {
        X500Name issuer = new X500Name("CN=Test CA");
        // CN=test matches the PostgreSQL role name used in tests
        X500Name subject = new X500Name("CN=test");
        Instant now = Instant.now();
        Date notBefore = Date.from(now);
        Date notAfter = Date.from(now.plus(Duration.ofDays(365)));

        X509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                issuer,
                BigInteger.valueOf(3),
                notBefore, notAfter,
                subject,
                clientKeyPair.getPublic()
        );
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.digitalSignature));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(caKeyPair.getPrivate());
        X509CertificateHolder holder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(holder);
    }

    private static void writeCertPem(Path path, X509Certificate cert) throws Exception {
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(path.toFile()))) {
            writer.writeObject(cert);
        }
    }

    private static void writeUnencryptedKeyPem(Path path, KeyPair keyPair) throws Exception {
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(path.toFile()))) {
            writer.writeObject(keyPair.getPrivate());
        }
    }

    private static void writeEncryptedKeyPem(Path path, KeyPair keyPair, char[] passphrase) throws Exception {
        // Write traditional encrypted PEM (DEK-Info style, e.g. "-----BEGIN RSA PRIVATE KEY-----" with Proc-Type/DEK-Info)
        JcePEMEncryptorBuilder encBuilder = new JcePEMEncryptorBuilder("AES-256-CBC");
        encBuilder.setProvider("BC");
        try (JcaPEMWriter writer = new JcaPEMWriter(new FileWriter(path.toFile()))) {
            writer.writeObject(keyPair.getPrivate(), encBuilder.build(passphrase));
        }
    }
}

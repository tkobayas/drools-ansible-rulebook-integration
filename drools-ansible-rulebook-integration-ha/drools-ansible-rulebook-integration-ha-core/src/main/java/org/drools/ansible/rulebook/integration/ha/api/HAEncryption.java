package org.drools.ansible.rulebook.integration.ha.api;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES-256-GCM encryption for HA event/action data at rest.
 *
 * <p>Thread-safe: immutable key pair, thread-safe {@link SecureRandom},
 * {@link Cipher} instances created per-operation.</p>
 *
 * <p>Ciphertext format: {@code $ENCRYPTED$} + Base64( 12-byte-IV | AES-GCM-ciphertext | 16-byte-tag )</p>
 */
public class HAEncryption {

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int IV_LENGTH = 12;
    private static final int TAG_LENGTH_BITS = 128;
    private static final int REQUIRED_KEY_LENGTH = 32; // 256 bits

    static final String ENCRYPTED_PREFIX = "$ENCRYPTED$";

    private final SecretKey primaryKey;
    private final SecretKey secondaryKey; // nullable
    private final SecureRandom secureRandom;

    /**
     * Creates an HAEncryption instance with the given keys.
     *
     * @param base64PrimaryKey   Base64-encoded 256-bit AES key (required)
     * @param base64SecondaryKey Base64-encoded 256-bit AES key for fallback decryption (optional, may be null or empty)
     * @throws IllegalArgumentException if keys are invalid
     */
    public HAEncryption(String base64PrimaryKey, String base64SecondaryKey) {
        this.primaryKey = decodeKey(base64PrimaryKey, "primary");
        this.secondaryKey = (base64SecondaryKey != null && !base64SecondaryKey.isEmpty())
                ? decodeKey(base64SecondaryKey, "secondary")
                : null;
        this.secureRandom = new SecureRandom();
    }

    private static SecretKey decodeKey(String base64Key, String keyName) {
        byte[] keyBytes;
        try {
            keyBytes = Base64.getDecoder().decode(base64Key);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid Base64 encoding for " + keyName + " encryption key", e);
        }
        if (keyBytes.length != REQUIRED_KEY_LENGTH) {
            throw new IllegalArgumentException(
                    "Encryption " + keyName + " key must be exactly 256 bits (32 bytes), got " + (keyBytes.length * 8) + " bits");
        }
        return new SecretKeySpec(keyBytes, "AES");
    }

    /**
     * Encrypt plaintext with the primary key.
     *
     * @param plaintext the data to encrypt
     * @return {@code $ENCRYPTED$} + Base64(IV + ciphertext + tag)
     * @throws HAEncryptionException if encryption fails
     */
    public String encrypt(String plaintext) {
        if (plaintext == null) {
            return null;
        }
        try {
            byte[] iv = new byte[IV_LENGTH];
            secureRandom.nextBytes(iv);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, primaryKey, new GCMParameterSpec(TAG_LENGTH_BITS, iv));

            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            // Combine IV + ciphertext (which includes the GCM auth tag)
            byte[] combined = new byte[IV_LENGTH + ciphertext.length];
            System.arraycopy(iv, 0, combined, 0, IV_LENGTH);
            System.arraycopy(ciphertext, 0, combined, IV_LENGTH, ciphertext.length);

            return ENCRYPTED_PREFIX + Base64.getEncoder().encodeToString(combined);
        } catch (GeneralSecurityException e) {
            throw new HAEncryptionException("Failed to encrypt data", e);
        }
    }

    /**
     * Decrypt data. If the data does not have the {@code $ENCRYPTED$} prefix,
     * it is returned as-is (plaintext passthrough for backward compatibility).
     *
     * <p>Tries the primary key first, then falls back to the secondary key.
     * Throws {@link HAEncryptionException} (FATAL) if both keys fail.</p>
     *
     * @param data the data to decrypt (may be plaintext or encrypted)
     * @return DecryptResult with plaintext and whether the secondary key was used
     * @throws HAEncryptionException if decryption fails with all available keys
     */
    public DecryptResult decrypt(String data) {
        if (data == null) {
            return new DecryptResult(null, false);
        }
        if (!data.startsWith(ENCRYPTED_PREFIX)) {
            return new DecryptResult(data, false);
        }

        String base64Payload = data.substring(ENCRYPTED_PREFIX.length());
        byte[] combined;
        try {
            combined = Base64.getDecoder().decode(base64Payload);
        } catch (IllegalArgumentException e) {
            throw new HAEncryptionException("FATAL: Corrupted encrypted data - invalid Base64 encoding", e);
        }

        if (combined.length < IV_LENGTH) {
            throw new HAEncryptionException("FATAL: Corrupted encrypted data - payload too short");
        }

        // Try primary key
        try {
            return new DecryptResult(decryptWithKey(combined, primaryKey), false);
        } catch (AEADBadTagException e) {
            // Primary key failed - try secondary if available
        } catch (GeneralSecurityException e) {
            throw new HAEncryptionException("FATAL: Decryption failed with primary key", e);
        }

        // Try secondary key
        if (secondaryKey != null) {
            try {
                return new DecryptResult(decryptWithKey(combined, secondaryKey), true);
            } catch (AEADBadTagException e) {
                // Secondary key also failed
            } catch (GeneralSecurityException e) {
                throw new HAEncryptionException("FATAL: Decryption failed with secondary key", e);
            }
        }

        throw new HAEncryptionException(
                "FATAL: Decryption failed with both primary and secondary keys. "
                + "Encrypted data exists but neither key can decrypt it. "
                + "Activation will not start until valid keys are provided.");
    }

    private String decryptWithKey(byte[] combined, SecretKey key) throws GeneralSecurityException {
        byte[] iv = new byte[IV_LENGTH];
        System.arraycopy(combined, 0, iv, 0, IV_LENGTH);

        byte[] ciphertext = new byte[combined.length - IV_LENGTH];
        System.arraycopy(combined, IV_LENGTH, ciphertext, 0, ciphertext.length);

        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_LENGTH_BITS, iv));

        return new String(cipher.doFinal(ciphertext), StandardCharsets.UTF_8);
    }

    /**
     * Check if a string appears to be encrypted (has the {@code $ENCRYPTED$} prefix).
     */
    public static boolean isEncrypted(String data) {
        return data != null && data.startsWith(ENCRYPTED_PREFIX);
    }

    /**
     * Result of a decryption operation.
     *
     * @param plaintext        the decrypted plaintext
     * @param usedSecondaryKey true if the secondary (fallback) key was used for decryption
     */
    public record DecryptResult(String plaintext, boolean usedSecondaryKey) {}
}

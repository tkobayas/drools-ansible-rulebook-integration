package org.drools.ansible.rulebook.integration.ha.api;

/**
 * Fatal exception thrown when encrypted data cannot be decrypted.
 * This indicates a key mismatch or misconfiguration that must be resolved
 * before the HA system can operate safely.
 */
public class HAEncryptionException extends RuntimeException {

    public HAEncryptionException(String message) {
        super(message);
    }

    public HAEncryptionException(String message, Throwable cause) {
        super(message, cause);
    }
}

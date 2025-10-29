package org.drools.ansible.rulebook.integration.ha.api;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;

import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.model.prototype.impl.HashMapEventImpl;
import org.kie.api.runtime.rule.FactHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HAUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HAUtils.class);

    private HAUtils() {}

    public static Optional<String> getEventUuid(FactHandle handle) {
        Object object = handle.getObject();
        if (object instanceof HashMapEventImpl hashMapEvent) {
            return getEventUuid(hashMapEvent.asMap());
        }
        return Optional.empty();
    }

    // ansible-rulebook event always has meta -> uuid
    public static Optional<String> getEventUuid(Map<String, Object> event) {
        if (event.get("meta") instanceof Map<?,?> metaMap && metaMap.get("uuid") instanceof String eventUuid) {
            return Optional.of(eventUuid);
        }
        return Optional.empty();
    }

    private static final HexFormat HEX = HexFormat.of();

    public static byte[] sha256(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return md.digest(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String sha256(String s) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] hash = md.digest(s.getBytes(StandardCharsets.UTF_8));
        return HEX.formatHex(hash);
    }

    /**
     * Calculate SHA256 of the SessionState content for integrity verification.
     * This can detect corruption or tampering of persisted data.
     *
     * @param sessionState The session state to hash
     * @return SHA256 hex string of the state content
     */
    public static String calculateStateSHA(SessionState sessionState) {
        if (sessionState == null) {
            return null;
        }

        String hashableContent = sessionState.toHashableContent();
        return sha256(hashableContent);
    }

    /**
     * Populate a HA match response as new scheme. We may apply this to non-HA use case in the future.
     * @param matchResponse
     * @param ruleName
     * @param events
     * @param meUuid
     */
    public static void populateHAMatchResponse(Map<String, Object> matchResponse, String ruleName, Map<String, Object> events, String meUuid) {
        matchResponse.put("name", ruleName);
        matchResponse.put("events", events);
        matchResponse.put("matching_uuid", meUuid);
    }
}

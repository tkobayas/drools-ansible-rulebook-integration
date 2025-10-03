package org.drools.ansible.rulebook.integration.ha.api;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;

import org.drools.model.prototype.impl.HashMapEventImpl;
import org.kie.api.runtime.rule.FactHandle;

public class HAUtils {

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

    public static String calculateStateSHA(String previousSHA, String eventUuid) {
        String input = (previousSHA != null ? previousSHA : "") + eventUuid;
        return sha256(input);
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

package org.drools.ansible.rulebook.integration.ha.api;

import java.util.Map;
import java.util.Optional;

import org.drools.model.prototype.impl.HashMapEventImpl;
import org.kie.api.runtime.rule.FactHandle;

public class HAUtils {

    private HAUtils() {}

    public static Optional<String> getEventUuid(FactHandle event) {
        if (event instanceof HashMapEventImpl hashMapEvent) {
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
}

package org.drools.ansible.rulebook.integration.ha.api;

import java.util.LinkedHashMap;

import org.drools.ansible.rulebook.integration.ha.model.EventRecord;

public class HASessionContext {

    // TODO: consider thread-safety. processEvent is called by singele client. But AutomaticPsuedoClock is called by another thread

    // TODO: Consider performance. For now, LinkedHashMap because we need Map lookup and order of insertion
    private LinkedHashMap<String, EventRecord> eventUuidsInMemory = new LinkedHashMap<>();

    private String currentEventUuid;

    public LinkedHashMap<String, EventRecord> getEventUuidsInMemory() {
        return eventUuidsInMemory;
    }

    public void addEventUuidInMemory(String uuid, EventRecord eventRecord) {
        // TODO: consider MAX_EVENTS? but we already have memory check (MemoryMonitorUtil)
        eventUuidsInMemory.put(uuid, eventRecord);
        currentEventUuid = uuid;
    }

    public void removeEventUuidInMemory(String uuid) {
        eventUuidsInMemory.remove(uuid);
    }

    public String getCurrentEventUuid() {
        return currentEventUuid;
    }
}

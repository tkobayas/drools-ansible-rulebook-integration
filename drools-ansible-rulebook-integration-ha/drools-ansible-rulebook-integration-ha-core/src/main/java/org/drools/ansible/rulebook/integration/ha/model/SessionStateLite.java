package org.drools.ansible.rulebook.integration.ha.model;

/**
 * Lightweight representation of session state for non-leader nodes.
 * Encapsulates basic information to validate against persisted leader's state
 * Not to be persisted
 */
public class SessionStateLite {

    private final String rulebookHash;   // Base hash derived from rulebook content
    private String currentStateSHA;      // SHA256 of current state (advances with processed events)
    private String lastProcessedEventUuid;

    public SessionStateLite(String rulebookHash, String currentStateSHA, String lastProcessedEventUuid) {
        this.rulebookHash = rulebookHash;
        this.currentStateSHA = currentStateSHA;
        this.lastProcessedEventUuid = lastProcessedEventUuid;
    }

    public String getRulebookHash() {
        return rulebookHash;
    }

    public String getCurrentStateSHA() {
        return currentStateSHA;
    }

    public void setCurrentStateSHA(String currentStateSHA) {
        this.currentStateSHA = currentStateSHA;
    }

    public String getLastProcessedEventUuid() {
        return lastProcessedEventUuid;
    }

    public void setLastProcessedEventUuid(String lastProcessedEventUuid) {
        this.lastProcessedEventUuid = lastProcessedEventUuid;
    }
}

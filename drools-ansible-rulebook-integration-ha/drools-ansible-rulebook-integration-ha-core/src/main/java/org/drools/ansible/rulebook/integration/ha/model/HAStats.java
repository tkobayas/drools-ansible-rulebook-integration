package org.drools.ansible.rulebook.integration.ha.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;

/**
 * High Availability statistics for tracking leader status, transitions, and processing metrics.
 * Based on the HA Stats specification from the EDA HA documentation.
 */
public class HAStats implements Serializable {

    private static final long serialVersionUID = 2L;

    private String haUuid;
    private String currentLeader;
    private int leaderSwitches;
    private String currentTermStartedAt;
    private int eventsProcessedInTerm;
    private int actionsProcessedInTerm;
    private int incompleteMatchingEvents;
    private int partialEventsInMemory;
    private int partialFulfilledRules;
    private SessionStats globalSessionStats;
    private Long sessionStateSize;  // Size in bytes of the latest SessionState record

    // Extensibility columns for future use without schema migration
    private Map<String, Object> metadata = new HashMap<>();
    private Map<String, Object> properties = new HashMap<>();
    private Map<String, Object> settings = new HashMap<>();
    private Map<String, Object> ext = new HashMap<>();

    public HAStats() {
        this.leaderSwitches = 0;
        this.eventsProcessedInTerm = 0;
        this.actionsProcessedInTerm = 0;
        this.incompleteMatchingEvents = 0;
        this.partialEventsInMemory = 0;
        this.partialFulfilledRules = 0;
        this.sessionStateSize = 0L;
    }

    public HAStats(String haUuid) {
        this();
        this.haUuid = haUuid;
    }

    public HAStats(String haUuid, String currentLeader) {
        this(haUuid);
        this.currentLeader = currentLeader;
        this.currentTermStartedAt = Instant.now().toString();
    }

    /**
     * Gets the HA UUID that this stats instance belongs to
     *
     * @return HA UUID
     */
    public String getHaUuid() {
        return haUuid;
    }

    /**
     * Sets the HA UUID
     *
     * @param haUuid HA UUID
     */
    public void setHaUuid(String haUuid) {
        this.haUuid = haUuid;
    }

    /**
     * Gets the name of the current leader node
     *
     * @return current leader name
     */
    public String getCurrentLeader() {
        return currentLeader;
    }

    /**
     * Sets the current leader and updates term start time
     *
     * @param currentLeader leader name
     */
    public void setCurrentLeader(String currentLeader) {
        if (!Objects.equals(this.currentLeader, currentLeader)) {
            this.currentLeader = currentLeader;
            this.currentTermStartedAt = Instant.now().toString();
            this.leaderSwitches++;
            // Reset term counters when leader changes
            this.eventsProcessedInTerm = 0;
            this.actionsProcessedInTerm = 0;
        }
    }

    /**
     * Gets the total number of leader switches that have occurred
     *
     * @return number of leader switches
     */
    public int getLeaderSwitches() {
        return leaderSwitches;
    }

    public void setLeaderSwitches(int leaderSwitches) {
        this.leaderSwitches = leaderSwitches;
    }

    /**
     * Gets the ISO8601 timestamp when the current term started
     *
     * @return current term start time
     */
    public String getCurrentTermStartedAt() {
        return currentTermStartedAt;
    }

    public void setCurrentTermStartedAt(String currentTermStartedAt) {
        this.currentTermStartedAt = currentTermStartedAt;
    }

    /**
     * Gets the number of events processed in the current leader term
     *
     * @return events processed count
     */
    public int getEventsProcessedInTerm() {
        return eventsProcessedInTerm;
    }

    public void setEventsProcessedInTerm(int eventsProcessedInTerm) {
        this.eventsProcessedInTerm = eventsProcessedInTerm;
    }

    /**
     * Increments the events processed counter for the current term
     */
    public void incrementEventsProcessed() {
        this.eventsProcessedInTerm++;
    }

    /**
     * Gets the number of actions processed in the current leader term
     *
     * @return actions processed count
     */
    public int getActionsProcessedInTerm() {
        return actionsProcessedInTerm;
    }

    public void setActionsProcessedInTerm(int actionsProcessedInTerm) {
        this.actionsProcessedInTerm = actionsProcessedInTerm;
    }

    /**
     * Increments the actions processed counter for the current term
     */
    public void incrementActionsProcessed() {
        this.actionsProcessedInTerm++;
    }

    /**
     * Gets the number of matching events still pending processing for this HA UUID
     *
     * @return pending matching events count
     */
    public int getIncompleteMatchingEvents() {
        return incompleteMatchingEvents;
    }

    /**
     * Sets the number of matching events still pending processing for this HA UUID
     *
     * @param incompleteMatchingEvents pending matching events count
     */
    public void setIncompleteMatchingEvents(int incompleteMatchingEvents) {
        this.incompleteMatchingEvents = incompleteMatchingEvents;
    }

    /**
     * Gets the number of partial events currently tracked in memory for this HA UUID
     *
     * @return partial events in memory count
     */
    public int getPartialEventsInMemory() {
        return partialEventsInMemory;
    }

    /**
     * Sets the number of partial events currently tracked in memory for this HA UUID
     *
     * @param partialEventsInMemory partial events in memory count
     */
    public void setPartialEventsInMemory(int partialEventsInMemory) {
        this.partialEventsInMemory = partialEventsInMemory;
    }

    /**
     * Gets the number of partially fulfilled rules (partial beta matches) currently in memory
     *
     * @return partial fulfilled rules count
     */
    public int getPartialFulfilledRules() {
        return partialFulfilledRules;
    }

    /**
     * Sets the number of partially fulfilled rules (partial beta matches) currently in memory
     *
     * @param partialFulfilledRules partial fulfilled rules count
     */
    public void setPartialFulfilledRules(int partialFulfilledRules) {
        this.partialFulfilledRules = partialFulfilledRules;
    }

    /**
     * Gets the aggregated SessionStats across leaders (survives failover)
     *
     * @return global session stats
     */
    public SessionStats getGlobalSessionStats() {
        return globalSessionStats;
    }

    /**
     * Sets the aggregated SessionStats across leaders (survives failover)
     *
     * @param globalSessionStats aggregated session stats
     */
    public void setGlobalSessionStats(SessionStats globalSessionStats) {
        this.globalSessionStats = globalSessionStats;
    }

    /**
     * Gets the size in bytes of the latest SessionState record
     *
     * @return session state size in bytes
     */
    public Long getSessionStateSize() {
        return sessionStateSize;
    }

    /**
     * Sets the size in bytes of the latest SessionState record
     *
     * @param sessionStateSize size in bytes
     */
    public void setSessionStateSize(Long sessionStateSize) {
        this.sessionStateSize = sessionStateSize;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata != null ? metadata : new HashMap<>();
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties != null ? properties : new HashMap<>();
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, Object> settings) {
        this.settings = settings != null ? settings : new HashMap<>();
    }

    public Map<String, Object> getExt() {
        return ext;
    }

    public void setExt(Map<String, Object> ext) {
        this.ext = ext != null ? ext : new HashMap<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HAStats haStats = (HAStats) o;
        return leaderSwitches == haStats.leaderSwitches &&
                eventsProcessedInTerm == haStats.eventsProcessedInTerm &&
                actionsProcessedInTerm == haStats.actionsProcessedInTerm &&
                incompleteMatchingEvents == haStats.incompleteMatchingEvents &&
                partialEventsInMemory == haStats.partialEventsInMemory &&
                partialFulfilledRules == haStats.partialFulfilledRules &&
                Objects.equals(globalSessionStats, haStats.globalSessionStats) &&
                Objects.equals(haUuid, haStats.haUuid) &&
                Objects.equals(currentLeader, haStats.currentLeader) &&
                Objects.equals(currentTermStartedAt, haStats.currentTermStartedAt) &&
                Objects.equals(sessionStateSize, haStats.sessionStateSize) &&
                Objects.equals(metadata, haStats.metadata) &&
                Objects.equals(properties, haStats.properties) &&
                Objects.equals(settings, haStats.settings) &&
                Objects.equals(ext, haStats.ext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(haUuid, currentLeader, leaderSwitches, currentTermStartedAt,
                            eventsProcessedInTerm, actionsProcessedInTerm, incompleteMatchingEvents,
                            partialEventsInMemory, partialFulfilledRules, globalSessionStats, sessionStateSize,
                            metadata, properties, settings, ext);
    }

    @Override
    public String toString() {
        return "HAStats{" +
                "haUuid='" + haUuid + '\'' +
                ", currentLeader='" + currentLeader + '\'' +
                ", leaderSwitches=" + leaderSwitches +
                ", currentTermStartedAt='" + currentTermStartedAt + '\'' +
                ", eventsProcessedInTerm=" + eventsProcessedInTerm +
                ", actionsProcessedInTerm=" + actionsProcessedInTerm +
                ", incompleteMatchingEvents=" + incompleteMatchingEvents +
                ", partialEventsInMemory=" + partialEventsInMemory +
                ", partialFulfilledRules=" + partialFulfilledRules +
                ", globalSessionStats=" + globalSessionStats +
                ", sessionStateSize=" + sessionStateSize +
                ", metadata=" + metadata +
                ", properties=" + properties +
                ", settings=" + settings +
                ", ext=" + ext +
                '}';
    }
}

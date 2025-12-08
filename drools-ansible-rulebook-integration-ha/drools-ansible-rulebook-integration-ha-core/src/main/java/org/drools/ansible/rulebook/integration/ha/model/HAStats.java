package org.drools.ansible.rulebook.integration.ha.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * High Availability statistics for tracking leader status, transitions, and processing metrics.
 * Based on the HA Stats specification from the EDA HA documentation.
 */
public class HAStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private String haUuid;
    private String currentLeader;
    private int leaderSwitches;
    private String currentTermStartedAt;
    private int eventsProcessedInTerm;
    private int actionsProcessedInTerm;
    private int incompleteMatchingEvents;
    private Long sessionStateSize;  // Size in bytes of the latest SessionState record

    public HAStats() {
        this.leaderSwitches = 0;
        this.eventsProcessedInTerm = 0;
        this.actionsProcessedInTerm = 0;
        this.incompleteMatchingEvents = 0;
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
                Objects.equals(haUuid, haStats.haUuid) &&
                Objects.equals(currentLeader, haStats.currentLeader) &&
                Objects.equals(currentTermStartedAt, haStats.currentTermStartedAt) &&
                Objects.equals(sessionStateSize, haStats.sessionStateSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(haUuid, currentLeader, leaderSwitches, currentTermStartedAt,
                            eventsProcessedInTerm, actionsProcessedInTerm, incompleteMatchingEvents, sessionStateSize);
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
                ", sessionStateSize=" + sessionStateSize +
                '}';
    }
}

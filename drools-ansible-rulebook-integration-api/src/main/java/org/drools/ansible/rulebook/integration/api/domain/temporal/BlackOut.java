package org.drools.ansible.rulebook.integration.api.domain.temporal;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.temporal.TemporalAdjusters;

import com.fasterxml.jackson.annotation.JsonValue;

public class BlackOut {

    private Trigger trigger = Trigger.ALL;
    private Timezone timezone = Timezone.UTC;

    private TimeSpec startTime;
    private TimeSpec endTime;

    public TimeSpec getStart_time() {
        return startTime;
    }

    public void setStart_time(TimeSpec startTime) {
        this.startTime = startTime;
    }

    public TimeSpec getEnd_time() {
        return endTime;
    }

    public void setEnd_time(TimeSpec endTime) {
        this.endTime = endTime;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }

    public Timezone getTimezone() {
        return timezone;
    }

    public void setTimezone(Timezone timezone) {
        this.timezone = timezone;
    }

    public boolean isBlackOutActive(LocalDateTime dateTime) {
        if (startTime.getMonth() != null && startTime.getDayOfMonth() != null) {
            return isInAnnualInterval(dateTime);
        } else if (startTime.getDayOfWeek() != null) {
            return isInWeeklyInterval(dateTime);
        } else if (startTime.getHour() != null) {
            return isInDailyInterval(dateTime);
        } else {
            throw new IllegalStateException("Insufficient time specification");
        }
    }

    private boolean isInDailyInterval(LocalDateTime dt) {
        // TODO
        return false;
    }

    private boolean isInWeeklyInterval(LocalDateTime dt) {
        // TODO
        return false;
    }

    private boolean isInAnnualInterval(LocalDateTime dt) {
        // Use MonthDay to represent recurring dates (ignoring the year).
        MonthDay startMD = MonthDay.of(startTime.getMonth(), startTime.getDayOfMonth());
        MonthDay endMD = MonthDay.of(endTime.getMonth(), endTime.getDayOfMonth());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(),
                                                startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);
        int year = dt.getYear();

        LocalDateTime blackoutStart;
        LocalDateTime blackoutEnd;

        // If the period does not span the end-of-year boundary.
        if (startMD.compareTo(endMD) <= 0) {
            blackoutStart = LocalDateTime.of(year, startTime.getMonth(), startTime.getDayOfMonth(),
                                             startLocalTime.getHour(), startLocalTime.getMinute());
            blackoutEnd = LocalDateTime.of(year, endTime.getMonth(), endTime.getDayOfMonth(),
                                           endLocalTime.getHour(), endLocalTime.getMinute());
        } else {
            // The period spans the year boundary (e.g., Dec 23 to Jan 2).
            // TODO
            return false;
        }
        return !dt.isBefore(blackoutStart) && dt.isBefore(blackoutEnd);
    }

    public LocalDateTime getBlackOutNextEndTime(LocalDateTime currentDateTime) {
        if (startTime.getMonth() != null && startTime.getDayOfMonth() != null) {
            return getAnnualNextEndTime(currentDateTime);
        } else if (startTime.getDayOfWeek() != null) {
            return getWeeklyNextEndTime(currentDateTime);
        } else if (startTime.getHour() != null) {
            return getDailyNextEndTime(currentDateTime);
        } else {
            throw new IllegalStateException("Insufficient time specification for blackout schedule");
        }
    }

    private LocalDateTime getDailyNextEndTime(LocalDateTime currentDateTime) {
        // TODO
        return null;
    }

    private LocalDateTime getWeeklyNextEndTime(LocalDateTime currentDateTime) {
        // TODO
        return null;
    }

    private LocalDateTime getAnnualNextEndTime(LocalDateTime currentDateTime) {
        MonthDay startMD = MonthDay.of(startTime.getMonth(), startTime.getDayOfMonth());
        MonthDay endMD = MonthDay.of(endTime.getMonth(), endTime.getDayOfMonth());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(), startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);
        int year = currentDateTime.getYear();
        LocalDateTime candidateBlackoutEnd;

        if (startMD.compareTo(endMD) <= 0) {
            // Does not span year boundary.
            candidateBlackoutEnd = LocalDateTime.of(year, endTime.getMonth(), endTime.getDayOfMonth(),
                                                    endLocalTime.getHour(), endLocalTime.getMinute());
            if (!candidateBlackoutEnd.isAfter(currentDateTime)) {
                candidateBlackoutEnd = LocalDateTime.of(year + 1, endTime.getMonth(), endTime.getDayOfMonth(),
                                                        endLocalTime.getHour(), endLocalTime.getMinute());
            }
        } else {
            // Spans the year boundary (e.g., Dec 23 to Jan 2).
            // TODO
            return null;
        }
        return candidateBlackoutEnd;
    }

    @Override
    public String toString() {
        return "BlackOut{" +
                "trigger=" + trigger +
                ", timezone=" + timezone +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }

    enum Trigger {
        ALL("all"), // default
        FIRST("first"),
        LAST("last");

        Trigger(String value) {
            this.value = value;
        }

        @JsonValue
        private String value;
    }

    enum Timezone {
        UTC("utc"), // default
        LOCAL("local");

        Timezone(String value) {
            this.value = value;
        }

        @JsonValue
        private String value;
    }
}

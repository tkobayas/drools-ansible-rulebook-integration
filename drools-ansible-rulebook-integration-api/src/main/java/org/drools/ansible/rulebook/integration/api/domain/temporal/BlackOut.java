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

    public BlackOut() {
    }

    public BlackOut(TimeSpec startTime, TimeSpec endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

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
        LocalTime time = dt.toLocalTime();
        LocalTime start = LocalTime.of(startTime.getHour(),
                                       startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime end = LocalTime.of(endTime.getHour(),
                                     endTime.getMinute() != null ? endTime.getMinute() : 0);
        // If the interval does not cross midnight:
        if (!start.isAfter(end)) {
            return !time.isBefore(start) && time.isBefore(end);
        } else { // interval spans midnight, e.g., 10PM to 2AM
            return !time.isBefore(start) || time.isBefore(end);
        }
    }

    private boolean isInWeeklyInterval(LocalDateTime dt) {
        // Assume dayOfWeek in the TimeSpec corresponds to Java's DayOfWeek.getValue().
        DayOfWeek startDow = DayOfWeek.of(startTime.getDayOfWeek());
        DayOfWeek endDow = DayOfWeek.of(endTime.getDayOfWeek());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(),
                                                startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);

        // Find the most recent occurrence (or today) for the start day.
        LocalDate dtDate = dt.toLocalDate();
        LocalDate startDate = dtDate.with(TemporalAdjusters.previousOrSame(startDow));
        LocalDateTime blackoutStart = LocalDateTime.of(startDate, startLocalTime);

        LocalDateTime blackoutEnd;
        if (startDow.equals(endDow)) {
            // Same day; check for possible midnight crossing.
            if (!startLocalTime.isAfter(endLocalTime)) {
                blackoutEnd = LocalDateTime.of(startDate, endLocalTime);
            } else {
                // Spanning midnight: end on the next day.
                blackoutEnd = LocalDateTime.of(startDate.plusDays(1), endLocalTime);
            }
        } else {
            // Different days. Compute the days between (wrap around the week if needed).
            int daysBetween = endDow.getValue() - startDow.getValue();
            if (daysBetween <= 0) {
                daysBetween += 7;
            }
            blackoutEnd = LocalDateTime.of(startDate.plusDays(daysBetween), endLocalTime);
        }

        // If dt occurs before the computed blackoutStart, check the previous week's interval.
        if (dt.isBefore(blackoutStart)) {
            blackoutStart = blackoutStart.minusWeeks(1);
            blackoutEnd = blackoutEnd.minusWeeks(1);
        }
        return !dt.isBefore(blackoutStart) && dt.isBefore(blackoutEnd);
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
            MonthDay currentMD = MonthDay.from(dt);
            if (currentMD.compareTo(startMD) >= 0) {
                // In later part of the year: interval from this year's Dec 23 to next year's Jan 2.
                blackoutStart = LocalDateTime.of(year, startTime.getMonth(), startTime.getDayOfMonth(),
                                                 startLocalTime.getHour(), startLocalTime.getMinute());
                blackoutEnd = LocalDateTime.of(year + 1, endTime.getMonth(), endTime.getDayOfMonth(),
                                               endLocalTime.getHour(), endLocalTime.getMinute());
            } else {
                // In early part of the year: interval from last year's Dec 23 to this year's Jan 2.
                blackoutStart = LocalDateTime.of(year - 1, startTime.getMonth(), startTime.getDayOfMonth(),
                                                 startLocalTime.getHour(), startLocalTime.getMinute());
                blackoutEnd = LocalDateTime.of(year, endTime.getMonth(), endTime.getDayOfMonth(),
                                               endLocalTime.getHour(), endLocalTime.getMinute());
            }
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
        LocalTime start = LocalTime.of(startTime.getHour(), startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime end = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);
        LocalDate today = currentDateTime.toLocalDate();

        if (!start.isAfter(end)) {
            // Does not span midnight: blackout period is today [start, end)
            LocalDateTime candidate = LocalDateTime.of(today, end);
            if (currentDateTime.isBefore(candidate)) {
                return candidate;
            } else {
                return LocalDateTime.of(today.plusDays(1), end);
            }
        } else {
            // Spans midnight (e.g., 22:00 to 02:00)
            // There are two potential instances: one ending today and one ending tomorrow.
            // If the current time is before the end time, we assume the blackout that started yesterday is still in effect.
            // Otherwise, the next blackout ends tomorrow.
            LocalTime currentTime = currentDateTime.toLocalTime();
            if (currentTime.isBefore(end)) {
                LocalDateTime candidate = LocalDateTime.of(today, end);
                if (currentDateTime.isBefore(candidate)) {
                    return candidate;
                } else {
                    // Exactly at the end time – return the next occurrence.
                    return LocalDateTime.of(today.plusDays(1), end);
                }
            } else {
                return LocalDateTime.of(today.plusDays(1), end);
            }
        }
    }

    private LocalDateTime getWeeklyNextEndTime(LocalDateTime currentDateTime) {
        DayOfWeek startDow = DayOfWeek.of(startTime.getDayOfWeek());
        DayOfWeek endDow = DayOfWeek.of(endTime.getDayOfWeek());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(), startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);

        LocalDate baseDate = currentDateTime.toLocalDate();
        // Find the most recent occurrence of the start day.
        LocalDate candidateStartDate = baseDate.with(TemporalAdjusters.previousOrSame(startDow));
        LocalDateTime candidateBlackoutEnd;
        if (startDow.equals(endDow)) {
            if (!startLocalTime.isAfter(endLocalTime)) {
                candidateBlackoutEnd = LocalDateTime.of(candidateStartDate, endLocalTime);
            } else {
                candidateBlackoutEnd = LocalDateTime.of(candidateStartDate.plusDays(1), endLocalTime);
            }
        } else {
            int daysBetween = endDow.getValue() - startDow.getValue();
            if (daysBetween <= 0) {
                daysBetween += 7;
            }
            candidateBlackoutEnd = LocalDateTime.of(candidateStartDate.plusDays(daysBetween), endLocalTime);
        }
        // If the candidate end time is not strictly after the current time, shift one week forward.
        if (!candidateBlackoutEnd.isAfter(currentDateTime)) {
            candidateBlackoutEnd = candidateBlackoutEnd.plusWeeks(1);
        }
        return candidateBlackoutEnd;
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
            MonthDay currentMD = MonthDay.from(currentDateTime);
            if (currentMD.compareTo(startMD) >= 0) {
                candidateBlackoutEnd = LocalDateTime.of(year + 1, endTime.getMonth(), endTime.getDayOfMonth(),
                                                        endLocalTime.getHour(), endLocalTime.getMinute());
            } else {
                candidateBlackoutEnd = LocalDateTime.of(year, endTime.getMonth(), endTime.getDayOfMonth(),
                                                        endLocalTime.getHour(), endLocalTime.getMinute());
            }
            if (!candidateBlackoutEnd.isAfter(currentDateTime)) {
                candidateBlackoutEnd = candidateBlackoutEnd.plusYears(1);
            }
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

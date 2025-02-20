package org.drools.ansible.rulebook.integration.api.domain.temporal;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;

import com.fasterxml.jackson.annotation.JsonValue;

public class BlackOut {

    private Trigger trigger = Trigger.ALL;
    private Timezone timezone = Timezone.UTC;
    private ZoneId effectiveZone = ZoneOffset.UTC; // default is UTC

    private TimeSpec startTime;
    private TimeSpec endTime;

    public BlackOut() {
    }

    public BlackOut(TimeSpec startTime, TimeSpec endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
        setTimezone(Timezone.UTC);
    }

    public BlackOut(TimeSpec startTime, TimeSpec endTime, Timezone timezone) {
        this.startTime = startTime;
        this.endTime = endTime;
        setTimezone(timezone);
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
        this.timezone = (timezone != null ? timezone : Timezone.UTC);
        this.effectiveZone = (this.timezone == Timezone.LOCAL ? ZoneId.systemDefault() : ZoneOffset.UTC);
    }

    public ZoneId getEffectiveZone() {
        return effectiveZone;
    }

    public void validate() {
        if (startTime == null || endTime == null) {
            throw new IllegalStateException("black_out start and end times must be specified");
        }
        // WIP
    }

    /**
     * Returns true if the given ZonedDateTime (which is assumed to be expressed in any zone)
     * falls within the blackout period defined in the configured effectiveZone.
     * The method first converts the argument to the effective zone.
     */
    public boolean isBlackOutActive(ZonedDateTime dt) {
        ZonedDateTime effectiveDt = dt.withZoneSameInstant(effectiveZone);
        if (startTime.getDayOfMonth() != null && startTime.getMonth() == null) {
            return isInMonthlyInterval(effectiveDt);
        } else if (startTime.getMonth() != null && startTime.getDayOfMonth() != null) {
            return isInAnnualInterval(effectiveDt);
        } else if (startTime.getDayOfWeek() != null) {
            return isInWeeklyInterval(effectiveDt);
        } else if (startTime.getHour() != null) {
            return isInDailyInterval(effectiveDt);
        } else {
            throw new IllegalStateException("Insufficient time specification");
        }
    }

    /**
     * Returns the next blackout end time (as a ZonedDateTime in the effectiveZone)
     * that comes strictly after the provided dt.
     * The dt is first converted to the effective zone.
     */
    public ZonedDateTime getBlackOutNextEndTime(ZonedDateTime dt) {
        ZonedDateTime effectiveDt = dt.withZoneSameInstant(effectiveZone);
        if (startTime.getDayOfMonth() != null && startTime.getMonth() == null) {
            return getMonthlyNextEndTime(effectiveDt);
        } else if (startTime.getMonth() != null && startTime.getDayOfMonth() != null) {
            return getAnnualNextEndTime(effectiveDt);
        } else if (startTime.getDayOfWeek() != null) {
            return getWeeklyNextEndTime(effectiveDt);
        } else if (startTime.getHour() != null) {
            return getDailyNextEndTime(effectiveDt);
        } else {
            throw new IllegalStateException("Insufficient time specification for blackout schedule");
        }
    }

    // --- Daily schedule helpers ---
    private boolean isInDailyInterval(ZonedDateTime dt) {
        LocalTime time = dt.toLocalTime();
        LocalTime start = LocalTime.of(startTime.getHour(),
                                       startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime end = LocalTime.of(endTime.getHour(),
                                     endTime.getMinute() != null ? endTime.getMinute() : 0);
        if (!start.isAfter(end)) {
            return !time.isBefore(start) && time.isBefore(end);
        } else { // spans midnight, e.g., 22:00 to 02:00
            return !time.isBefore(start) || time.isBefore(end);
        }
    }

    private ZonedDateTime getDailyNextEndTime(ZonedDateTime dt) {
        LocalDate today = dt.toLocalDate();
        LocalTime end = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);
        ZonedDateTime candidate = today.atTime(end).atZone(effectiveZone);
        return dt.toLocalTime().isBefore(end) ? candidate : today.plusDays(1).atTime(end).atZone(effectiveZone);
    }

    // --- Weekly schedule helpers ---
    private boolean isInWeeklyInterval(ZonedDateTime dt) {
        DayOfWeek startDow = DayOfWeek.of(startTime.getDayOfWeek());
        DayOfWeek endDow = DayOfWeek.of(endTime.getDayOfWeek());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(),
                                                startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);

        LocalDate date = dt.toLocalDate();
        LocalDate startDate = date.with(TemporalAdjusters.previousOrSame(startDow));
        ZonedDateTime blackoutStart = startDate.atTime(startLocalTime).atZone(effectiveZone);
        ZonedDateTime blackoutEnd;
        if (startDow.equals(endDow)) {
            if (!startLocalTime.isAfter(endLocalTime)) {
                blackoutEnd = startDate.atTime(endLocalTime).atZone(effectiveZone);
            } else {
                blackoutEnd = startDate.plusDays(1).atTime(endLocalTime).atZone(effectiveZone);
            }
        } else {
            int daysBetween = endDow.getValue() - startDow.getValue();
            if (daysBetween <= 0) {
                daysBetween += 7;
            }
            blackoutEnd = startDate.plusDays(daysBetween).atTime(endLocalTime).atZone(effectiveZone);
        }
        if (dt.isBefore(blackoutStart)) {
            blackoutStart = blackoutStart.minusWeeks(1);
            blackoutEnd = blackoutEnd.minusWeeks(1);
        }
        return !dt.isBefore(blackoutStart) && dt.isBefore(blackoutEnd);
    }

    private ZonedDateTime getWeeklyNextEndTime(ZonedDateTime dt) {
        DayOfWeek startDow = DayOfWeek.of(startTime.getDayOfWeek());
        DayOfWeek endDow = DayOfWeek.of(endTime.getDayOfWeek());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(),
                                                startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);

        LocalDate baseDate = dt.toLocalDate();
        LocalDate candidateStartDate = baseDate.with(TemporalAdjusters.previousOrSame(startDow));
        ZonedDateTime candidateBlackoutEnd;
        if (startDow.equals(endDow)) {
            if (!startLocalTime.isAfter(endLocalTime)) {
                candidateBlackoutEnd = candidateStartDate.atTime(endLocalTime).atZone(effectiveZone);
            } else {
                candidateBlackoutEnd = candidateStartDate.plusDays(1).atTime(endLocalTime).atZone(effectiveZone);
            }
        } else {
            int daysBetween = endDow.getValue() - startDow.getValue();
            if (daysBetween <= 0) {
                daysBetween += 7;
            }
            candidateBlackoutEnd = candidateStartDate.plusDays(daysBetween).atTime(endLocalTime).atZone(effectiveZone);
        }
        if (!candidateBlackoutEnd.isAfter(dt)) {
            candidateBlackoutEnd = candidateBlackoutEnd.plusWeeks(1);
        }
        return candidateBlackoutEnd;
    }

    // --- Annual schedule helpers ---
    private boolean isInAnnualInterval(ZonedDateTime dt) {
        MonthDay startMD = MonthDay.of(startTime.getMonth(), startTime.getDayOfMonth());
        MonthDay endMD = MonthDay.of(endTime.getMonth(), endTime.getDayOfMonth());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(),
                                                startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);
        int year = dt.getYear();

        ZonedDateTime blackoutStart;
        ZonedDateTime blackoutEnd;
        if (startMD.compareTo(endMD) <= 0) {
            blackoutStart = LocalDateTime.of(year, startTime.getMonth(), startTime.getDayOfMonth(),
                                             startLocalTime.getHour(), startLocalTime.getMinute()).atZone(effectiveZone);
            blackoutEnd = LocalDateTime.of(year, endTime.getMonth(), endTime.getDayOfMonth(),
                                           endLocalTime.getHour(), endLocalTime.getMinute()).atZone(effectiveZone);
        } else {
            if (MonthDay.from(dt).compareTo(startMD) >= 0) {
                blackoutStart = LocalDateTime.of(year, startTime.getMonth(), startTime.getDayOfMonth(),
                                                 startLocalTime.getHour(), startLocalTime.getMinute()).atZone(effectiveZone);
                blackoutEnd = LocalDateTime.of(year + 1, endTime.getMonth(), endTime.getDayOfMonth(),
                                               endLocalTime.getHour(), endLocalTime.getMinute()).atZone(effectiveZone);
            } else {
                blackoutStart = LocalDateTime.of(year - 1, startTime.getMonth(), startTime.getDayOfMonth(),
                                                 startLocalTime.getHour(), startLocalTime.getMinute()).atZone(effectiveZone);
                blackoutEnd = LocalDateTime.of(year, endTime.getMonth(), endTime.getDayOfMonth(),
                                               endLocalTime.getHour(), endLocalTime.getMinute()).atZone(effectiveZone);
            }
        }
        return !dt.isBefore(blackoutStart) && dt.isBefore(blackoutEnd);
    }

    private ZonedDateTime getAnnualNextEndTime(ZonedDateTime dt) {
        MonthDay startMD = MonthDay.of(startTime.getMonth(), startTime.getDayOfMonth());
        MonthDay endMD = MonthDay.of(endTime.getMonth(), endTime.getDayOfMonth());
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);
        int year = dt.getYear();
        ZonedDateTime candidateBlackoutEnd;
        if (startMD.compareTo(endMD) <= 0) {
            candidateBlackoutEnd = LocalDateTime.of(year, endTime.getMonth(), endTime.getDayOfMonth(),
                                                    endLocalTime.getHour(), endLocalTime.getMinute()).atZone(effectiveZone);
            if (!candidateBlackoutEnd.isAfter(dt)) {
                candidateBlackoutEnd = LocalDateTime.of(year + 1, endTime.getMonth(), endTime.getDayOfMonth(),
                                                        endLocalTime.getHour(), endLocalTime.getMinute()).atZone(effectiveZone);
            }
        } else {
            if (MonthDay.from(dt).compareTo(startMD) >= 0) {
                candidateBlackoutEnd = LocalDateTime.of(year + 1, endTime.getMonth(), endTime.getDayOfMonth(),
                                                        endLocalTime.getHour(), endLocalTime.getMinute()).atZone(effectiveZone);
            } else {
                candidateBlackoutEnd = LocalDateTime.of(year, endTime.getMonth(), endTime.getDayOfMonth(),
                                                        endLocalTime.getHour(), endLocalTime.getMinute()).atZone(effectiveZone);
            }
            if (!candidateBlackoutEnd.isAfter(dt)) {
                candidateBlackoutEnd = candidateBlackoutEnd.plusYears(1);
            }
        }
        return candidateBlackoutEnd;
    }

    // --- Monthly schedule helpers ---
    private boolean isInMonthlyInterval(ZonedDateTime dt) {
        int startDay = startTime.getDayOfMonth();
        int endDay = endTime.getDayOfMonth();
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(),
                                                startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);

        if (startDay <= endDay) {
            LocalDate periodStartDate = LocalDate.of(dt.getYear(), dt.getMonthValue(), startDay);
            LocalDate periodEndDate = LocalDate.of(dt.getYear(), dt.getMonthValue(), endDay);
            ZonedDateTime blackoutStart = periodStartDate.atTime(startLocalTime).atZone(effectiveZone);
            ZonedDateTime blackoutEnd = periodEndDate.atTime(endLocalTime).atZone(effectiveZone);
            return !dt.isBefore(blackoutStart) && dt.isBefore(blackoutEnd);
        } else {
            ZonedDateTime blackoutStart;
            ZonedDateTime blackoutEnd;
            if (dt.getDayOfMonth() >= startDay) {
                blackoutStart = LocalDate.of(dt.getYear(), dt.getMonthValue(), startDay)
                        .atTime(startLocalTime).atZone(effectiveZone);
                LocalDate nextMonth = dt.toLocalDate().plusMonths(1);
                blackoutEnd = LocalDate.of(nextMonth.getYear(), nextMonth.getMonthValue(), endDay)
                        .atTime(endLocalTime).atZone(effectiveZone);
            } else {
                LocalDate prevMonth = dt.toLocalDate().minusMonths(1);
                blackoutStart = LocalDate.of(prevMonth.getYear(), prevMonth.getMonthValue(), startDay)
                        .atTime(startLocalTime).atZone(effectiveZone);
                blackoutEnd = LocalDate.of(dt.getYear(), dt.getMonthValue(), endDay)
                        .atTime(endLocalTime).atZone(effectiveZone);
            }
            return !dt.isBefore(blackoutStart) && dt.isBefore(blackoutEnd);
        }
    }

    private ZonedDateTime getMonthlyNextEndTime(ZonedDateTime dt) {
        int startDay = startTime.getDayOfMonth();
        int endDay = endTime.getDayOfMonth();
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);
        ZonedDateTime candidate;
        if (startDay <= endDay) {
            candidate = LocalDate.of(dt.getYear(), dt.getMonthValue(), endDay)
                    .atTime(endLocalTime).atZone(effectiveZone);
            if (!candidate.isAfter(dt)) {
                candidate = candidate.plusMonths(1);
            }
        } else {
            if (dt.getDayOfMonth() >= startDay) {
                candidate = LocalDate.of(dt.toLocalDate().plusMonths(1).getYear(),
                                         dt.toLocalDate().plusMonths(1).getMonthValue(), endDay)
                        .atTime(endLocalTime).atZone(effectiveZone);
            } else {
                candidate = LocalDate.of(dt.getYear(), dt.getMonthValue(), endDay)
                        .atTime(endLocalTime).atZone(effectiveZone);
                if (!candidate.isAfter(dt)) {
                    candidate = candidate.plusMonths(1);
                }
            }
        }
        return candidate;
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

    public enum Trigger {
        ALL("all"), // default
        FIRST("first"),
        LAST("last");

        Trigger(String value) {
            this.value = value;
        }

        @JsonValue
        private String value;
    }

    public enum Timezone {
        UTC("utc"), // default
        LOCAL("local");

        Timezone(String value) {
            this.value = value;
        }

        @JsonValue
        private String value;
    }
}

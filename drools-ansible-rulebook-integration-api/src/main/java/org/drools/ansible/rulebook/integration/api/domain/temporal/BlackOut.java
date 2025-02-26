package org.drools.ansible.rulebook.integration.api.domain.temporal;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.YearMonth;
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

    /**
     * Validates that both startTime and endTime are non-null, that numeric fields fall within allowed ranges,
     * that the combination of fields in each TimeSpec conforms to one of the acceptable schedule types,
     * and that startTime and endTime represent the same schedule type.
     * In addition, for Annual schedules the combination of month and day_of_month must form a possible date
     * (e.g. April 31 is rejected).
     */
    public void validate() {
        if (startTime == null || endTime == null) {
            throw new IllegalStateException("Both startTime and endTime must be provided.");
        }
        // Numeric ranges.
        if (startTime.getMinute() != null && (startTime.getMinute() < 0 || startTime.getMinute() > 59))
            throw new IllegalArgumentException("Start minute must be between 0 and 59.");
        if (endTime.getMinute() != null && (endTime.getMinute() < 0 || endTime.getMinute() > 59))
            throw new IllegalArgumentException("End minute must be between 0 and 59.");
        if (startTime.getHour() == null || startTime.getHour() < 0 || startTime.getHour() > 23)
            throw new IllegalArgumentException("Start hour must be between 0 and 23 and is required.");
        if (endTime.getHour() == null || endTime.getHour() < 0 || endTime.getHour() > 23)
            throw new IllegalArgumentException("End hour must be between 0 and 23 and is required.");
        if (startTime.getMonth() != null && (startTime.getMonth() < 1 || startTime.getMonth() > 12))
            throw new IllegalArgumentException("Start month must be between 1 and 12.");
        if (endTime.getMonth() != null && (endTime.getMonth() < 1 || endTime.getMonth() > 12))
            throw new IllegalArgumentException("End month must be between 1 and 12.");
        if (startTime.getDayOfMonth() != null && (startTime.getDayOfMonth() < 1 || startTime.getDayOfMonth() > 31))
            throw new IllegalArgumentException("Start day_of_month must be between 1 and 31.");
        if (endTime.getDayOfMonth() != null && (endTime.getDayOfMonth() < 1 || endTime.getDayOfMonth() > 31))
            throw new IllegalArgumentException("End day_of_month must be between 1 and 31.");
        if (startTime.getDayOfWeek() != null && (startTime.getDayOfWeek() < 0 || startTime.getDayOfWeek() > 7))
            throw new IllegalArgumentException("Start day_of_week must be between 0 and 7.");
        if (endTime.getDayOfWeek() != null && (endTime.getDayOfWeek() < 0 || endTime.getDayOfWeek() > 7))
            throw new IllegalArgumentException("End day_of_week must be between 0 and 7.");

        // Determine schedule types.
        TimeSpec.ScheduleType startType = startTime.getScheduleType();
        TimeSpec.ScheduleType endType = endTime.getScheduleType();
        if (startType == TimeSpec.ScheduleType.UNKNOWN || endType == TimeSpec.ScheduleType.UNKNOWN) {
            throw new IllegalArgumentException("TimeSpec combination does not match any acceptable schedule type.");
        }
        if (startType != endType) {
            throw new IllegalArgumentException("Start and end TimeSpecs must be of the same schedule type.");
        }
        // For Annual schedules, check that the day_of_month is valid for the given month.
        if (startType == TimeSpec.ScheduleType.ANNUAL) {
            // Use an arbitrary leap year (e.g. 2000) to allow February 29.
            YearMonth ymStart = YearMonth.of(2000, startTime.getMonth());
            if (startTime.getDayOfMonth() > ymStart.lengthOfMonth()) {
                throw new IllegalArgumentException("Start date " + startTime.getMonth() + "/" + startTime.getDayOfMonth() + " is not a valid date.");
            }
            YearMonth ymEnd = YearMonth.of(2000, endTime.getMonth());
            if (endTime.getDayOfMonth() > ymEnd.lengthOfMonth()) {
                throw new IllegalArgumentException("End date " + endTime.getMonth() + "/" + endTime.getDayOfMonth() + " is not a valid date.");
            }
        }
    }

    /**
     * Returns true if the given ZonedDateTime (converted to effectiveZone) falls within the blackout period.
     */
    public boolean isBlackOutActive(ZonedDateTime dt) {
        ZonedDateTime effectiveDt = dt.withZoneSameInstant(effectiveZone);
        switch (startTime.getScheduleType()) {
            case DAILY:
                return isInDailyInterval(effectiveDt);
            case WEEKLY:
                return isInWeeklyInterval(effectiveDt);
            case MONTHLY:
                return isInMonthlyInterval(effectiveDt);
            case ANNUAL:
                return isInAnnualInterval(effectiveDt);
            default:
                throw new IllegalStateException("Unknown schedule type in TimeSpec.");
        }
    }

    /**
     * Returns the next blackout end time (as a ZonedDateTime in effectiveZone) that occurs strictly after dt.
     */
    public ZonedDateTime getBlackOutNextEndTime(ZonedDateTime dt) {
        ZonedDateTime effectiveDt = dt.withZoneSameInstant(effectiveZone);
        switch (startTime.getScheduleType()) {
            case DAILY:
                return getDailyNextEndTime(effectiveDt);
            case WEEKLY:
                return getWeeklyNextEndTime(effectiveDt);
            case MONTHLY:
                return getMonthlyNextEndTime(effectiveDt);
            case ANNUAL:
                return getAnnualNextEndTime(effectiveDt);
            default:
                throw new IllegalStateException("Unknown schedule type in TimeSpec.");
        }
    }

    // --- Helper methods for computing blackout boundaries ---

    // DAILY:
    private boolean isInDailyInterval(ZonedDateTime dt) {
        LocalTime time = dt.toLocalTime();
        LocalTime start = LocalTime.of(startTime.getHour(), startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime end = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);
        if (!start.isAfter(end)) {
            return !time.isBefore(start) && time.isBefore(end);
        } else {
            return !time.isBefore(start) || time.isBefore(end);
        }
    }

    private ZonedDateTime getDailyNextEndTime(ZonedDateTime dt) {
        LocalDate today = dt.toLocalDate();
        LocalTime end = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);
        ZonedDateTime candidate = today.atTime(end).atZone(effectiveZone);
        return dt.toLocalTime().isBefore(end) ? candidate : today.plusDays(1).atTime(end).atZone(effectiveZone);
    }

    // WEEKLY:
    private boolean isInWeeklyInterval(ZonedDateTime dt) {
        // Our spec uses day_of_week values: 0 or 7 represent Sunday; internally we convert 0 to 7.
        int sDOW = (startTime.getDayOfWeek() == null ? -1 : (startTime.getDayOfWeek() == 0 ? 7 : startTime.getDayOfWeek()));
        int eDOW = (endTime.getDayOfWeek() == null ? -1 : (endTime.getDayOfWeek() == 0 ? 7 : endTime.getDayOfWeek()));
        DayOfWeek startDow = DayOfWeek.of(sDOW);
        DayOfWeek endDow = DayOfWeek.of(eDOW);
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(), startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);

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
        int sDOW = (startTime.getDayOfWeek() == null ? -1 : (startTime.getDayOfWeek() == 0 ? 7 : startTime.getDayOfWeek()));
        int eDOW = (endTime.getDayOfWeek() == null ? -1 : (endTime.getDayOfWeek() == 0 ? 7 : endTime.getDayOfWeek()));
        DayOfWeek startDow = DayOfWeek.of(sDOW);
        DayOfWeek endDow = DayOfWeek.of(eDOW);
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(), startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);

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

    // ANNUAL:
    private boolean isInAnnualInterval(ZonedDateTime dt) {
        MonthDay startMD = MonthDay.of(startTime.getMonth(), startTime.getDayOfMonth());
        MonthDay endMD = MonthDay.of(endTime.getMonth(), endTime.getDayOfMonth());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(), startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);
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
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);
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

    // MONTHLY:
    private boolean isInMonthlyInterval(ZonedDateTime dt) {
        int startDay = startTime.getDayOfMonth();
        int endDay = endTime.getDayOfMonth();
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(), startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);

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
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);
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

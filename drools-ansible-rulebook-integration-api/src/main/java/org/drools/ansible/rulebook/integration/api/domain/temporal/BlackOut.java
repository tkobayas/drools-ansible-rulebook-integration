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

    public void validate() {
        if (startTime == null || endTime == null) {
            throw new IllegalStateException("black_out start and end times must be specified");
        }
        // WIP
    }

    /**
     * Returns true if the given LocalDateTime falls within the blackout period.
     * The schedule type is determined by the following rules:
     * <ul>
     *   <li><b>Monthly:</b> dayOfMonth is set and month is null.</li>
     *   <li><b>Annual:</b> both dayOfMonth and month are set.</li>
     *   <li><b>Weekly:</b> dayOfWeek is set.</li>
     *   <li><b>Daily:</b> hour (and optionally minute) is set.</li>
     * </ul>
     * (Previously named {@code isBlackOut}).
     */
    public boolean isBlackOutActive(LocalDateTime dt) {
        if (startTime.getDayOfMonth() != null && startTime.getMonth() == null) {
            return isInMonthlyInterval(dt);
        } else if (startTime.getMonth() != null && startTime.getDayOfMonth() != null) {
            return isInAnnualInterval(dt);
        } else if (startTime.getDayOfWeek() != null) {
            return isInWeeklyInterval(dt);
        } else if (startTime.getHour() != null) {
            return isInDailyInterval(dt);
        } else {
            throw new IllegalStateException("Insufficient time specification");
        }
    }

    /**
     * Returns the next blackout end time (the next occurrence when the blackout period ends)
     * that comes strictly after the provided {@code currentDateTime}.
     */
    public LocalDateTime getBlackOutNextEndTime(LocalDateTime currentDateTime) {
        if (startTime.getDayOfMonth() != null && startTime.getMonth() == null) {
            return getMonthlyNextEndTime(currentDateTime);
        } else if (startTime.getMonth() != null && startTime.getDayOfMonth() != null) {
            return getAnnualNextEndTime(currentDateTime);
        } else if (startTime.getDayOfWeek() != null) {
            return getWeeklyNextEndTime(currentDateTime);
        } else if (startTime.getHour() != null) {
            return getDailyNextEndTime(currentDateTime);
        } else {
            throw new IllegalStateException("Insufficient time specification for blackout schedule");
        }
    }

    // --- Daily schedule helpers ---
    private boolean isInDailyInterval(LocalDateTime dt) {
        LocalTime time = dt.toLocalTime();
        LocalTime start = LocalTime.of(startTime.getHour(),
                                       startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime end = LocalTime.of(endTime.getHour(),
                                     endTime.getMinute() != null ? endTime.getMinute() : 0);
        if (!start.isAfter(end)) {
            return !time.isBefore(start) && time.isBefore(end);
        } else { // spans midnight
            return !time.isBefore(start) || time.isBefore(end);
        }
    }

    private LocalDateTime getDailyNextEndTime(LocalDateTime currentDateTime) {
        LocalTime start = LocalTime.of(startTime.getHour(), startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime end = LocalTime.of(endTime.getHour(), endTime.getMinute() != null ? endTime.getMinute() : 0);
        LocalDate today = currentDateTime.toLocalDate();

        if (!start.isAfter(end)) {
            LocalDateTime candidate = LocalDateTime.of(today, end);
            return currentDateTime.isBefore(candidate) ? candidate : LocalDateTime.of(today.plusDays(1), end);
        } else {
            LocalTime currentTime = currentDateTime.toLocalTime();
            if (currentTime.isBefore(end)) {
                LocalDateTime candidate = LocalDateTime.of(today, end);
                return currentDateTime.isBefore(candidate) ? candidate : LocalDateTime.of(today.plusDays(1), end);
            } else {
                return LocalDateTime.of(today.plusDays(1), end);
            }
        }
    }

    // --- Weekly schedule helpers ---
    private boolean isInWeeklyInterval(LocalDateTime dt) {
        DayOfWeek startDow = DayOfWeek.of(startTime.getDayOfWeek());
        DayOfWeek endDow = DayOfWeek.of(endTime.getDayOfWeek());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(),
                                                startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);

        LocalDate dtDate = dt.toLocalDate();
        LocalDate startDate = dtDate.with(TemporalAdjusters.previousOrSame(startDow));
        LocalDateTime blackoutStart = LocalDateTime.of(startDate, startLocalTime);
        LocalDateTime blackoutEnd;
        if (startDow.equals(endDow)) {
            if (!startLocalTime.isAfter(endLocalTime)) {
                blackoutEnd = LocalDateTime.of(startDate, endLocalTime);
            } else {
                blackoutEnd = LocalDateTime.of(startDate.plusDays(1), endLocalTime);
            }
        } else {
            int daysBetween = endDow.getValue() - startDow.getValue();
            if (daysBetween <= 0) {
                daysBetween += 7;
            }
            blackoutEnd = LocalDateTime.of(startDate.plusDays(daysBetween), endLocalTime);
        }
        if (dt.isBefore(blackoutStart)) {
            blackoutStart = blackoutStart.minusWeeks(1);
            blackoutEnd = blackoutEnd.minusWeeks(1);
        }
        return !dt.isBefore(blackoutStart) && dt.isBefore(blackoutEnd);
    }

    private LocalDateTime getWeeklyNextEndTime(LocalDateTime currentDateTime) {
        DayOfWeek startDow = DayOfWeek.of(startTime.getDayOfWeek());
        DayOfWeek endDow = DayOfWeek.of(endTime.getDayOfWeek());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(),
                                                startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);

        LocalDate baseDate = currentDateTime.toLocalDate();
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
        if (!candidateBlackoutEnd.isAfter(currentDateTime)) {
            candidateBlackoutEnd = candidateBlackoutEnd.plusWeeks(1);
        }
        return candidateBlackoutEnd;
    }

    // --- Annual schedule helpers ---
    private boolean isInAnnualInterval(LocalDateTime dt) {
        MonthDay startMD = MonthDay.of(startTime.getMonth(), startTime.getDayOfMonth());
        MonthDay endMD = MonthDay.of(endTime.getMonth(), endTime.getDayOfMonth());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(),
                                                startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);
        int year = dt.getYear();

        LocalDateTime blackoutStart;
        LocalDateTime blackoutEnd;
        if (startMD.compareTo(endMD) <= 0) {
            blackoutStart = LocalDateTime.of(year, startTime.getMonth(), startTime.getDayOfMonth(),
                                             startLocalTime.getHour(), startLocalTime.getMinute());
            blackoutEnd = LocalDateTime.of(year, endTime.getMonth(), endTime.getDayOfMonth(),
                                           endLocalTime.getHour(), endLocalTime.getMinute());
        } else {
            if (MonthDay.from(dt).compareTo(startMD) >= 0) {
                blackoutStart = LocalDateTime.of(year, startTime.getMonth(), startTime.getDayOfMonth(),
                                                 startLocalTime.getHour(), startLocalTime.getMinute());
                blackoutEnd = LocalDateTime.of(year + 1, endTime.getMonth(), endTime.getDayOfMonth(),
                                               endLocalTime.getHour(), endLocalTime.getMinute());
            } else {
                blackoutStart = LocalDateTime.of(year - 1, startTime.getMonth(), startTime.getDayOfMonth(),
                                                 startLocalTime.getHour(), startLocalTime.getMinute());
                blackoutEnd = LocalDateTime.of(year, endTime.getMonth(), endTime.getDayOfMonth(),
                                               endLocalTime.getHour(), endLocalTime.getMinute());
            }
        }
        return !dt.isBefore(blackoutStart) && dt.isBefore(blackoutEnd);
    }

    private LocalDateTime getAnnualNextEndTime(LocalDateTime currentDateTime) {
        MonthDay startMD = MonthDay.of(startTime.getMonth(), startTime.getDayOfMonth());
        MonthDay endMD = MonthDay.of(endTime.getMonth(), endTime.getDayOfMonth());
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(),
                                                startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);
        int year = currentDateTime.getYear();
        LocalDateTime candidateBlackoutEnd;

        if (startMD.compareTo(endMD) <= 0) {
            candidateBlackoutEnd = LocalDateTime.of(year, endTime.getMonth(), endTime.getDayOfMonth(),
                                                    endLocalTime.getHour(), endLocalTime.getMinute());
            if (!candidateBlackoutEnd.isAfter(currentDateTime)) {
                candidateBlackoutEnd = LocalDateTime.of(year + 1, endTime.getMonth(), endTime.getDayOfMonth(),
                                                        endLocalTime.getHour(), endLocalTime.getMinute());
            }
        } else {
            if (MonthDay.from(currentDateTime).compareTo(startMD) >= 0) {
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

    // --- Monthly schedule helpers ---
    private boolean isInMonthlyInterval(LocalDateTime dt) {
        int startDay = startTime.getDayOfMonth();
        int endDay = endTime.getDayOfMonth();
        LocalTime startLocalTime = LocalTime.of(startTime.getHour(),
                                                startTime.getMinute() != null ? startTime.getMinute() : 0);
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);

        if (startDay <= endDay) {
            // Blackout period is within the same month.
            LocalDate periodStartDate = LocalDate.of(dt.getYear(), dt.getMonthValue(), startDay);
            LocalDate periodEndDate = LocalDate.of(dt.getYear(), dt.getMonthValue(), endDay);
            LocalDateTime blackoutStart = LocalDateTime.of(periodStartDate, startLocalTime);
            LocalDateTime blackoutEnd = LocalDateTime.of(periodEndDate, endLocalTime);
            return !dt.isBefore(blackoutStart) && dt.isBefore(blackoutEnd);
        } else {
            // Blackout period spans the month boundary.
            LocalDateTime blackoutStart;
            LocalDateTime blackoutEnd;
            if (dt.getDayOfMonth() >= startDay) {
                // Blackout starts this month and ends next month.
                blackoutStart = LocalDateTime.of(dt.getYear(), dt.getMonthValue(), startDay,
                                                 startLocalTime.getHour(), startLocalTime.getMinute());
                LocalDate nextMonth = dt.toLocalDate().plusMonths(1);
                blackoutEnd = LocalDateTime.of(nextMonth.getYear(), nextMonth.getMonthValue(), endDay,
                                               endLocalTime.getHour(), endLocalTime.getMinute());
            } else { // This covers dt.getDayOfMonth() < startDay (including when it equals the end day)
                LocalDate prevMonth = dt.toLocalDate().minusMonths(1);
                blackoutStart = LocalDateTime.of(prevMonth.getYear(), prevMonth.getMonthValue(), startDay,
                                                 startLocalTime.getHour(), startLocalTime.getMinute());
                blackoutEnd = LocalDateTime.of(dt.getYear(), dt.getMonthValue(), endDay,
                                               endLocalTime.getHour(), endLocalTime.getMinute());
            }
            return !dt.isBefore(blackoutStart) && dt.isBefore(blackoutEnd);
        }
    }

    private LocalDateTime getMonthlyNextEndTime(LocalDateTime currentDateTime) {
        int startDay = startTime.getDayOfMonth();
        int endDay = endTime.getDayOfMonth();
        LocalTime endLocalTime = LocalTime.of(endTime.getHour(),
                                              endTime.getMinute() != null ? endTime.getMinute() : 0);
        LocalDateTime candidate;
        if (startDay <= endDay) {
            candidate = LocalDateTime.of(currentDateTime.getYear(), currentDateTime.getMonthValue(), endDay,
                                         endLocalTime.getHour(), endLocalTime.getMinute());
            if (!candidate.isAfter(currentDateTime)) {
                candidate = candidate.plusMonths(1);
            }
        } else {
            if (currentDateTime.getDayOfMonth() >= startDay) {
                candidate = LocalDateTime.of(currentDateTime.toLocalDate().plusMonths(1).getYear(),
                                             currentDateTime.toLocalDate().plusMonths(1).getMonthValue(),
                                             endDay,
                                             endLocalTime.getHour(), endLocalTime.getMinute());
            } else {
                candidate = LocalDateTime.of(currentDateTime.getYear(), currentDateTime.getMonthValue(), endDay,
                                             endLocalTime.getHour(), endLocalTime.getMinute());
                if (!candidate.isAfter(currentDateTime)) {
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

package org.drools.ansible.rulebook.integration.api.domain.temporal;

/**
 * Represents a cron-like time specification.
 */
public class TimeSpec {
    // These fields are all nullable, so that a null value means "*" (any value).
    // If null,
    //   - a time field which is larger than a specified field means "*" (any value).
    //   - a time field which is smaller than a specified field means "0" (the smallest value).
    // dayOfWeek and dayOfMonth are mutually exclusive. If one is null, it's ignored (not any value). If both are specified, raise an error.
    private Integer minute;      // 0 - 59
    private Integer hour;        // 0 - 23
    private Integer dayOfWeek;   // 0-7 (0/7 is Sunday, 1 Monday, 2 Tuesday...6 Saturday)
    private Integer dayOfMonth;  // 1-31
    private Integer month;       // 1 - 12 (1 = January)

    public TimeSpec() {}

    public TimeSpec(Integer minute, Integer hour, Integer dayOfWeek, Integer dayOfMonth, Integer month) {
        this.minute = minute;
        this.hour = hour;
        this.dayOfWeek = dayOfWeek;
        this.dayOfMonth = dayOfMonth;
        this.month = month;
    }

    public Integer getMinute() {
        return minute;
    }

    public void setMinute(Integer minute) {
        this.minute = minute;
    }

    public Integer getHour() {
        return hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }

    public Integer getDayOfWeek() {
        return dayOfWeek;
    }

    public Integer getDay_of_week() {
        return dayOfWeek;
    }

    public void setDay_of_week(Integer dayOfWeek) {
        this.dayOfWeek = dayOfWeek;
    }

    public Integer getDayOfMonth() {
        return dayOfMonth;
    }

    public Integer getDay_of_month() {
        return dayOfMonth;
    }

    public void setDay_of_month(Integer dayOfMonth) {
        this.dayOfMonth = dayOfMonth;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    @Override
    public String toString() {
        return "TimeSpec{" +
                "minute=" + minute +
                ", hour=" + hour +
                ", dayOfWeek=" + dayOfWeek +
                ", dayOfMonth=" + dayOfMonth +
                ", month=" + month +
                '}';
    }

    public enum ScheduleType {
        DAILY,
        WEEKLY,
        MONTHLY,
        ANNUAL,
        UNKNOWN
    }

    /**
     * Determines the schedule type based on which fields are set.
     */
    public ScheduleType getScheduleType() {
        if (hour == null) {
            return ScheduleType.UNKNOWN;
        }
        // Daily: month, dayOfWeek, and dayOfMonth are null.
        if (month == null && dayOfWeek == null && dayOfMonth == null) {
            return ScheduleType.DAILY;
        }
        // Weekly: month is null, dayOfWeek is non-null, and dayOfMonth is null.
        if (month == null && dayOfWeek != null && dayOfMonth == null) {
            return ScheduleType.WEEKLY;
        }
        // Monthly: month and dayOfWeek are null, and dayOfMonth is non-null.
        if (month == null && dayOfWeek == null && dayOfMonth != null) {
            return ScheduleType.MONTHLY;
        }
        // Annual: month and dayOfMonth are non-null and dayOfWeek is null.
        if (month != null && dayOfWeek == null && dayOfMonth != null) {
            return ScheduleType.ANNUAL;
        }
        return ScheduleType.UNKNOWN;
    }
}

package org.drools.ansible.rulebook.integration.api.domain.temporal;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class BlackOutUnitTest {

    @Parameterized.Parameter
    public BlackOut.Timezone timezone;

    @Parameterized.Parameters(name = "Timezone: {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {BlackOut.Timezone.UTC},
                {BlackOut.Timezone.LOCAL}
        });
    }

    // Helper: converts a LocalDateTime to a ZonedDateTime using the BlackOut's effective zone.
    private ZonedDateTime toZdt(LocalDateTime ldt, BlackOut blackOut) {
        return ldt.atZone(blackOut.getEffectiveZone());
    }

    // --------------------------------------------------------------------
    // VALIDATION TESTS
    // --------------------------------------------------------------------

    @Test
    public void testValidateValidDaily() {
        // Daily: month, dayOfWeek, dayOfMonth must be null; hour required; minute optional.
        TimeSpec daily = new TimeSpec(30, 14, null, null, null);
        BlackOut blackout = new BlackOut(daily, daily, timezone);
        // Should not throw an exception.
        blackout.validate();
    }

    @Test
    public void testValidateValidWeekly() {
        // Weekly: month and dayOfMonth null; dayOfWeek required.
        TimeSpec weekly = new TimeSpec(0, 3, 4, null, null); // 4 = Thursday
        BlackOut blackout = new BlackOut(weekly, weekly, timezone);
        blackout.validate();
    }

    @Test
    public void testValidateValidMonthly() {
        // Monthly: month and dayOfWeek null; dayOfMonth required.
        TimeSpec monthly = new TimeSpec(0, 20, null, 15, null);
        BlackOut blackout = new BlackOut(monthly, monthly, timezone);
        blackout.validate();
    }

    @Test
    public void testValidateValidAnnual() {
        // Annual: dayOfWeek null; month and dayOfMonth required.
        TimeSpec annual = new TimeSpec(0, 0, null, 23, 12);
        BlackOut blackout = new BlackOut(annual, annual, timezone);
        blackout.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateInvalidHour() {
        // Hour out-of-range (e.g. 25).
        TimeSpec invalid = new TimeSpec(0, 25, null, null, null);
        TimeSpec daily = new TimeSpec(0, 14, null, null, null);
        BlackOut blackout = new BlackOut(invalid, daily, timezone);
        blackout.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateInvalidCombinationDaily() {
        // Daily schedule but dayOfMonth is set.
        TimeSpec invalidDaily = new TimeSpec(0, 14, null, 10, null);
        TimeSpec validDaily = new TimeSpec(0, 14, null, null, null);
        BlackOut blackout = new BlackOut(invalidDaily, validDaily, timezone);
        blackout.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateDifferentScheduleTypes() {
        // Start is daily; end is weekly.
        TimeSpec daily = new TimeSpec(0, 14, null, null, null);
        TimeSpec weekly = new TimeSpec(0, 14, 4, null, null);
        BlackOut blackout = new BlackOut(daily, weekly, timezone);
        blackout.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateInvalidAnnualDate() {
        // Create an annual schedule with an impossible date: April 31 (April has only 30 days).
        // For an annual schedule, month and day_of_month are required.
        TimeSpec invalidAnnual = new TimeSpec(0, 10, null, 31, 4);
        BlackOut blackout = new BlackOut(invalidAnnual, invalidAnnual, timezone);
        blackout.validate();
    }

    // --------------------------------------------------------------------
    // DAILY SCHEDULE TESTS
    // --------------------------------------------------------------------

    @Test
    public void testDailyNonSpanning() {
        // Daily: from 14:30 to 16:15.
        TimeSpec start = new TimeSpec(30, 14, null, null, null);
        TimeSpec end = new TimeSpec(15, 16, null, null, null);
        BlackOut blackout = new BlackOut(start, end, timezone);
        blackout.validate();

        // 14:45 should be active.
        ZonedDateTime dtInside = toZdt(LocalDateTime.of(2025, 2, 14, 14, 45), blackout);
        assertThat(blackout.isBlackOutActive(dtInside))
                .as("Daily non-spanning: 14:45 active for tz=" + timezone)
                .isTrue();

        // 14:20 should be inactive.
        ZonedDateTime dtBefore = toZdt(LocalDateTime.of(2025, 2, 14, 14, 20), blackout);
        assertThat(blackout.isBlackOutActive(dtBefore))
                .as("Daily non-spanning: 14:20 inactive for tz=" + timezone)
                .isFalse();

        // 16:20 should be inactive.
        ZonedDateTime dtAfter = toZdt(LocalDateTime.of(2025, 2, 14, 16, 20), blackout);
        assertThat(blackout.isBlackOutActive(dtAfter))
                .as("Daily non-spanning: 16:20 inactive for tz=" + timezone)
                .isFalse();

        // Next end time for 14:45 should be 16:15 the same day.
        ZonedDateTime nextEnd = blackout.getBlackOutNextEndTime(dtInside);
        ZonedDateTime expectedEnd = toZdt(LocalDateTime.of(2025, 2, 14, 16, 15), blackout);
        assertThat(nextEnd).isEqualTo(expectedEnd);
    }

    @Test
    public void testDailySpanning() {
        // Daily spanning midnight: from 22:00 to 02:00.
        TimeSpec start = new TimeSpec(0, 22, null, null, null);
        TimeSpec end = new TimeSpec(0, 2, null, null, null);
        BlackOut blackout = new BlackOut(start, end, timezone);
        blackout.validate();

        // 22:30 should be active.
        ZonedDateTime dtStart = toZdt(LocalDateTime.of(2025, 6, 10, 22, 30), blackout);
        assertThat(blackout.isBlackOutActive(dtStart))
                .as("Daily spanning: 22:30 active for tz=" + timezone)
                .isTrue();

        // 01:30 next day should be active.
        ZonedDateTime dtMid = toZdt(LocalDateTime.of(2025, 6, 11, 1, 30), blackout);
        assertThat(blackout.isBlackOutActive(dtMid))
                .as("Daily spanning: 01:30 active for tz=" + timezone)
                .isTrue();

        // Exactly at 02:00 should be inactive.
        ZonedDateTime dtEnd = toZdt(LocalDateTime.of(2025, 6, 11, 2, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtEnd))
                .as("Daily spanning: 02:00 inactive for tz=" + timezone)
                .isFalse();

        // Next end time for 22:30 should be the next day at 02:00.
        ZonedDateTime nextEnd = blackout.getBlackOutNextEndTime(dtStart);
        ZonedDateTime expectedEnd = toZdt(LocalDateTime.of(2025, 6, 11, 2, 0), blackout);
        assertThat(nextEnd).isEqualTo(expectedEnd);
    }

    // --------------------------------------------------------------------
    // WEEKLY SCHEDULE TESTS
    // --------------------------------------------------------------------

    @Test
    public void testWeeklyNonSpanning() {
        // Weekly: from 03:00 to 04:00 on Thursday (dayOfWeek=4; note: 0/7=Sunday).
        TimeSpec start = new TimeSpec(0, 3, 4, null, null);
        TimeSpec end = new TimeSpec(0, 4, 4, null, null);
        BlackOut blackout = new BlackOut(start, end, timezone);
        blackout.validate();

        ZonedDateTime dtInside = toZdt(LocalDateTime.of(2025, 2, 13, 3, 30), blackout);
        assertThat(blackout.isBlackOutActive(dtInside))
                .as("Weekly non-spanning: 03:30 active for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtBefore = toZdt(LocalDateTime.of(2025, 2, 13, 2, 30), blackout);
        assertThat(blackout.isBlackOutActive(dtBefore))
                .as("Weekly non-spanning: 02:30 inactive for tz=" + timezone)
                .isFalse();

        ZonedDateTime dtAfter = toZdt(LocalDateTime.of(2025, 2, 13, 4, 30), blackout);
        assertThat(blackout.isBlackOutActive(dtAfter))
                .as("Weekly non-spanning: 04:30 inactive for tz=" + timezone)
                .isFalse();

        // Next end time for 03:30 should be 04:00 the same Thursday.
        ZonedDateTime nextEnd = blackout.getBlackOutNextEndTime(dtInside);
        ZonedDateTime expectedEnd = toZdt(LocalDateTime.of(2025, 2, 13, 4, 0), blackout);
        assertThat(nextEnd).isEqualTo(expectedEnd);
    }

    @Test
    public void testWeeklySpanning() {
        // Weekly spanning midnight: from Thursday 23:00 to Thursday 02:00 (spanning midnight into Friday).
        // Use dayOfWeek = 4 (Thursday).
        TimeSpec start = new TimeSpec(0, 23, 4, null, null);
        TimeSpec end = new TimeSpec(0, 2, 4, null, null);
        BlackOut blackout = new BlackOut(start, end, timezone);
        blackout.validate();

        ZonedDateTime dtStart = toZdt(LocalDateTime.of(2025, 6, 12, 23, 30), blackout);
        assertThat(blackout.isBlackOutActive(dtStart))
                .as("Weekly spanning: 23:30 active for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtMid = toZdt(LocalDateTime.of(2025, 6, 13, 1, 30), blackout);
        assertThat(blackout.isBlackOutActive(dtMid))
                .as("Weekly spanning: 01:30 active for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtEnd = toZdt(LocalDateTime.of(2025, 6, 13, 2, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtEnd))
                .as("Weekly spanning: 02:00 inactive for tz=" + timezone)
                .isFalse();

        ZonedDateTime nextEnd = blackout.getBlackOutNextEndTime(dtStart);
        ZonedDateTime expectedEnd = toZdt(LocalDateTime.of(2025, 6, 13, 2, 0), blackout);
        assertThat(nextEnd).isEqualTo(expectedEnd);
    }

    // --------------------------------------------------------------------
    // MONTHLY SCHEDULE TESTS
    // --------------------------------------------------------------------

    @Test
    public void testMonthlyNonSpanning() {
        // Monthly: from the 15th at 20:00 to the 16th at 02:00.
        TimeSpec start = new TimeSpec(0, 20, null, 15, null);
        TimeSpec end = new TimeSpec(0, 2, null, 16, null);
        BlackOut blackout = new BlackOut(start, end, timezone);
        blackout.validate();

        ZonedDateTime dtInside = toZdt(LocalDateTime.of(2025, 3, 15, 21, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtInside))
                .as("Monthly non-spanning: 15th 21:00 active for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtBefore = toZdt(LocalDateTime.of(2025, 3, 15, 19, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtBefore))
                .as("Monthly non-spanning: 15th 19:00 inactive for tz=" + timezone)
                .isFalse();

        ZonedDateTime dtAtEnd = toZdt(LocalDateTime.of(2025, 3, 16, 2, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtAtEnd))
                .as("Monthly non-spanning: 16th 02:00 inactive for tz=" + timezone)
                .isFalse();

        ZonedDateTime nextEnd = blackout.getBlackOutNextEndTime(dtInside);
        ZonedDateTime expectedEnd = toZdt(LocalDateTime.of(2025, 3, 16, 2, 0), blackout);
        assertThat(nextEnd).isEqualTo(expectedEnd);
    }

    @Test
    public void testMonthlySpanning() {
        // Monthly spanning: from the 28th at 22:00 to the 2nd at 06:00.
        TimeSpec start = new TimeSpec(0, 22, null, 28, null);
        TimeSpec end = new TimeSpec(0, 6, null, 2, null);
        BlackOut blackout = new BlackOut(start, end, timezone);
        blackout.validate();

        ZonedDateTime dtInside = toZdt(LocalDateTime.of(2025, 5, 29, 23, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtInside))
                .as("Monthly spanning: May 29 23:00 active for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtInside2 = toZdt(LocalDateTime.of(2025, 6, 2, 5, 30), blackout);
        assertThat(blackout.isBlackOutActive(dtInside2))
                .as("Monthly spanning: June 2 05:30 active for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtAtEnd = toZdt(LocalDateTime.of(2025, 6, 2, 6, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtAtEnd))
                .as("Monthly spanning: June 2 06:00 inactive for tz=" + timezone)
                .isFalse();

        ZonedDateTime nextEnd = blackout.getBlackOutNextEndTime(dtInside);
        ZonedDateTime expectedEnd = toZdt(LocalDateTime.of(2025, 6, 2, 6, 0), blackout);
        assertThat(nextEnd).isEqualTo(expectedEnd);
    }

    // --------------------------------------------------------------------
    // ANNUAL SCHEDULE TESTS
    // --------------------------------------------------------------------

    @Test
    public void testAnnual() {
        // Annual: from December 23 00:00 to January 2 00:00.
        TimeSpec start = new TimeSpec(0, 0, null, 23, 12);
        TimeSpec end = new TimeSpec(0, 0, null, 2, 1);
        BlackOut blackout = new BlackOut(start, end, timezone);
        blackout.validate();

        ZonedDateTime dtInside = toZdt(LocalDateTime.of(2025, 12, 25, 10, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtInside))
                .as("Annual: Dec 25 10:00 active for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtBefore = toZdt(LocalDateTime.of(2025, 12, 22, 10, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtBefore))
                .as("Annual: Dec 22 10:00 inactive for tz=" + timezone)
                .isFalse();

        ZonedDateTime dtAfter = toZdt(LocalDateTime.of(2026, 1, 3, 10, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtAfter))
                .as("Annual: Jan 3 10:00 inactive for tz=" + timezone)
                .isFalse();

        ZonedDateTime nextEnd = blackout.getBlackOutNextEndTime(dtInside);
        ZonedDateTime expectedEnd = toZdt(LocalDateTime.of(2026, 1, 2, 0, 0), blackout);
        assertThat(nextEnd).isEqualTo(expectedEnd);
    }

    @Test
    public void testAnnualOnBoundary() {
        // Annual: from December 23 00:00 to January 2 00:00.
        TimeSpec start = new TimeSpec(0, 0, null, 23, 12);
        TimeSpec end = new TimeSpec(0, 0, null, 2, 1);
        BlackOut blackout = new BlackOut(start, end, timezone);
        blackout.validate();

        ZonedDateTime dtStart = toZdt(LocalDateTime.of(2025, 12, 23, 0, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtStart))
                .as("Annual on-boundary: Dec 23 00:00 active for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtJustBeforeEnd = toZdt(LocalDateTime.of(2026, 1, 1, 23, 59), blackout);
        assertThat(blackout.isBlackOutActive(dtJustBeforeEnd))
                .as("Annual on-boundary: Jan 1 23:59 active for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtEnd = toZdt(LocalDateTime.of(2026, 1, 2, 0, 0), blackout);
        assertThat(blackout.isBlackOutActive(dtEnd))
                .as("Annual on-boundary: Jan 2 00:00 inactive for tz=" + timezone)
                .isFalse();
    }
}
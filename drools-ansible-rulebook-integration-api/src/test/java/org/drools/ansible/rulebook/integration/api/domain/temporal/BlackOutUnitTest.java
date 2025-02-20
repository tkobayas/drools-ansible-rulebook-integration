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
        return Arrays.asList(new Object[][] {
                { BlackOut.Timezone.UTC },
                { BlackOut.Timezone.LOCAL }
        });
    }

    // Helper: given a LocalDateTime, convert it into a ZonedDateTime using the BlackOut's effectiveZone.
    private ZonedDateTime toZdt(LocalDateTime ldt, BlackOut blackOut) {
        return ldt.atZone(blackOut.getEffectiveZone());
    }

    // ====================
    // DAILY TESTS
    // ====================

    @Test
    public void testDailyIsBlackOut() {
        TimeSpec dailyStart = new TimeSpec(30, 14, null, null, null);
        TimeSpec dailyEnd = new TimeSpec(15, 16, null, null, null);
        BlackOut dailyBlackOut = new BlackOut(dailyStart, dailyEnd, timezone);

        // Build ZonedDateTime from LocalDateTime using the blackout's effective zone.
        ZonedDateTime dtInside = toZdt(LocalDateTime.of(2025, 2, 14, 15, 0), dailyBlackOut);

        assertThat(dailyBlackOut.isBlackOutActive(dtInside))
                .as("Daily inside for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtBefore = toZdt(LocalDateTime.of(2025, 2, 14, 14, 0), dailyBlackOut);
        assertThat(dailyBlackOut.isBlackOutActive(dtBefore))
                .as("Daily before for tz=" + timezone)
                .isFalse();

        ZonedDateTime dtAfter = toZdt(LocalDateTime.of(2025, 2, 14, 16, 30), dailyBlackOut);
        assertThat(dailyBlackOut.isBlackOutActive(dtAfter))
                .as("Daily after for tz=" + timezone)
                .isFalse();
    }

    @Test
    public void testDailyNextEndTime() {
        TimeSpec dailyStart = new TimeSpec(30, 14, null, null, null);
        TimeSpec dailyEnd = new TimeSpec(15, 16, null, null, null);
        BlackOut dailyBlackOut = new BlackOut(dailyStart, dailyEnd, timezone);

        ZonedDateTime current = toZdt(LocalDateTime.of(2025, 2, 14, 15, 30), dailyBlackOut);
        ZonedDateTime expected = toZdt(LocalDateTime.of(2025, 2, 14, 16, 15), dailyBlackOut);
        assertThat(dailyBlackOut.getBlackOutNextEndTime(current))
                .as("Daily next end time for tz=" + timezone)
                .isEqualTo(expected);

        current = toZdt(LocalDateTime.of(2025, 2, 14, 16, 30), dailyBlackOut);
        expected = toZdt(LocalDateTime.of(2025, 2, 15, 16, 15), dailyBlackOut);
        assertThat(dailyBlackOut.getBlackOutNextEndTime(current))
                .as("Daily next end time (after period) for tz=" + timezone)
                .isEqualTo(expected);
    }

    @Test
    public void testDailySpanningBoundaryIsBlackOut() {
        // Blackout from 22:00 to 02:00 spanning midnight.
        TimeSpec dailyStart = new TimeSpec(0, 22, null, null, null);
        TimeSpec dailyEnd = new TimeSpec(0, 2, null, null, null);
        BlackOut dailyBlackOut = new BlackOut(dailyStart, dailyEnd, timezone);

        ZonedDateTime dtStartBoundary = toZdt(LocalDateTime.of(2025, 6, 10, 22, 0), dailyBlackOut);
        assertThat(dailyBlackOut.isBlackOutActive(dtStartBoundary))
                .as("Daily spanning at start boundary for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtBeforeMidnight = toZdt(LocalDateTime.of(2025, 6, 10, 23, 30), dailyBlackOut);
        assertThat(dailyBlackOut.isBlackOutActive(dtBeforeMidnight))
                .as("Daily spanning before midnight for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtAfterMidnight = toZdt(LocalDateTime.of(2025, 6, 11, 1, 30), dailyBlackOut);
        assertThat(dailyBlackOut.isBlackOutActive(dtAfterMidnight))
                .as("Daily spanning after midnight for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtEndBoundary = toZdt(LocalDateTime.of(2025, 6, 11, 2, 0), dailyBlackOut);
        assertThat(dailyBlackOut.isBlackOutActive(dtEndBoundary))
                .as("Daily spanning at end boundary for tz=" + timezone)
                .isFalse();
    }

    // ====================
    // WEEKLY TESTS
    // ====================

    @Test
    public void testWeeklyIsBlackOut() {
        // Weekly blackout: Every Thursday from 03:00 to 04:00.
        TimeSpec weeklyStart = new TimeSpec(null, 3, 4, null, null);
        TimeSpec weeklyEnd = new TimeSpec(null, 4, 4, null, null);
        BlackOut weeklyBlackOut = new BlackOut(weeklyStart, weeklyEnd, timezone);

        ZonedDateTime dtInside = toZdt(LocalDateTime.of(2025, 2, 13, 3, 30), weeklyBlackOut);
        assertThat(weeklyBlackOut.isBlackOutActive(dtInside))
                .as("Weekly inside for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtBefore = toZdt(LocalDateTime.of(2025, 2, 13, 2, 30), weeklyBlackOut);
        assertThat(weeklyBlackOut.isBlackOutActive(dtBefore))
                .as("Weekly before for tz=" + timezone)
                .isFalse();

        ZonedDateTime dtAfter = toZdt(LocalDateTime.of(2025, 2, 13, 4, 30), weeklyBlackOut);
        assertThat(weeklyBlackOut.isBlackOutActive(dtAfter))
                .as("Weekly after for tz=" + timezone)
                .isFalse();
    }

    @Test
    public void testWeeklyNextEndTime() {
        TimeSpec weeklyStart = new TimeSpec(null, 3, 4, null, null);
        TimeSpec weeklyEnd = new TimeSpec(null, 4, 4, null, null);
        BlackOut weeklyBlackOut = new BlackOut(weeklyStart, weeklyEnd, timezone);

        ZonedDateTime current = toZdt(LocalDateTime.of(2025, 2, 13, 3, 30), weeklyBlackOut);
        ZonedDateTime expected = toZdt(LocalDateTime.of(2025, 2, 13, 4, 0), weeklyBlackOut);
        assertThat(weeklyBlackOut.getBlackOutNextEndTime(current))
                .as("Weekly next end time for tz=" + timezone)
                .isEqualTo(expected);

        current = toZdt(LocalDateTime.of(2025, 2, 13, 4, 30), weeklyBlackOut);
        expected = toZdt(LocalDateTime.of(2025, 2, 20, 4, 0), weeklyBlackOut);
        assertThat(weeklyBlackOut.getBlackOutNextEndTime(current))
                .as("Weekly next end time (after period) for tz=" + timezone)
                .isEqualTo(expected);
    }

    @Test
    public void testWeeklySpanningBoundaryIsBlackOut() {
        // Weekly blackout spanning midnight: Thursday from 23:00 to 02:00.
        // We use dayOfWeek = 4 for Thursday.
        TimeSpec weeklyStart = new TimeSpec(null, 23, 4, null, null);
        TimeSpec weeklyEnd = new TimeSpec(null, 2, 4, null, null);
        BlackOut weeklyBlackOut = new BlackOut(weeklyStart, weeklyEnd, timezone);

        ZonedDateTime dtStart = toZdt(LocalDateTime.of(2025, 6, 12, 23, 0), weeklyBlackOut);
        assertThat(weeklyBlackOut.isBlackOutActive(dtStart))
                .as("Weekly spanning at start for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtDuring = toZdt(LocalDateTime.of(2025, 6, 12, 23, 30), weeklyBlackOut);
        assertThat(weeklyBlackOut.isBlackOutActive(dtDuring))
                .as("Weekly spanning during for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtAfterMidnight = toZdt(LocalDateTime.of(2025, 6, 13, 1, 30), weeklyBlackOut);
        assertThat(weeklyBlackOut.isBlackOutActive(dtAfterMidnight))
                .as("Weekly spanning after midnight for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtEnd = toZdt(LocalDateTime.of(2025, 6, 13, 2, 0), weeklyBlackOut);
        assertThat(weeklyBlackOut.isBlackOutActive(dtEnd))
                .as("Weekly spanning at end for tz=" + timezone)
                .isFalse();
    }

    // ====================
    // ANNUAL TESTS
    // ====================

    @Test
    public void testAnnualIsBlackOut() {
        // Annual blackout: December 23 00:00 to January 2 00:00.
        TimeSpec annualStart = new TimeSpec(null, 0, null, 23, 12);
        TimeSpec annualEnd = new TimeSpec(null, 0, null, 2, 1);
        BlackOut annualBlackOut = new BlackOut(annualStart, annualEnd, timezone);

        ZonedDateTime dtInside = toZdt(LocalDateTime.of(2025, 12, 25, 10, 0), annualBlackOut);
        assertThat(annualBlackOut.isBlackOutActive(dtInside))
                .as("Annual inside for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtBefore = toZdt(LocalDateTime.of(2025, 12, 22, 10, 0), annualBlackOut);
        assertThat(annualBlackOut.isBlackOutActive(dtBefore))
                .as("Annual before for tz=" + timezone)
                .isFalse();

        ZonedDateTime dtAfter = toZdt(LocalDateTime.of(2026, 1, 3, 10, 0), annualBlackOut);
        assertThat(annualBlackOut.isBlackOutActive(dtAfter))
                .as("Annual after for tz=" + timezone)
                .isFalse();
    }

    @Test
    public void testAnnualNextEndTime() {
        TimeSpec annualStart = new TimeSpec(null, 0, null, 23, 12);
        TimeSpec annualEnd = new TimeSpec(null, 0, null, 2, 1);
        BlackOut annualBlackOut = new BlackOut(annualStart, annualEnd, timezone);

        ZonedDateTime current = toZdt(LocalDateTime.of(2025, 12, 25, 10, 0), annualBlackOut);
        ZonedDateTime expected = toZdt(LocalDateTime.of(2026, 1, 2, 0, 0), annualBlackOut);
        assertThat(annualBlackOut.getBlackOutNextEndTime(current))
                .as("Annual next end time for tz=" + timezone)
                .isEqualTo(expected);

        current = toZdt(LocalDateTime.of(2025, 12, 22, 10, 0), annualBlackOut);
        expected = toZdt(LocalDateTime.of(2026, 1, 2, 0, 0), annualBlackOut);
        assertThat(annualBlackOut.getBlackOutNextEndTime(current))
                .as("Annual next end time (before) for tz=" + timezone)
                .isEqualTo(expected);

        current = toZdt(LocalDateTime.of(2026, 1, 3, 10, 0), annualBlackOut);
        expected = toZdt(LocalDateTime.of(2027, 1, 2, 0, 0), annualBlackOut);
        assertThat(annualBlackOut.getBlackOutNextEndTime(current))
                .as("Annual next end time (after) for tz=" + timezone)
                .isEqualTo(expected);
    }

    @Test
    public void testAnnualOnBoundary() {
        TimeSpec annualStart = new TimeSpec(null, 0, null, 23, 12);
        TimeSpec annualEnd = new TimeSpec(null, 0, null, 2, 1);
        BlackOut annualBlackOut = new BlackOut(annualStart, annualEnd, timezone);

        ZonedDateTime dtStart = toZdt(LocalDateTime.of(2025, 12, 23, 0, 0), annualBlackOut);
        assertThat(annualBlackOut.isBlackOutActive(dtStart))
                .as("Annual on boundary (start) for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtJustBeforeEnd = toZdt(LocalDateTime.of(2026, 1, 1, 23, 59), annualBlackOut);
        assertThat(annualBlackOut.isBlackOutActive(dtJustBeforeEnd))
                .as("Annual on boundary (just before end) for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtEnd = toZdt(LocalDateTime.of(2026, 1, 2, 0, 0), annualBlackOut);
        assertThat(annualBlackOut.isBlackOutActive(dtEnd))
                .as("Annual on boundary (end) for tz=" + timezone)
                .isFalse();
    }

    // ====================
    // MONTHLY TESTS
    // ====================

    @Test
    public void testMonthlyIsBlackOut() {
        // Monthly blackout: Every month from the 15th at 20:00 to the 16th at 02:00.
        TimeSpec monthlyStart = new TimeSpec(0, 20, null, 15, null);
        TimeSpec monthlyEnd = new TimeSpec(0, 2, null, 16, null);
        BlackOut monthlyBlackOut = new BlackOut(monthlyStart, monthlyEnd, timezone);

        ZonedDateTime dtInside = toZdt(LocalDateTime.of(2025, 3, 15, 21, 0), monthlyBlackOut);
        assertThat(monthlyBlackOut.isBlackOutActive(dtInside))
                .as("Monthly inside for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtAtEnd = toZdt(LocalDateTime.of(2025, 3, 16, 2, 0), monthlyBlackOut);
        assertThat(monthlyBlackOut.isBlackOutActive(dtAtEnd))
                .as("Monthly at end for tz=" + timezone)
                .isFalse();

        ZonedDateTime dtJustBeforeEnd = toZdt(LocalDateTime.of(2025, 3, 16, 1, 59), monthlyBlackOut);
        assertThat(monthlyBlackOut.isBlackOutActive(dtJustBeforeEnd))
                .as("Monthly just before end for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtBefore = toZdt(LocalDateTime.of(2025, 3, 15, 19, 59), monthlyBlackOut);
        assertThat(monthlyBlackOut.isBlackOutActive(dtBefore))
                .as("Monthly before period for tz=" + timezone)
                .isFalse();
    }

    @Test
    public void testMonthlyNextEndTime() {
        TimeSpec monthlyStart = new TimeSpec(0, 20, null, 15, null);
        TimeSpec monthlyEnd = new TimeSpec(0, 2, null, 16, null);
        BlackOut monthlyBlackOut = new BlackOut(monthlyStart, monthlyEnd, timezone);

        ZonedDateTime current = toZdt(LocalDateTime.of(2025, 3, 15, 21, 0), monthlyBlackOut);
        ZonedDateTime expected = toZdt(LocalDateTime.of(2025, 3, 16, 2, 0), monthlyBlackOut);
        assertThat(monthlyBlackOut.getBlackOutNextEndTime(current))
                .as("Monthly next end time for tz=" + timezone)
                .isEqualTo(expected);

        current = toZdt(LocalDateTime.of(2025, 3, 16, 2, 1), monthlyBlackOut);
        expected = toZdt(LocalDateTime.of(2025, 4, 16, 2, 0), monthlyBlackOut);
        assertThat(monthlyBlackOut.getBlackOutNextEndTime(current))
                .as("Monthly next end time (after period) for tz=" + timezone)
                .isEqualTo(expected);

        current = toZdt(LocalDateTime.of(2025, 3, 15, 19, 0), monthlyBlackOut);
        expected = toZdt(LocalDateTime.of(2025, 3, 16, 2, 0), monthlyBlackOut);
        assertThat(monthlyBlackOut.getBlackOutNextEndTime(current))
                .as("Monthly next end time (before period) for tz=" + timezone)
                .isEqualTo(expected);
    }

    @Test
    public void testMonthlySpanningBoundaryIsBlackOut() {
        // Monthly blackout spanning the month boundary: from the 28th at 22:00 to the 2nd at 06:00.
        TimeSpec monthlyStart = new TimeSpec(0, 22, null, 28, null);
        TimeSpec monthlyEnd = new TimeSpec(0, 6, null, 2, null);
        BlackOut monthlyBlackOut = new BlackOut(monthlyStart, monthlyEnd, timezone);

        ZonedDateTime dtInside = toZdt(LocalDateTime.of(2025, 5, 29, 23, 0), monthlyBlackOut);
        assertThat(monthlyBlackOut.isBlackOutActive(dtInside))
                .as("Monthly spanning inside for tz=" + timezone)
                .isTrue();

        dtInside = toZdt(LocalDateTime.of(2025, 6, 2, 5, 30), monthlyBlackOut);
        assertThat(monthlyBlackOut.isBlackOutActive(dtInside))
                .as("Monthly spanning inside (new month) for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtAtEnd = toZdt(LocalDateTime.of(2025, 6, 2, 6, 0), monthlyBlackOut);
        assertThat(monthlyBlackOut.isBlackOutActive(dtAtEnd))
                .as("Monthly spanning at end for tz=" + timezone)
                .isFalse();
    }

    @Test
    public void testMonthlySpanningBoundaryNextEndTime() {
        TimeSpec monthlyStart = new TimeSpec(0, 22, null, 28, null);
        TimeSpec monthlyEnd = new TimeSpec(0, 6, null, 2, null);
        BlackOut monthlyBlackOut = new BlackOut(monthlyStart, monthlyEnd, timezone);

        ZonedDateTime current = toZdt(LocalDateTime.of(2025, 5, 29, 23, 0), monthlyBlackOut);
        ZonedDateTime expected = toZdt(LocalDateTime.of(2025, 6, 2, 6, 0), monthlyBlackOut);
        assertThat(monthlyBlackOut.getBlackOutNextEndTime(current))
                .as("Monthly spanning next end time for tz=" + timezone)
                .isEqualTo(expected);

        current = toZdt(LocalDateTime.of(2025, 6, 2, 6, 1), monthlyBlackOut);
        expected = toZdt(LocalDateTime.of(2025, 7, 2, 6, 0), monthlyBlackOut);
        assertThat(monthlyBlackOut.getBlackOutNextEndTime(current))
                .as("Monthly spanning next end time (after period) for tz=" + timezone)
                .isEqualTo(expected);
    }

    @Test
    public void testMonthlyOnBoundary() {
        // For a monthly blackout from the 15th at 20:00 to the 16th at 02:00.
        TimeSpec monthlyStart = new TimeSpec(0, 20, null, 15, null);
        TimeSpec monthlyEnd = new TimeSpec(0, 2, null, 16, null);
        BlackOut monthlyBlackOut = new BlackOut(monthlyStart, monthlyEnd, timezone);

        ZonedDateTime dtStart = toZdt(LocalDateTime.of(2025, 3, 15, 20, 0), monthlyBlackOut);
        assertThat(monthlyBlackOut.isBlackOutActive(dtStart))
                .as("Monthly on boundary (start) for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtJustBeforeEnd = toZdt(LocalDateTime.of(2025, 3, 16, 1, 59), monthlyBlackOut);
        assertThat(monthlyBlackOut.isBlackOutActive(dtJustBeforeEnd))
                .as("Monthly on boundary (just before end) for tz=" + timezone)
                .isTrue();

        ZonedDateTime dtEnd = toZdt(LocalDateTime.of(2025, 3, 16, 2, 0), monthlyBlackOut);
        assertThat(monthlyBlackOut.isBlackOutActive(dtEnd))
                .as("Monthly on boundary (end) for tz=" + timezone)
                .isFalse();
    }
}

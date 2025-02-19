package org.drools.ansible.rulebook.integration.api.domain.temporal;

import java.time.LocalDateTime;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BlackOutUnitTest {

    // ====================
    // DAILY TESTS
    // ====================

    @Test
    public void testDailyIsBlackOut() {
        // Daily blackout: Every day between 2:30 PM and 4:15 PM (non-spanning)
        TimeSpec dailyStart = new TimeSpec(30, 14, null, null, null);
        TimeSpec dailyEnd = new TimeSpec(15, 16, null, null, null);
        BlackOut dailyBlackOut = new BlackOut(dailyStart, dailyEnd);

        LocalDateTime dtInside = LocalDateTime.of(2025, 2, 14, 15, 0);
        assertThat(dailyBlackOut.isBlackOutActive(dtInside)).isTrue();

        LocalDateTime dtBefore = LocalDateTime.of(2025, 2, 14, 14, 0);
        assertThat(dailyBlackOut.isBlackOutActive(dtBefore)).isFalse();

        LocalDateTime dtAfter = LocalDateTime.of(2025, 2, 14, 16, 30);
        assertThat(dailyBlackOut.isBlackOutActive(dtAfter)).isFalse();
    }

    @Test
    public void testDailyNextEndTime() {
        TimeSpec dailyStart = new TimeSpec(30, 14, null, null, null);
        TimeSpec dailyEnd = new TimeSpec(15, 16, null, null, null);
        BlackOut dailyBlackOut = new BlackOut(dailyStart, dailyEnd);

        LocalDateTime current = LocalDateTime.of(2025, 2, 14, 15, 30);
        LocalDateTime expected = LocalDateTime.of(2025, 2, 14, 16, 15);
        assertThat(dailyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);

        current = LocalDateTime.of(2025, 2, 14, 16, 30);
        expected = LocalDateTime.of(2025, 2, 15, 16, 15);
        assertThat(dailyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);
    }

    // Daily Spanning Boundary Tests (e.g. blackout from 22:00 to 02:00)
    @Test
    public void testDailySpanningBoundaryIsBlackOut() {
        TimeSpec dailyStart = new TimeSpec(0, 22, null, null, null);
        TimeSpec dailyEnd = new TimeSpec(0, 2, null, null, null);
        BlackOut dailyBlackOut = new BlackOut(dailyStart, dailyEnd);

        // Exactly at start boundary: 22:00 is active.
        LocalDateTime dtStartBoundary = LocalDateTime.of(2025, 6, 10, 22, 0);
        assertThat(dailyBlackOut.isBlackOutActive(dtStartBoundary)).isTrue();

        // In the middle of the period (before midnight)
        LocalDateTime dtBeforeMidnight = LocalDateTime.of(2025, 6, 10, 23, 30);
        assertThat(dailyBlackOut.isBlackOutActive(dtBeforeMidnight)).isTrue();

        // After midnight, still within blackout (e.g., 01:30)
        LocalDateTime dtAfterMidnight = LocalDateTime.of(2025, 6, 11, 1, 30);
        assertThat(dailyBlackOut.isBlackOutActive(dtAfterMidnight)).isTrue();

        // Exactly at end boundary: 02:00 should be inactive.
        LocalDateTime dtEndBoundary = LocalDateTime.of(2025, 6, 11, 2, 0);
        assertThat(dailyBlackOut.isBlackOutActive(dtEndBoundary)).isFalse();
    }

    // ====================
    // WEEKLY TESTS
    // ====================

    @Test
    public void testWeeklyIsBlackOut() {
        // Weekly blackout: Every Thursday between 3:00 AM and 4:00 AM.
        // Assume dayOfWeek=4 represents Thursday.
        TimeSpec weeklyStart = new TimeSpec(null, 3, 4, null, null);
        TimeSpec weeklyEnd = new TimeSpec(null, 4, 4, null, null);
        BlackOut weeklyBlackOut = new BlackOut(weeklyStart, weeklyEnd);

        LocalDateTime dtInside = LocalDateTime.of(2025, 2, 13, 3, 30);
        assertThat(weeklyBlackOut.isBlackOutActive(dtInside)).isTrue();

        LocalDateTime dtBefore = LocalDateTime.of(2025, 2, 13, 2, 30);
        assertThat(weeklyBlackOut.isBlackOutActive(dtBefore)).isFalse();

        LocalDateTime dtAfter = LocalDateTime.of(2025, 2, 13, 4, 30);
        assertThat(weeklyBlackOut.isBlackOutActive(dtAfter)).isFalse();
    }

    @Test
    public void testWeeklyNextEndTime() {
        TimeSpec weeklyStart = new TimeSpec(null, 3, 4, null, null);
        TimeSpec weeklyEnd = new TimeSpec(null, 4, 4, null, null);
        BlackOut weeklyBlackOut = new BlackOut(weeklyStart, weeklyEnd);

        LocalDateTime current = LocalDateTime.of(2025, 2, 13, 3, 30);
        LocalDateTime expected = LocalDateTime.of(2025, 2, 13, 4, 0);
        assertThat(weeklyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);

        current = LocalDateTime.of(2025, 2, 13, 4, 30);
        expected = LocalDateTime.of(2025, 2, 20, 4, 0);
        assertThat(weeklyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);
    }

    // Weekly Spanning Boundary Tests (spanning midnight with same dayOfWeek)
    @Test
    public void testWeeklySpanningBoundaryIsBlackOut() {
        // Define a weekly blackout for Thursday that spans midnight:
        // Start at 23:00 on Thursday and end at 02:00 (interpreted as Friday 02:00).
        TimeSpec weeklyStart = new TimeSpec(null, 23, 4, null, null); // 4 = Thursday
        TimeSpec weeklyEnd   = new TimeSpec(null, 2, 4, null, null);
        BlackOut weeklyBlackOut = new BlackOut(weeklyStart, weeklyEnd);

        // Thursday at 23:00 should be active.
        LocalDateTime dtStart = LocalDateTime.of(2025, 6, 12, 23, 0);
        assertThat(weeklyBlackOut.isBlackOutActive(dtStart)).isTrue();

        // Later on Thursday night
        LocalDateTime dtDuring = LocalDateTime.of(2025, 6, 12, 23, 30);
        assertThat(weeklyBlackOut.isBlackOutActive(dtDuring)).isTrue();

        // Early Friday morning (still within blackout period)
        LocalDateTime dtAfterMidnight = LocalDateTime.of(2025, 6, 13, 1, 30);
        assertThat(weeklyBlackOut.isBlackOutActive(dtAfterMidnight)).isTrue();

        // Exactly at 02:00 on Friday: blackout ends.
        LocalDateTime dtEnd = LocalDateTime.of(2025, 6, 13, 2, 0);
        assertThat(weeklyBlackOut.isBlackOutActive(dtEnd)).isFalse();
    }

    // ====================
    // ANNUAL TESTS
    // ====================

    @Test
    public void testAnnualIsBlackOut() {
        // Annual blackout: from December 23 (00:00) to January 2 (00:00)
        TimeSpec annualStart = new TimeSpec(null, 0, null, 23, 12);
        TimeSpec annualEnd = new TimeSpec(null, 0, null, 2, 1);
        BlackOut annualBlackOut = new BlackOut(annualStart, annualEnd);

        LocalDateTime dtInside = LocalDateTime.of(2025, 12, 25, 10, 0);
        assertThat(annualBlackOut.isBlackOutActive(dtInside)).isTrue();

        LocalDateTime dtBefore = LocalDateTime.of(2025, 12, 22, 10, 0);
        assertThat(annualBlackOut.isBlackOutActive(dtBefore)).isFalse();

        LocalDateTime dtAfter = LocalDateTime.of(2026, 1, 3, 10, 0);
        assertThat(annualBlackOut.isBlackOutActive(dtAfter)).isFalse();
    }

    @Test
    public void testAnnualNextEndTime() {
        TimeSpec annualStart = new TimeSpec(null, 0, null, 23, 12);
        TimeSpec annualEnd = new TimeSpec(null, 0, null, 2, 1);
        BlackOut annualBlackOut = new BlackOut(annualStart, annualEnd);

        LocalDateTime current = LocalDateTime.of(2025, 12, 25, 10, 0);
        LocalDateTime expected = LocalDateTime.of(2026, 1, 2, 0, 0);
        assertThat(annualBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);

        current = LocalDateTime.of(2025, 12, 22, 10, 0);
        expected = LocalDateTime.of(2026, 1, 2, 0, 0);
        assertThat(annualBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);

        current = LocalDateTime.of(2026, 1, 3, 10, 0);
        expected = LocalDateTime.of(2027, 1, 2, 0, 0);
        assertThat(annualBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);
    }

    // Annual On-Boundary Tests
    @Test
    public void testAnnualOnBoundary() {
        TimeSpec annualStart = new TimeSpec(null, 0, null, 23, 12);
        TimeSpec annualEnd = new TimeSpec(null, 0, null, 2, 1);
        BlackOut annualBlackOut = new BlackOut(annualStart, annualEnd);

        // On the start boundary: December 23, 2025 at 00:00 is active.
        LocalDateTime dtStart = LocalDateTime.of(2025, 12, 23, 0, 0);
        assertThat(annualBlackOut.isBlackOutActive(dtStart)).isTrue();

        // Just before the end boundary: January 1, 2026 at 23:59 is active.
        LocalDateTime dtJustBeforeEnd = LocalDateTime.of(2026, 1, 1, 23, 59);
        assertThat(annualBlackOut.isBlackOutActive(dtJustBeforeEnd)).isTrue();

        // Exactly at the end boundary: January 2, 2026 at 00:00 is inactive.
        LocalDateTime dtEnd = LocalDateTime.of(2026, 1, 2, 0, 0);
        assertThat(annualBlackOut.isBlackOutActive(dtEnd)).isFalse();
    }

    // ====================
    // MONTHLY TESTS
    // ====================

    @Test
    public void testMonthlyIsBlackOut() {
        // Monthly blackout: Every month from the 15th at 20:00 to the 16th at 02:00.
        TimeSpec monthlyStart = new TimeSpec(0, 20, null, 15, null);
        TimeSpec monthlyEnd = new TimeSpec(0, 2, null, 16, null);
        BlackOut monthlyBlackOut = new BlackOut(monthlyStart, monthlyEnd);

        LocalDateTime dtInside = LocalDateTime.of(2025, 3, 15, 21, 0);
        assertThat(monthlyBlackOut.isBlackOutActive(dtInside))
                .as("March 15, 21:00 should be in blackout")
                .isTrue();

        LocalDateTime dtAtEnd = LocalDateTime.of(2025, 3, 16, 2, 0);
        assertThat(monthlyBlackOut.isBlackOutActive(dtAtEnd))
                .as("March 16, 02:00 should not be in blackout")
                .isFalse();

        LocalDateTime dtJustBeforeEnd = LocalDateTime.of(2025, 3, 16, 1, 59);
        assertThat(monthlyBlackOut.isBlackOutActive(dtJustBeforeEnd))
                .as("March 16, 01:59 should be in blackout")
                .isTrue();

        LocalDateTime dtBefore = LocalDateTime.of(2025, 3, 15, 19, 59);
        assertThat(monthlyBlackOut.isBlackOutActive(dtBefore))
                .as("March 15, 19:59 should not be in blackout")
                .isFalse();
    }

    @Test
    public void testMonthlyNextEndTime() {
        TimeSpec monthlyStart = new TimeSpec(0, 20, null, 15, null);
        TimeSpec monthlyEnd = new TimeSpec(0, 2, null, 16, null);
        BlackOut monthlyBlackOut = new BlackOut(monthlyStart, monthlyEnd);

        LocalDateTime current = LocalDateTime.of(2025, 3, 15, 21, 0);
        LocalDateTime expected = LocalDateTime.of(2025, 3, 16, 2, 0);
        assertThat(monthlyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);

        current = LocalDateTime.of(2025, 3, 16, 2, 1);
        expected = LocalDateTime.of(2025, 4, 16, 2, 0);
        assertThat(monthlyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);

        current = LocalDateTime.of(2025, 3, 15, 19, 0);
        expected = LocalDateTime.of(2025, 3, 16, 2, 0);
        assertThat(monthlyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);
    }

    // Monthly Spanning Boundary Tests (e.g. blackout from the 28th at 22:00 to the 2nd at 06:00)
    @Test
    public void testMonthlySpanningBoundaryIsBlackOut() {
        TimeSpec monthlyStart = new TimeSpec(0, 22, null, 28, null);
        TimeSpec monthlyEnd = new TimeSpec(0, 6, null, 2, null);
        BlackOut monthlyBlackOut = new BlackOut(monthlyStart, monthlyEnd);

        // Test a time in the current month: May 29, 2025 at 23:00 should be active.
        LocalDateTime dtInside = LocalDateTime.of(2025, 5, 29, 23, 0);
        assertThat(monthlyBlackOut.isBlackOutActive(dtInside)).isTrue();

        // Test a time in the new month: June 2, 2025 at 05:30 should be active.
        dtInside = LocalDateTime.of(2025, 6, 2, 5, 30);
        assertThat(monthlyBlackOut.isBlackOutActive(dtInside)).isTrue();

        // Exactly at end boundary: June 2, 2025 at 06:00 should be inactive.
        LocalDateTime dtAtEnd = LocalDateTime.of(2025, 6, 2, 6, 0);
        assertThat(monthlyBlackOut.isBlackOutActive(dtAtEnd)).isFalse();
    }

    @Test
    public void testMonthlySpanningBoundaryNextEndTime() {
        TimeSpec monthlyStart = new TimeSpec(0, 22, null, 28, null);
        TimeSpec monthlyEnd = new TimeSpec(0, 6, null, 2, null);
        BlackOut monthlyBlackOut = new BlackOut(monthlyStart, monthlyEnd);

        LocalDateTime current = LocalDateTime.of(2025, 5, 29, 23, 0);
        LocalDateTime expected = LocalDateTime.of(2025, 6, 2, 6, 0);
        assertThat(monthlyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);

        current = LocalDateTime.of(2025, 6, 2, 6, 1);
        expected = LocalDateTime.of(2025, 7, 2, 6, 0);
        assertThat(monthlyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);
    }

    // Monthly On-Boundary Tests
    @Test
    public void testMonthlyOnBoundary() {
        // For a monthly blackout from the 15th at 20:00 to the 16th at 02:00:
        TimeSpec monthlyStart = new TimeSpec(0, 20, null, 15, null);
        TimeSpec monthlyEnd = new TimeSpec(0, 2, null, 16, null);
        BlackOut monthlyBlackOut = new BlackOut(monthlyStart, monthlyEnd);

        // On the start boundary (March 15, 2025 at 20:00) should be active.
        LocalDateTime dtStart = LocalDateTime.of(2025, 3, 15, 20, 0);
        assertThat(monthlyBlackOut.isBlackOutActive(dtStart)).isTrue();

        // Just before the end boundary (March 16, 2025 at 01:59) should be active.
        LocalDateTime dtJustBeforeEnd = LocalDateTime.of(2025, 3, 16, 1, 59);
        assertThat(monthlyBlackOut.isBlackOutActive(dtJustBeforeEnd)).isTrue();

        // Exactly at the end boundary (March 16, 2025 at 02:00) should be inactive.
        LocalDateTime dtEnd = LocalDateTime.of(2025, 3, 16, 2, 0);
        assertThat(monthlyBlackOut.isBlackOutActive(dtEnd)).isFalse();
    }

    @Test
    public void invalidDateTime() {

    }
}

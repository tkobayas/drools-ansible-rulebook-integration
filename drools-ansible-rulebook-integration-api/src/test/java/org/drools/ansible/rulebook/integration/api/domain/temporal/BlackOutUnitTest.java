package org.drools.ansible.rulebook.integration.api.domain.temporal;

import java.time.LocalDateTime;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BlackOutUnitTest {

    @Test
    public void testDailyIsBlackOutActive() {
        // Daily blackout: Every day between 2:30 PM and 4:15 PM
        TimeSpec dailyStart = new TimeSpec(30, 14, null, null, null);
        TimeSpec dailyEnd = new TimeSpec(15, 16, null, null, null);
        BlackOut dailyBlackOut = new BlackOut(dailyStart, dailyEnd);

        // Test a time that is inside the blackout period: 3:00 PM
        LocalDateTime dtInside = LocalDateTime.of(2025, 2, 14, 15, 0);
        assertThat(dailyBlackOut.isBlackOutActive(dtInside)).isTrue();

        // Test a time that is before the blackout period: 2:00 PM
        LocalDateTime dtBefore = LocalDateTime.of(2025, 2, 14, 14, 0);
        assertThat(dailyBlackOut.isBlackOutActive(dtBefore)).isFalse();

        // Test a time that is after the blackout period: 4:30 PM
        LocalDateTime dtAfter = LocalDateTime.of(2025, 2, 14, 16, 30);
        assertThat(dailyBlackOut.isBlackOutActive(dtAfter)).isFalse();
    }

    @Test
    public void testDailyNextEndTime() {
        // Daily blackout: Every day between 2:30 PM and 4:15 PM
        TimeSpec dailyStart = new TimeSpec(30, 14, null, null, null);
        TimeSpec dailyEnd = new TimeSpec(15, 16, null, null, null);
        BlackOut dailyBlackOut = new BlackOut(dailyStart, dailyEnd);

        // If current time is before the end time on the same day, expect the blackout to end today.
        LocalDateTime current = LocalDateTime.of(2025, 2, 14, 15, 30);
        LocalDateTime expected = LocalDateTime.of(2025, 2, 14, 16, 15);
        assertThat(dailyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);

        // If current time is after the blackout period, expect the next end time to be tomorrow.
        current = LocalDateTime.of(2025, 2, 14, 16, 30);
        expected = LocalDateTime.of(2025, 2, 15, 16, 15);
        assertThat(dailyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);
    }

    @Test
    public void testWeeklyIsBlackOutActive() {
        // Weekly blackout: Every Thursday between 3:00 AM and 4:00 AM.
        // Assuming dayOfWeek=4 represents Thursday (with Monday=1, Tuesday=2, etc.)
        TimeSpec weeklyStart = new TimeSpec(null, 3, 4, null, null);
        TimeSpec weeklyEnd = new TimeSpec(null, 4, 4, null, null);
        BlackOut weeklyBlackOut = new BlackOut(weeklyStart, weeklyEnd);

        // Feb 13, 2025 is a Thursday. Test a time within the blackout period: 3:30 AM.
        LocalDateTime dtInside = LocalDateTime.of(2025, 2, 13, 3, 30);
        assertThat(weeklyBlackOut.isBlackOutActive(dtInside)).isTrue();

        // Test a time before the blackout: 2:30 AM.
        LocalDateTime dtBefore = LocalDateTime.of(2025, 2, 13, 2, 30);
        assertThat(weeklyBlackOut.isBlackOutActive(dtBefore)).isFalse();

        // Test a time after the blackout: 4:30 AM.
        LocalDateTime dtAfter = LocalDateTime.of(2025, 2, 13, 4, 30);
        assertThat(weeklyBlackOut.isBlackOutActive(dtAfter)).isFalse();
    }

    @Test
    public void testWeeklyNextEndTime() {
        // Weekly blackout: Every Thursday between 3:00 AM and 4:00 AM.
        TimeSpec weeklyStart = new TimeSpec(null, 3, 4, null, null);
        TimeSpec weeklyEnd = new TimeSpec(null, 4, 4, null, null);
        BlackOut weeklyBlackOut = new BlackOut(weeklyStart, weeklyEnd);

        // When current time is during the blackout period (Thursday 3:30 AM), expect end time today at 4:00 AM.
        LocalDateTime current = LocalDateTime.of(2025, 2, 13, 3, 30);
        LocalDateTime expected = LocalDateTime.of(2025, 2, 13, 4, 0);
        assertThat(weeklyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);

        // When current time is after the blackout period on Thursday, expect the end time next week.
        current = LocalDateTime.of(2025, 2, 13, 4, 30);
        expected = LocalDateTime.of(2025, 2, 20, 4, 0);
        assertThat(weeklyBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);
    }

    @Test
    public void testAnnualIsBlackOutActive() {
        // Annual blackout: from December 23 (midnight) to January 2 (midnight)
        TimeSpec annualStart = new TimeSpec(null, 0, null, 23, 12);
        TimeSpec annualEnd = new TimeSpec(null, 0, null, 2, 1);
        BlackOut annualBlackOut = new BlackOut(annualStart, annualEnd);

        // Test a date inside the blackout period: December 25, 2025, 10:00 AM.
        LocalDateTime dtInside = LocalDateTime.of(2025, 12, 25, 10, 0);
        assertThat(annualBlackOut.isBlackOutActive(dtInside)).isTrue();

        // Test a date before the blackout period: December 22, 2025.
        LocalDateTime dtBefore = LocalDateTime.of(2025, 12, 22, 10, 0);
        assertThat(annualBlackOut.isBlackOutActive(dtBefore)).isFalse();

        // Test a date after the blackout period: January 3, 2026.
        LocalDateTime dtAfter = LocalDateTime.of(2026, 1, 3, 10, 0);
        assertThat(annualBlackOut.isBlackOutActive(dtAfter)).isFalse();
    }

    @Test
    public void testAnnualNextEndTime() {
        // Annual blackout: from December 23 (midnight) to January 2 (midnight)
        TimeSpec annualStart = new TimeSpec(null, 0, null, 23, 12);
        TimeSpec annualEnd = new TimeSpec(null, 0, null, 2, 1);
        BlackOut annualBlackOut = new BlackOut(annualStart, annualEnd);

        // When current time is during the blackout period (Dec 25, 2025),
        // the next end time should be January 2, 2026 at 00:00.
        LocalDateTime current = LocalDateTime.of(2025, 12, 25, 10, 0);
        LocalDateTime expected = LocalDateTime.of(2026, 1, 2, 0, 0);
        assertThat(annualBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);

        // When current time is before the blackout period (Dec 22, 2025),
        // expect the upcoming end time to be January 2, 2026 at 00:00.
        current = LocalDateTime.of(2025, 12, 22, 10, 0);
        expected = LocalDateTime.of(2026, 1, 2, 0, 0);
        assertThat(annualBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);

        // When current time is after the blackout period (Jan 3, 2026),
        // the next blackout end time should be January 2, 2027 at 00:00.
        current = LocalDateTime.of(2026, 1, 3, 10, 0);
        expected = LocalDateTime.of(2027, 1, 2, 0, 0);
        assertThat(annualBlackOut.getBlackOutNextEndTime(current)).isEqualTo(expected);
    }

    @Test
    public void invalidDateTime() {

    }
}

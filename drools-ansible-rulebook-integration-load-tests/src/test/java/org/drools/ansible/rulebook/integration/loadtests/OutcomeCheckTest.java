package org.drools.ansible.rulebook.integration.loadtests;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutcomeCheckTest {

    @Test
    void matchExpected_withMatches_passes() {
        List<Map> matches = List.of(Map.of("rule", "r1"));
        assertThatCode(() -> OutcomeCheck.verify(matches, ExpectedOutcome.MATCH, "24kb_1k_events.json"))
                .doesNotThrowAnyException();
    }

    @Test
    void matchExpected_withZeroMatches_throws() {
        List<Map> matches = List.of();
        assertThatThrownBy(() -> OutcomeCheck.verify(matches, ExpectedOutcome.MATCH, "24kb_1k_events.json"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Expected at least one match but got 0")
                .hasMessageContaining("24kb_1k_events.json");
    }

    @Test
    void noMatchExpected_withZeroMatches_passes() {
        List<Map> matches = List.of();
        assertThatCode(() -> OutcomeCheck.verify(matches, ExpectedOutcome.NO_MATCH, "retention_100_events.json"))
                .doesNotThrowAnyException();
    }

    @Test
    void noMatchExpected_withSomeMatches_throws() {
        Map<String, String> firstMatch = Map.of("rule", "r-unexpected");
        List<Map> matches = List.of(firstMatch, Map.of("rule", "r2"));
        assertThatThrownBy(() -> OutcomeCheck.verify(matches, ExpectedOutcome.NO_MATCH, "24kb_1k_events_unmatch.json"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Expected no matches but got 2")
                .hasMessageContaining("24kb_1k_events_unmatch.json")
                .hasMessageContaining("r-unexpected");
    }
}

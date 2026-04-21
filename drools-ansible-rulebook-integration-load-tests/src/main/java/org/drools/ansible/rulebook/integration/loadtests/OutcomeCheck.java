package org.drools.ansible.rulebook.integration.loadtests;

import java.util.List;
import java.util.Map;

public final class OutcomeCheck {

    private OutcomeCheck() {}

    public static void verify(List<Map> matches, ExpectedOutcome expected, String eventsJson) {
        int count = matches.size();
        if (expected == ExpectedOutcome.MATCH && count == 0) {
            throw new RuntimeException(
                    "Expected at least one match but got 0 (events: " + eventsJson + ")");
        }
        if (expected == ExpectedOutcome.NO_MATCH && count > 0) {
            throw new RuntimeException(
                    "Expected no matches but got " + count
                            + " (events: " + eventsJson
                            + ", first match: " + matches.get(0) + ")");
        }
    }
}

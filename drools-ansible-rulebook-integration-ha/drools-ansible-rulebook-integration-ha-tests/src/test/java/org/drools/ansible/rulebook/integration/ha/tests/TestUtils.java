package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class TestUtils {

    private TestUtils() {
    }

    public static MatchingEvent createMatchingEvent(String sessionId, String rulesetName,
                                              String ruleName, Map<String, Object> matchingFacts) {
        MatchingEvent me = new MatchingEvent();
        me.setSessionId(sessionId);
        me.setRuleSetName(rulesetName);
        me.setRuleName(ruleName);

        // Serialize matching facts to JSON
        String eventDataJson = toJson(matchingFacts);
        me.setEventData(eventDataJson);
        return me;
    }
}

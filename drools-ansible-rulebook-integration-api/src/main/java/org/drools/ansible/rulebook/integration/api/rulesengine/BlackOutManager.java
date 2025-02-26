package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.domain.temporal.BlackOut;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage blackout state in session
 */
public class BlackOutManager {

    protected static final Logger LOG = LoggerFactory.getLogger(BlackOutManager.class);

    private final Map<String, BlackOut> blackOuts = new HashMap<>();
    private final Map<String, ZonedDateTime> blackOutEndTimes = new HashMap<>();
    private final Map<String, Deque<Match>> blackOutMatches = new HashMap<>();

    public BlackOutManager(RulesSet rulesSet) {
        enlistBlackOuts(rulesSet);
    }

    private void enlistBlackOuts(RulesSet rulesSet) {
        rulesSet.getRules().forEach(rule -> {
            BlackOut blackOut = rule.getRule().getBlackOut();
            if (blackOut != null) {
                blackOut.validate();
                blackOuts.put(rule.getRule().getName(), blackOut);
            }
        });
    }

    public boolean isBlackOutActive(String ruleName, ZonedDateTime currentDateTime) {
        if (!blackOuts.containsKey(ruleName)) {
            return false;
        }
        BlackOut blackOut = blackOuts.get(ruleName);
        return blackOut.isBlackOutActive(currentDateTime);
    }

    public synchronized void queueBlackOutMatch(String ruleName, Match match, ZonedDateTime currentDateTime) {
        BlackOut blackOut = blackOuts.get(ruleName);
        ZonedDateTime blackOutNextEndTime = blackOut.getBlackOutNextEndTime(currentDateTime);
        blackOutEndTimes.put(ruleName, blackOutNextEndTime);
        blackOutMatches.computeIfAbsent(ruleName, k -> new ArrayDeque<>()).add(match);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Queueing a match for rule {} until black out ends {}", ruleName, blackOutNextEndTime);
        }
    }

    public synchronized List<Match> getMatchesAfterBlackOut(ZonedDateTime currentDateTime) {
        System.out.println("getMatchesAfterBlackOut:");
        System.out.println("  blackOuts = " + blackOuts);
        System.out.println("  blackOutMatches = " + blackOutMatches);
        System.out.println("  blackOutEndTimes = " + blackOutEndTimes);
        List<Match> matches = new ArrayList<>();
        List<String> processedRules = new ArrayList<>();
        for (Map.Entry<String, ZonedDateTime> entry : blackOutEndTimes.entrySet()) {
            String ruleName = entry.getKey();
            ZonedDateTime blackOutEndTime = entry.getValue();
            if (blackOutEndTime.isBefore(currentDateTime)) {
                // Not checking isBlackOutActive(). Even if now is during the next blackout, the queued matches of the previous blackout should be retrieved
                Deque<Match> blackOutMatchesForRule = blackOutMatches.get(ruleName);
                if (blackOutMatchesForRule != null && !blackOutMatchesForRule.isEmpty()) {
                    BlackOut blackOut = blackOuts.get(ruleName);
                    switch (blackOut.getTrigger()) {
                        case ALL:
                            matches.addAll(blackOutMatchesForRule);
                            break;
                        case FIRST:
                            matches.add(blackOutMatchesForRule.pollFirst());
                            break;
                        case LAST:
                            matches.add(blackOutMatchesForRule.pollLast());
                            break;
                    }
                }
                blackOutMatches.remove(ruleName);
                processedRules.add(ruleName);
            }
        }
        processedRules.forEach(blackOutEndTimes::remove);
        return matches;
    }
}

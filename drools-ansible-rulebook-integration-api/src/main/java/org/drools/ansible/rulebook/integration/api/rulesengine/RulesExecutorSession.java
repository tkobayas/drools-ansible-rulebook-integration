package org.drools.ansible.rulebook.integration.api.rulesengine;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.domain.temporal.BlackOut;
import org.drools.core.common.InternalFactHandle;
import org.kie.api.prototype.PrototypeEventInstance;
import org.kie.api.prototype.PrototypeFactInstance;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.ObjectFilter;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;
import org.kie.api.time.SessionPseudoClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.mapToFact;


public class RulesExecutorSession {

    protected static final Logger log = LoggerFactory.getLogger(RulesExecutorSession.class);

    private final RulesSet rulesSet;

    private final KieSession kieSession;

    private final RulesExecutionController rulesExecutionController;

    private final long id;

    private final SessionStatsCollector sessionStatsCollector;

    private final RulesSetEventStructure rulesSetEventStructure;

    private final Map<String, BlackOut> blackOuts = new HashMap<>();
    private final Map<String, ZonedDateTime> blackOutEndTimes = new HashMap<>();
    private final Map<String, Deque<Match>> blackOutMatches = new HashMap<>();

    public RulesExecutorSession(RulesSet rulesSet, KieSession kieSession, RulesExecutionController rulesExecutionController, long id) {
        this.rulesSet = rulesSet;
        this.kieSession = kieSession;
        this.rulesExecutionController = rulesExecutionController;
        this.id = id;
        this.sessionStatsCollector = new SessionStatsCollector(id);
        this.rulesSetEventStructure = new RulesSetEventStructure(rulesSet);
        enlistBlackOuts();

        initClock();
    }

    private void enlistBlackOuts() {
        rulesSet.getRules().forEach(rule -> {
            BlackOut blackOut = rule.getRule().getBlackOut();
            if (blackOut != null) {
                blackOuts.put(rule.getRule().getName(), blackOut);
            }
        });
    }

    public boolean blackOutExists(String ruleName) {
        return blackOuts.containsKey(ruleName);
    }

    public boolean isBlackOutActive(String ruleName) {
        if (!blackOuts.containsKey(ruleName)) {
            return false;
        }
        BlackOut blackOut = blackOuts.get(ruleName);
        ZonedDateTime currentDateTime = getCurrentDateTime();
        return blackOut.isBlackOutActive(currentDateTime);
    }

    private ZonedDateTime getCurrentDateTime() {
        long epochMillis = getPseudoClock().getCurrentTime();
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
    }

    public void queueBlackOutMatch(String ruleName, Match match) {
        BlackOut blackOut = blackOuts.get(ruleName);
        ZonedDateTime currentDateTime = getCurrentDateTime();
        ZonedDateTime blackOutNextEndTime = blackOut.getBlackOutNextEndTime(currentDateTime);
        blackOutEndTimes.put(ruleName, blackOutNextEndTime);
        blackOutMatches.computeIfAbsent(ruleName, k -> new ArrayDeque<>()).add(match);
        if (log.isDebugEnabled()) {
            log.debug("Queueing a match for rule {} until black out ends {}", ruleName, blackOutNextEndTime);
        }
    }

    public List<Match> getMatchesAfterBlackOut() {
        List<Match> matches = new ArrayList<>();
        ZonedDateTime currentDateTime = getCurrentDateTime();
        List<String> processedRules = new ArrayList<>();
        for (Map.Entry<String, ZonedDateTime> entry : blackOutEndTimes.entrySet()) {
            String ruleName = entry.getKey();
            ZonedDateTime blackOutEndTime = entry.getValue();
            if (blackOutEndTime.isBefore(currentDateTime)) {
                // Not checking isBlackOutActive(). Even if now is during the blackout, the queued matches of the previous blackout should be retrieved
                Deque<Match> blackOutMatchesForRule = blackOutMatches.get(ruleName);
                if (blackOutMatchesForRule != null) {
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
                    blackOutMatches.remove(ruleName);
                }
                processedRules.add(ruleName);
            }
        }
        processedRules.forEach(blackOutEndTimes::remove);
        return matches;
    }

    private void initClock() {
        getPseudoClock().advanceTime(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    long getId() {
        return id;
    }

    String getRuleSetName() {
        return rulesSet.getName();
    }

    Collection<? extends Object> getObjects() {
        return kieSession.getObjects();
    }

    Collection<? extends Object> getObjects(ObjectFilter filter) {
        return kieSession.getObjects(filter);
    }

    InternalFactHandle insert(Map<String, Object> factMap, boolean event) {
        PrototypeFactInstance fact = mapToFact(factMap, event);
        if (event) {
            ((PrototypeEventInstance) fact).withExpiration(rulesSet.getEventsTtl().getAmount(), rulesSet.getEventsTtl().getTimeUnit());
        }
        InternalFactHandle fh = (InternalFactHandle) kieSession.insert(fact);
        if (event) {
            sessionStatsCollector.registerProcessedEvent(this, fh);
        }
        return fh;
    }

    void delete(FactHandle fh) {
        kieSession.delete(fh);
    }

    List<InternalFactHandle> deleteAllMatchingFacts(Map<String, Object> toBeRetracted, boolean allowPartialMatch, String... keysToExclude) {
        BiPredicate<Map<String, Object>, Map<String, Object>> factsComparator = allowPartialMatch ?
                (wmFact, retract) -> wmFact.entrySet().containsAll(retract.entrySet()) :
                (wmFact, retract) -> areFactsEqual(wmFact, retract, keysToExclude);

        Collection<FactHandle> fhs = kieSession.getFactHandles(o -> o instanceof PrototypeFactInstance && factsComparator.test( ((PrototypeFactInstance) o).asMap(), toBeRetracted ) );
        return new ArrayList<>( fhs ).stream().peek( kieSession::delete ).map( InternalFactHandle.class::cast ).collect(Collectors.toList());
    }

    private static boolean areFactsEqual(Map<String, Object> wmFact, Map<String, Object> retract, String... keysToExclude) {
        Set<String> checkedKeys = new HashSet<>();
        for (Map.Entry<String, Object> wmFactValue : wmFact.entrySet()) {
            String wmFactKey = wmFactValue.getKey();
            if (!isKeyToBeIgnored(wmFactKey, keysToExclude) && !wmFactValue.getValue().equals(retract.get(wmFactKey))) {
                return false;
            }
            checkedKeys.add(wmFactKey);
        }
        return checkedKeys.size() == wmFact.size();
    }

    private static boolean isKeyToBeIgnored(String wmFactKey, String... keysToExclude) {
        if (keysToExclude != null && keysToExclude.length > 0) {
            for (String keyToExclude : keysToExclude) {
                if (wmFactKey.equals(keyToExclude) || wmFactKey.startsWith(keyToExclude + ".")) {
                    return true;
                }
            }
        }
        return false;
    }

    int fireAllRules() {
        return kieSession.fireAllRules();
    }

    int fireAllRules(AgendaFilter agendaFilter) {
        return kieSession.fireAllRules(agendaFilter);
    }

    SessionStats dispose() {
        SessionStats stats = getSessionStats(true);
        kieSession.dispose();
        return stats;
    }

    SessionStats getSessionStats(boolean disposing) {
        return sessionStatsCollector.generateStats(this, disposing);
    }

    public void registerMatch(Match match) {
        sessionStatsCollector.registerMatch(this, match);
    }

    public void registerMatchedEvents(Collection<FactHandle> events) {
        sessionStatsCollector.registerMatchedEvents(events);
    }

    public void registerAsyncResponse(byte[] bytes) {
        sessionStatsCollector.registerAsyncResponse(bytes);
    }

    int rulesCount() {
        return rulesSet.getEnabledRulesNumber();
    }

    int disabledRulesCount() {
        return rulesSet.getDisabledRulesNumber();
    }

    void advanceTime( long amount, TimeUnit unit ) {
        SessionPseudoClock clock = getPseudoClock();
        clock.advanceTime(amount, unit);
        sessionStatsCollector.registerClockAdvance(amount, unit);
    }

    SessionPseudoClock getPseudoClock() {
        return kieSession.getSessionClock();
    }

    void setExecuteActions(boolean executeActions) {
        rulesExecutionController.setExecuteActions(executeActions);
    }

    public boolean isMatchMultipleRules() {
        return rulesSet.isMatchMultipleRules();
    }

    public KieSession asKieSession() {
        return kieSession;
    }

    public RulesSetEventStructure getRulesSetEventStructure() {
        return rulesSetEventStructure;
    }
}

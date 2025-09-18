package org.drools.ansible.rulebook.integration.ha.api;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.rulesengine.MemoryMonitorUtil;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesEvaluator;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutorSession;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.asFactMap;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.getEventUuid;

public class HARulesExecutor extends RulesExecutor {

    public HARulesExecutor(RulesExecutorSession rulesExecutorSession) {
        super(createRulesEvaluator(rulesExecutorSession));
    }

    private static RulesEvaluator createRulesEvaluator(RulesExecutorSession rulesExecutorSession) {
        return new HARulesEvaluator(rulesExecutorSession);
    }

    public void setOnRecovery(boolean onRecovery) {
        ((HARulesEvaluator) rulesEvaluator).setOnRecovery(onRecovery);
    }

    public RulesSet getRulesSet() {
        return ((HARulesEvaluator) rulesEvaluator).getRulesSet();
    }

    @Override
    public CompletableFuture<List<Match>> processEvents(String json) {
        MemoryMonitorUtil.checkMemoryOccupation(rulesEvaluator.getSessionStatsCollector());
        rulesEvaluator.stashFirstEventJsonForValidation(json);

        Map<String, Object> eventMap = asFactMap(json);
        getEventUuid(eventMap).ifPresent(eventUuid -> {
            EventRecord eventRecord = new EventRecord(eventUuid, json, asKieSession().getSessionClock().getCurrentTime());
            getHaSessionContext().addEventUuidInMemory(eventUuid, eventRecord);
        });

        return rulesEvaluator.processEvents(asFactMap(json));
    }

    public HASessionContext getHaSessionContext() {
        return ((HARulesEvaluator) rulesEvaluator).getHaSessionContext();
    }
}

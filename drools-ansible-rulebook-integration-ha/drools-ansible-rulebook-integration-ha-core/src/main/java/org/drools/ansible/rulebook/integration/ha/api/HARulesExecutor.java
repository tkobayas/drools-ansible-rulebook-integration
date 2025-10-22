package org.drools.ansible.rulebook.integration.ha.api;

import java.util.List;
import java.util.Map;
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
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256;

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
        String eventUuid = getEventUuid(eventMap).orElse(null); // TODO: clarify how to handle an event without uuid! (add sha to map?)
        getHaSessionContext().preparePendingRecord(eventUuid, json, EventRecord.RecordType.EVENT);

        return rulesEvaluator.processEvents(eventMap);
    }

    @Override
    public CompletableFuture<List<Match>> processFacts(String json) {
        MemoryMonitorUtil.checkMemoryOccupation(rulesEvaluator.getSessionStatsCollector());
        getHaSessionContext().preparePendingRecord(sha256(json), json, EventRecord.RecordType.FACT);
        return rulesEvaluator.processFacts(asFactMap(json));
    }

    public HASessionContext getHaSessionContext() {
        return ((HARulesEvaluator) rulesEvaluator).getHaSessionContext();
    }
}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.asFactMap;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.getEventUuid;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256;

public class HARulesExecutor extends RulesExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(HARulesExecutor.class);

    // In HA mode, ID should be consistent across session recoveries for python client.
    // The ID is essentially used to lookup rulesExecutor from container
    private long externalSessionId;

    // Retain ruleset string for clean KieBase generation during recovery
    private String rulesetString;

    public HARulesExecutor(RulesExecutorSession rulesExecutorSession, String rulesetString) {
        super(createRulesEvaluator(rulesExecutorSession));
        this.externalSessionId = rulesEvaluator.getSessionId(); // Initially set to internal session ID
        this.rulesetString = rulesetString;

        // Pass container lookup ID to evaluator so it can use it for SessionStats, async responses, etc.
        if (rulesEvaluator instanceof HARulesEvaluator) {
            ((HARulesEvaluator) rulesEvaluator).setExternalSessionId(this.externalSessionId);
        }
    }

    private static RulesEvaluator createRulesEvaluator(RulesExecutorSession rulesExecutorSession) {
        return new HARulesEvaluator(rulesExecutorSession);
    }

    public String getRulesetString() {
        return rulesetString;
    }

    @Override
    public long getId() {
        return externalSessionId;  // Returns container key, not internal ID
    }

    public void setExternalSessionId(long externalSessionId) {
        this.externalSessionId = externalSessionId;

        // Update evaluator's container lookup ID as well
        if (rulesEvaluator instanceof HARulesEvaluator) {
            ((HARulesEvaluator) rulesEvaluator).setExternalSessionId(externalSessionId);
        }
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
        String eventUuid = getEventUuid(eventMap)
                .orElseGet(() -> {
                    LOG.warn("Event UUID not found in event data, computing SHA-256 hash as fallback");
                    return sha256(json);
                });
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

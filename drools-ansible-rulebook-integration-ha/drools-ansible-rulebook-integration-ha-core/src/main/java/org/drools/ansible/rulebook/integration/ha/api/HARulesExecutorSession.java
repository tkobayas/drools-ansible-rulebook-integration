package org.drools.ansible.rulebook.integration.ha.api;

import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutionController;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutorSession;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.core.common.InternalFactHandle;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.getEventUuid;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256;

public class HARulesExecutorSession extends RulesExecutorSession {

    private final HASessionContext haSessionContext;

    public HARulesExecutorSession(RulesSet rulesSet, KieSession kieSession, RulesExecutionController rulesExecutionController, long andIncrement) {
        super(rulesSet, kieSession, rulesExecutionController, andIncrement);
        this.haSessionContext = new HASessionContext();
    }

    // TODO: Review if we can simplify this logic. We may assume more constrained input (e.g. json and identifier must not be null in PendingRecord)

    @Override
    protected InternalFactHandle insert(Map<String, Object> factMap, boolean event) {
        HASessionContext.PendingRecord pending = haSessionContext.consumePendingRecord();
        InternalFactHandle factHandle = super.insert(factMap, event);

        if (pending != null) {
            String identifier = pending.getIdentifier();
            String json = pending.getJson();

            if (event) {
                identifier = getEventUuid(factHandle).orElse(identifier);
            }
            if (json == null) {
                json = JsonMapper.toJson(factMap);
            }
            if (identifier == null) {
                identifier = sha256(json);
            }

            long timestamp = asKieSession().getSessionClock().getCurrentTime();
            EventRecord record = new EventRecord(identifier, json, timestamp, pending.getType());
            haSessionContext.addRecord(identifier, record, factHandle.getId());
        }

        return factHandle;
    }

    @Override
    protected void delete(FactHandle fh) {
        super.delete(fh);
        getEventUuid(fh).ifPresentOrElse(haSessionContext::removeEventUuidInMemory,
                () -> haSessionContext.removeRecordByFactHandle(((InternalFactHandle) fh).getId()));
    }

    public RulesSet getRulesSet() {
        return rulesSet;
    }

    public HASessionContext getHaSessionContext() {
        return haSessionContext;
    }
}

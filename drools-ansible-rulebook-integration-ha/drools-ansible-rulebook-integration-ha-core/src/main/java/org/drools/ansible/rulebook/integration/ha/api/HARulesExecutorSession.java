package org.drools.ansible.rulebook.integration.ha.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutionController;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutorSession;
import org.drools.core.common.InternalFactHandle;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.getEventUuid;

public class HARulesExecutorSession extends RulesExecutorSession {

    private final HASessionContext haSessionContext;

    public HARulesExecutorSession(RulesSet rulesSet, KieSession kieSession, RulesExecutionController rulesExecutionController, long andIncrement) {
        super(rulesSet, kieSession, rulesExecutionController, andIncrement);
        this.haSessionContext = new HASessionContext();
    }

    @Override
    protected void delete(FactHandle fh) {
        super.delete(fh);
        getEventUuid(fh).ifPresent(haSessionContext::removeEventUuidInMemory);
    }

    public RulesSet getRulesSet() {
        return rulesSet;
    }

    public HASessionContext getHaSessionContext() {
        return haSessionContext;
    }
}

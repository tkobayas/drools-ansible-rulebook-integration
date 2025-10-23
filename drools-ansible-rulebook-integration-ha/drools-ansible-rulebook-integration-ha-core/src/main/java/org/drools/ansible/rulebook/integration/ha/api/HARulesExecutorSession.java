package org.drools.ansible.rulebook.integration.ha.api;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutionController;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutorSession;
import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.kie.api.runtime.KieSession;

public class HARulesExecutorSession extends RulesExecutorSession {

    // External session ID for HA mode - may differ from internal id after recovery
    private Long externalSessionId = null;

    private final HASessionContext haSessionContext;
    private final HARuleRuntimeEventListener eventListener;

    public HARulesExecutorSession(RulesSet rulesSet, KieSession kieSession, RulesExecutionController rulesExecutionController, long andIncrement) {
        super(rulesSet, kieSession, rulesExecutionController, andIncrement);
        this.haSessionContext = new HASessionContext();
        this.eventListener = new HARuleRuntimeEventListener(haSessionContext, kieSession);

        // Register listener to track ALL insertions/deletions (including control events)
        kieSession.addEventListener(eventListener);
    }

    public RulesSet getRulesSet() {
        return rulesSet;
    }

    public HASessionContext getHaSessionContext() {
        return haSessionContext;
    }

    /**
     * Sets external session ID for HA mode.
     * This ID is what external clients (Python) use and should remain consistent across recovery.
     */
    public void setExternalSessionId(Long externalSessionId) {
        this.externalSessionId = externalSessionId;
    }

    @Override
    public long getId() {
        // Return external session ID if set, otherwise internal ID
        return externalSessionId != null ? externalSessionId : super.getId();
    }
}

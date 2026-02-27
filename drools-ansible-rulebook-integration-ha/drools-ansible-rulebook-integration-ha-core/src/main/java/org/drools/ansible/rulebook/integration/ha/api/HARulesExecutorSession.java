package org.drools.ansible.rulebook.integration.ha.api;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutionController;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutorSession;
import org.drools.core.common.DefaultEventHandle;
import org.drools.core.common.ReteEvaluator;
import org.drools.core.impl.WorkingMemoryReteExpireAction;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HARulesExecutorSession extends RulesExecutorSession {

    private static final Logger LOG = LoggerFactory.getLogger(HARulesExecutorSession.class);

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

        // Register working memory action listener to detect event expirations.
        // Drools does NOT fire objectDeleted for TTL-expired events, so we use this hook
        // (the same mechanism used by drools-reliability) to clean up trackedRecords.
        ((ReteEvaluator) kieSession).setWorkingMemoryActionListener(entry -> {
            if (entry instanceof WorkingMemoryReteExpireAction) {
                DefaultEventHandle factHandle = ((WorkingMemoryReteExpireAction) entry).getFactHandle();
                haSessionContext.removeTrackedRecordByFactHandle(factHandle.getId());
                LOG.debug("Removed expired event from trackedRecords: factHandleId={}", factHandle.getId());
            }
        });
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

package org.drools.ansible.rulebook.integration.ha.api;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutorSession;
import org.drools.ansible.rulebook.integration.api.rulesengine.SyncRulesEvaluator;
import org.drools.core.common.InternalFactHandle;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.getEventUuid;

/**
 * Extends SyncRulesEvaluator.
 * AsyncRulesEvaluator is only used by AsyncAstRulesEngine (which sets RuleConfigurationOption.ASYNC_EVALUATION), which uses async channel even for synchronous evaluation (processEvent).
 * But AsyncAstRulesEngine is not used by drools_jpy as of 1.0.11.
 *
 * So the HA scenario is HARulesEvaluator + async channel
 */
public class HARulesEvaluator extends SyncRulesEvaluator {

    private volatile boolean onRecovery = false;

    public HARulesEvaluator(RulesExecutorSession rulesExecutorSession) {
        super(rulesExecutorSession);
    }

    @Override
    protected List<Match> writeResponseOnChannel(List<Match> matches) {
        // Do not send responses if we are recovering the session
        if (!onRecovery && !matches.isEmpty()) { // skip empty result
            byte[] bytes = channel.write(new Response(getSessionId(), RuleMatch.asList(matches)));
            rulesExecutorSession.registerAsyncResponse(bytes);
        }
        return matches;
    }

    public void setOnRecovery(boolean onRecovery) {
        this.onRecovery = onRecovery;
    }

    public RulesSet getRulesSet() {
        return ((HARulesExecutorSession) rulesExecutorSession).getRulesSet();
    }

    public HASessionContext getHaSessionContext() {
        return ((HARulesExecutorSession) rulesExecutorSession).getHaSessionContext();
    }

    @Override
    protected void processDiscardedFact(InternalFactHandle fh) {
        getEventUuid(fh).ifPresentOrElse(
                uuid -> getHaSessionContext().removeEventUuidInMemory(uuid),
                () -> getHaSessionContext().removeRecordByFactHandle(fh.getId()));
    }
}

package org.drools.ansible.rulebook.integration.ha.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord.RecordType;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.ansible.rulebook.integration.ha.model.SessionStateLite;
import org.drools.core.time.impl.PseudoClockScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHAStateManager implements HAStateManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHAStateManager.class);

    private final Map<String, SessionStateLite> sessionStateLiteMap = new HashMap<>();

    @Override
    public RulesExecutor recoverSession(RulesSet rulesSet, SessionState sessionState) {
        return HARulesExecutorFactory.createRulesExecutorWithRecovery(rulesSet, rulesExecutor -> {
            // Replay events to bring session up-to-date
            ((PseudoClockScheduler) rulesExecutor.asKieSession().getSessionClock()).setStartupTime(sessionState.getCreatedTime());
            long currentTime = sessionState.getCreatedTime();
            List<EventRecord> partialEvents = sessionState.getPartialEvents();
            for (EventRecord eventRecord : partialEvents) {
                rulesExecutor.advanceTime(eventRecord.getInsertedAt() - currentTime, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (eventRecord.getRecordType() == RecordType.FACT) {
                    rulesExecutor.processFacts(eventRecord.getEventJson());
                } else {
                    rulesExecutor.processEvents(eventRecord.getEventJson());
                }
                currentTime = eventRecord.getInsertedAt();
            }
            rulesExecutor.advanceTime(sessionState.getPersistedTime() - currentTime, java.util.concurrent.TimeUnit.MILLISECONDS);
        });
    }

    @Override
    public void registerSessionStateLite(String ruleSetName, SessionStateLite sessionStateLite) {
        sessionStateLiteMap.put(ruleSetName, sessionStateLite);
    }

    @Override
    public SessionStateLite getSessionStateLite(String ruleSetName) {
        return sessionStateLiteMap.get(ruleSetName);
    }

}

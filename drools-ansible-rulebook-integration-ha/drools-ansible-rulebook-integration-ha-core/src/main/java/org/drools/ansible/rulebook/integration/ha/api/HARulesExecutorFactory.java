package org.drools.ansible.rulebook.integration.ha.api;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Consumer;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutionController;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutorSession;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HARulesExecutorFactory extends RulesExecutorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HARulesExecutorFactory.class);

    public static RulesExecutor createRulesExecutor(RulesSet rulesSet) {
        RulesExecutor rulesExecutor = new HARulesExecutor(createRulesExecutorSession(rulesSet));
        configurePseudoClock(rulesSet, rulesExecutor);
        return rulesExecutor;
    }

    public static RulesExecutor createRulesExecutorWithRecovery(RulesSet rulesSet, Consumer<RulesExecutor> recovery) {
        LOG.info("Creating RulesExecutor in recovery mode");
        HARulesExecutor rulesExecutor = new HARulesExecutor(createRulesExecutorSession(rulesSet));
        rulesExecutor.setOnRecovery(true);
        LOG.info("No automatic advance of internal pseudo-clock because session recovery is in-progress");
        recovery.accept(rulesExecutor);
        rulesExecutor.setOnRecovery(false);
        LOG.info("Session recovery completed");
        configurePseudoClock(rulesSet, rulesExecutor); // now configure the pseudo-clock as per normal
        return rulesExecutor;
    }

    protected static RulesExecutorSession createRulesExecutorSession(RulesSet rulesSet) {
        RulesExecutionController rulesExecutionController = new RulesExecutionController();
        KieSession kieSession = createKieSession(rulesSet, rulesExecutionController);
        return new HARulesExecutorSession(rulesSet, kieSession, rulesExecutionController, ID_GENERATOR.getAndIncrement());
    }
}

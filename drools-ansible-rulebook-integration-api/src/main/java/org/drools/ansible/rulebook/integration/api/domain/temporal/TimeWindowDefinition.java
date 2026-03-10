package org.drools.ansible.rulebook.integration.api.domain.temporal;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.model.PatternDSL;
import org.drools.model.Rule;
import org.drools.model.prototype.PrototypeDSL;
import org.drools.model.prototype.PrototypeVariable;
import org.drools.model.view.ViewItem;
import org.kie.api.prototype.PrototypeEventInstance;
import org.kie.api.prototype.PrototypeFactInstance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.drools.ansible.rulebook.integration.api.domain.temporal.TimeAmount.parseTimeAmount;
import static org.drools.ansible.rulebook.integration.api.rulesengine.RegisterOnlyAgendaFilter.SYNTHETIC_RULE_TAG;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.DEFAULT_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.getPrototypeEvent;
import static org.drools.model.DSL.after;
import static org.drools.model.DSL.not;
import static org.drools.model.DSL.on;
import static org.drools.model.Index.ConstraintType;
import static org.drools.model.PatternDSL.rule;
import static org.drools.model.prototype.PrototypeDSL.protoPattern;
import static org.drools.model.prototype.PrototypeDSL.variable;

/**
 * Related events have to match within a time window
 *
 *    e.g.:
 *      condition:
 *         all:
 *           - events.ping << event.ping.timeout == true   # no host info
 *           - events.process << event.sensu.process.status == "stopped"   # web server
 *           - events.database << event.sensu.storage.percent > 95  # database server
 *         timeout: 5 minutes
 *
 * This feature has been implemented synthetically adding temporal constraints to the events patterns so for example this
 * condition is internally rewritten as it follows:
 *
 * - events.ping << event.ping.timeout == true
 * - events.process << event.sensu.process.status == "stopped"
 *       && this after [-5, TimeUnit.MINUTE, 5, TimeUnit.MINUTE] events.ping
 * - events.database << event.sensu.storage.percent > 95  # database server
 *       && this after [-5, TimeUnit.MINUTE, 5, TimeUnit.MINUTE] events.ping
 *       && this after [-5, TimeUnit.MINUTE, 5, TimeUnit.MINUTE] events.process
 *
 * Note that the use of a negative range in the after constraint allows the matching of this rule also when the events arrive
 * in an order that is different from the one listed in the rule itself.
 *
 * <h2>HA sentinel rules (generated only in HA mode)</h2>
 *
 * In HA mode, one sentinel rule is generated per pattern to track which events have been matched.
 * Each sentinel fires independently when its condition matches, inserting a control event that expires
 * after the window duration. On recovery, expired sentinels are detected and a WARN is logged.
 *
 * For the example above, the following 3 sentinel rules are generated:
 *
 * <pre>
 * rule R_tw_sentinel_0 when
 *   ping : Event( ping.timeout == true )
 *   not Control( control_name == "time_window_control", drools_rule_name == "R", pattern_index == 0 )
 * then
 *   insert Control( control_name = "time_window_control", drools_rule_name = "R",
 *                   pattern_index = 0, total_patterns = 3,
 *                   matched_event = ping.asMap(), @expires( 5m ) )
 *
 * rule R_tw_sentinel_1 when
 *   process : Event( sensu.process.status == "stopped" )
 *   not Control( control_name == "time_window_control", drools_rule_name == "R", pattern_index == 1 )
 * then
 *   insert Control( control_name = "time_window_control", drools_rule_name = "R",
 *                   pattern_index = 1, total_patterns = 3,
 *                   matched_event = process.asMap(), @expires( 5m ) )
 *
 * rule R_tw_sentinel_2 when
 *   database : Event( sensu.storage.percent > 95 )
 *   not Control( control_name == "time_window_control", drools_rule_name == "R", pattern_index == 2 )
 * then
 *   insert Control( control_name = "time_window_control", drools_rule_name = "R",
 *                   pattern_index = 2, total_patterns = 3,
 *                   matched_event = database.asMap(), @expires( 5m ) )
 * </pre>
 */
public class TimeWindowDefinition implements TimeConstraint {

    public static final String TIME_WINDOW_CONTROL = "time_window_control";

    private final TimeAmount timeAmount;

    private final List<PrototypeVariable> formerVariables = new ArrayList<>();

    // Sentinel support: saved pattern item snapshots (user conditions only, before temporal constraints)
    private String ruleName;
    private final List<PrototypeDSL.PrototypePatternDef> savedPatterns = new ArrayList<>();
    private final List<Integer> savedUserItemCounts = new ArrayList<>();

    private TimeWindowDefinition(TimeAmount timeAmount) {
        this.timeAmount = timeAmount;
    }

    @Override
    public boolean requiresAsyncExecution() {
        return false;
    }

    public static TimeWindowDefinition parseTimeWindow(String timeWindow) {
        return new TimeWindowDefinition(parseTimeAmount(timeWindow));
    }

    public ViewItem processTimeConstraint(String ruleName, ViewItem pattern) {
        this.ruleName = ruleName;
        PrototypeDSL.PrototypePatternDef protoPattern = (PrototypeDSL.PrototypePatternDef) pattern;

        // Snapshot: save pattern reference and user-item count BEFORE temporal constraints are added
        savedPatterns.add(protoPattern);
        savedUserItemCounts.add(((PatternDSL.PatternDefImpl<?>) protoPattern).getItems().size());

        formerVariables.forEach(v -> protoPattern.expr(after(-timeAmount.getAmount(), timeAmount.getTimeUnit(), timeAmount.getAmount(), timeAmount.getTimeUnit()), v));
        formerVariables.add((PrototypeVariable) protoPattern.getFirstVariable());
        return protoPattern;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Rule> getControlRules(RuleGenerationContext ruleContext) {
        if (!ruleContext.isHaMode() || savedPatterns.isEmpty()) {
            return Collections.emptyList();
        }

        int totalPatterns = savedPatterns.size();
        List<Rule> rules = new ArrayList<>();

        for (int i = 0; i < totalPatterns; i++) {
            final int patternIndex = i;

            // Create sentinel event pattern: same prototype, same user conditions, NO temporal constraints
            PrototypeDSL.PrototypePatternDef sentinelEventPattern = protoPattern(variable(getPrototypeEvent(DEFAULT_PROTOTYPE_NAME)));
            List sentinelItems = ((PatternDSL.PatternDefImpl<?>) sentinelEventPattern).getItems();
            List sourceItems = ((PatternDSL.PatternDefImpl<?>) savedPatterns.get(i)).getItems();
            int userItemCount = savedUserItemCounts.get(i);
            for (int j = 0; j < userItemCount; j++) {
                sentinelItems.add(sourceItems.get(j));
            }

            // "not exists" pattern: no sentinel control for this rule + pattern_index yet
            PrototypeDSL.PrototypePatternDef notExistsControl = protoPattern(variable(getPrototypeEvent(SYNTHETIC_PROTOTYPE_NAME)))
                    .expr(CONTROL_NAME, ConstraintType.EQUAL, TIME_WINDOW_CONTROL)
                    .expr("drools_rule_name", ConstraintType.EQUAL, ruleName)
                    .expr("pattern_index", ConstraintType.EQUAL, patternIndex);

            rules.add(
                    rule(ruleName + "_tw_sentinel_" + patternIndex).metadata(SYNTHETIC_RULE_TAG, true)
                            .build(
                                    sentinelEventPattern,
                                    not(notExistsControl),
                                    on(sentinelEventPattern.getFirstVariable()).execute((drools, event) -> {
                                        PrototypeEventInstance controlEvent = getPrototypeEvent(SYNTHETIC_PROTOTYPE_NAME).newInstance()
                                                .withExpiration(timeAmount.getAmount(), timeAmount.getTimeUnit());
                                        controlEvent.put(CONTROL_NAME, TIME_WINDOW_CONTROL);
                                        controlEvent.put("drools_rule_name", ruleName);
                                        controlEvent.put("pattern_index", patternIndex);
                                        controlEvent.put("total_patterns", totalPatterns);
                                        // Store the matched event payload on the sentinel for guaranteed retrieval on recovery
                                        controlEvent.put("matched_event", ((PrototypeFactInstance) event).asMap());
                                        drools.insert(controlEvent);
                                    })
                            )
            );
        }

        return rules;
    }
}

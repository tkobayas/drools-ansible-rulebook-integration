package org.drools.ansible.rulebook.integration.ha.util;

import java.util.HashMap;
import java.util.Map;

import org.drools.core.WorkingMemoryEntryPoint;
import org.drools.core.common.InternalWorkingMemory;
import org.drools.core.common.ObjectTypeConfigurationRegistry;
import org.drools.core.reteoo.BetaMemory;
import org.drools.core.reteoo.BetaNode;
import org.drools.core.reteoo.EntryPointNode;
import org.drools.core.reteoo.LeftInputAdapterNode;
import org.drools.core.reteoo.LeftTupleSource;
import org.drools.core.reteoo.ObjectSink;
import org.drools.core.reteoo.ObjectSource;
import org.drools.core.reteoo.ObjectTypeNode;
import org.drools.core.reteoo.Rete;
import org.drools.kiesession.rulebase.InternalKnowledgeBase;
import org.kie.api.runtime.KieSession;

/**
 * Utility to count partial (left tuple) matches per rule by traversing the rete graph
 * and inspecting beta node memories. Uses Drools internal APIs.
 *
 * TODO: Review performance and consider any improvement if needed.
 */
public final class PartialMatchCounter {

    private PartialMatchCounter() {
    }

    public static Map<String, Integer> countPartialTuplesPerRule(KieSession ksession) {
        InternalWorkingMemory wm = (InternalWorkingMemory) ksession;
        InternalKnowledgeBase kb = (InternalKnowledgeBase) wm.getKnowledgeBase();
        Rete rete = kb.getRete();
        Map<String, Integer> counts = new HashMap<>();
        for (EntryPointNode ep : rete.getEntryPointNodes().values()) {
            for (ObjectTypeNode objectTypeNode : ep.getObjectTypeNodes().values()) {
                walkObjectSource(objectTypeNode, wm, counts);
            }
        }
        return counts;
    }

    public static int countPartialTuplesTotal(KieSession ksession) {
        return countPartialTuplesPerRule(ksession).values().stream().mapToInt(Integer::intValue).sum();
    }

    private static void walkObjectSource(ObjectSource source, InternalWorkingMemory wm, Map<String, Integer> counts) {
        for (ObjectSink sink : source.getObjectSinkPropagator().getSinks()) {
            if (sink instanceof BetaNode betaNode) {
                addBetaMemoryCounts(betaNode, wm, counts);
                walkLeftTupleSource(betaNode, wm, counts);
            } else if (sink instanceof LeftInputAdapterNode lia) {
                walkLeftTupleSource(lia, wm, counts);
            } else if (sink instanceof ObjectSource os) {
                walkObjectSource(os, wm, counts);
            }
        }
    }

    private static void walkLeftTupleSource(LeftTupleSource source, InternalWorkingMemory wm, Map<String, Integer> counts) {
        for (var sink : source.getSinkPropagator().getSinks()) {
            if (sink instanceof BetaNode betaNode) {
                addBetaMemoryCounts(betaNode, wm, counts);
                walkLeftTupleSource(betaNode, wm, counts);
            } else if (sink instanceof LeftTupleSource lts) {
                walkLeftTupleSource(lts, wm, counts);
            } else if (sink instanceof ObjectSource os) {
                walkObjectSource(os, wm, counts);
            }
        }
    }

    /**
     * TODO: getRules() may be slow. Consider caching rule names per beta node.
     * TODO: mem.getLeftTupleMemory() may not be an exact way to count partial matches, because of lazy evaluation (rule linking),
     *       but it works so far with current tests.
     */
    private static void addBetaMemoryCounts(BetaNode betaNode, InternalWorkingMemory wm, Map<String, Integer> counts) {
        BetaMemory mem = (BetaMemory) wm.getNodeMemory(betaNode);
        int partialTuples = mem.getLeftTupleMemory().size();
        for (String ruleName : betaNode.getRules()) {
            counts.merge(ruleName, partialTuples, Integer::sum);
        }
    }
}

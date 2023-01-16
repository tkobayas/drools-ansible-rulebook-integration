package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.constraints.ExistsField;
import org.drools.ansible.rulebook.integration.api.domain.constraints.ItemInListConstraint;
import org.drools.ansible.rulebook.integration.api.domain.constraints.ItemNotInListConstraint;
import org.drools.ansible.rulebook.integration.api.domain.constraints.ListContainsConstraint;
import org.drools.ansible.rulebook.integration.api.domain.constraints.ListNotContainsConstraint;
import org.drools.ansible.rulebook.integration.api.domain.temporal.OnceAfterDefinition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.OnceWithinDefinition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeConstraint;
import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeWindowDefinition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.TimedOutDefinition;
import org.drools.ansible.rulebook.integration.api.rulesmodel.BetaParsedCondition;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.ConstraintOperator;
import org.drools.model.Index;
import org.drools.model.PrototypeDSL.PrototypePatternDef;
import org.drools.model.PrototypeExpression;
import org.drools.model.PrototypeVariable;
import org.drools.model.view.ViewItem;
import org.json.JSONObject;

import static org.drools.model.PrototypeDSL.fieldName2PrototypeExpression;
import static org.drools.model.PrototypeExpression.fixedValue;
import static org.drools.model.PrototypeExpression.thisPrototype;

public class MapCondition implements Condition {

    private Map<?,?> map;

    private String patternBinding;

    public MapCondition() { } // used for serialization

    public MapCondition(Map<?,?> map) {
        this.map = map;
    }

    public Map<?,?> getMap() {
        return map;
    }

    public void setMap(Map<?,?> map) {
        this.map = map;
    }

    private String getPatternBinding(RuleGenerationContext ruleContext) {
        if (patternBinding == null) {
            patternBinding = ruleContext.generateBinding();
        }
        return patternBinding;
    }

    private MapCondition withPatternBinding(String patternBinding) {
        this.patternBinding = patternBinding;
        return this;
    }

    @Override
    public ViewItem toPattern(RuleGenerationContext ruleContext) {
        return map2Ast(ruleContext, parseConditionAttributes(ruleContext, this), null).toPattern(ruleContext);
    }

    private static MapCondition parseConditionAttributes(RuleGenerationContext ruleContext, MapCondition condition) {
        parseThrottle(ruleContext, condition);
        parseTimeOut(ruleContext, condition);
        return condition;
    }

    private static void parseThrottle(RuleGenerationContext ruleContext, MapCondition condition) {
        Map throttle = (Map) condition.getMap().remove("throttle");
        if (throttle != null) {
            List<String> groupByAttributes = (List<String>) throttle.get(TimeConstraint.GROUP_BY_ATTRIBUTES);
            String onceWithin = (String) throttle.get(OnceWithinDefinition.KEYWORD);
            if (onceWithin != null) {
                ruleContext.setTimeConstraint(OnceWithinDefinition.parseOnceWithin(onceWithin, groupByAttributes));
                return;
            }
            String onceAfter = (String) throttle.get(OnceAfterDefinition.KEYWORD);
            if (onceAfter != null) {
                ruleContext.setTimeConstraint(OnceAfterDefinition.parseOnceAfter(onceAfter, groupByAttributes));
                return;
            }
            throw new IllegalArgumentException("Invalid throttle definition");
        }
    }

    private static void parseTimeOut(RuleGenerationContext ruleContext, MapCondition condition) {
        String timeOut = (String) condition.getMap().remove("timeout");
        if (timeOut != null) {
            if (condition.getMap().containsKey("NotAllCondition")) {
                ruleContext.setTimeConstraint(TimedOutDefinition.parseTimedOut(timeOut));
            } else {
                ruleContext.setTimeConstraint(TimeWindowDefinition.parseTimeWindow(timeOut));
            }
        }
    }

    private static Condition map2Ast(RuleGenerationContext ruleContext, MapCondition condition, AstCondition.MultipleConditions parent) {
        assert(condition.getMap().size() == 1);
        Map.Entry entry = condition.getMap().entrySet().iterator().next();
        String expressionName = (String) entry.getKey();

        switch (expressionName) {
            case "NotAllCondition":
                if (ruleContext.getTimeConstraint().filter( tc -> tc instanceof TimedOutDefinition ).isEmpty()) {
                    throw new IllegalArgumentException("NotAllCondition requires a timeout");
                }
            case "AnyCondition":
            case "AllCondition":
                AstCondition.MultipleConditions conditions = expressionName.equals("AnyCondition") ?
                        new AstCondition.AnyCondition(ruleContext) : new AstCondition.AllCondition(ruleContext);
                List<Map> innerMaps = (List<Map>) entry.getValue();
                ruleContext.setMultiplePatterns(innerMaps.size() > 1);
                for (Map subC : innerMaps) {
                    conditions.addCondition(map2Ast(ruleContext, new MapCondition(subC), conditions));
                }
                ruleContext.setMultiplePatterns(false);
                return conditions;
        }

        return pattern2Ast(ruleContext, condition.withPatternBinding( condition.getPatternBinding(ruleContext) ), parent);
    }

    private static AstCondition.PatternCondition pattern2Ast(RuleGenerationContext ruleContext, MapCondition condition, AstCondition.MultipleConditions parent) {
        assert(condition.getMap().size() == 1);
        Map.Entry entry = condition.getMap().entrySet().iterator().next();
        String expressionName = (String) entry.getKey();

        switch (expressionName) {
            case "AssignmentExpression": {
                Map<?, ?> expression = (Map<?, ?>) entry.getValue();

                Map<?, ?> assignment = (Map<?, ?>) expression.get("lhs");
                assert (assignment.size() == 1);
                String binding = (String) assignment.values().iterator().next();

                Map<?, ?> assigned = (Map<?, ?>) expression.get("rhs");
                assert (assigned.size() == 1);
                return pattern2Ast(ruleContext, new MapCondition(assigned).withPatternBinding(binding), parent);
            }

            case "NegateExpression": {
                String binding = condition.getPatternBinding(ruleContext);
                return pattern2Ast(ruleContext, new MapCondition((Map) entry.getValue()).withPatternBinding(binding), parent).negate(ruleContext);
            }

            case "AndExpression":
            case "OrExpression": {
                String binding = condition.getPatternBinding(ruleContext);
                MapCondition lhs = new MapCondition((Map) ((Map) entry.getValue()).get("lhs")).withPatternBinding(binding);
                MapCondition rhs = new MapCondition((Map) ((Map) entry.getValue()).get("rhs")).withPatternBinding(binding);

                AstCondition.CombinedPatternCondition combinedPatternCondition = expressionName.equals("OrExpression") ?
                        new AstCondition.OrCondition(binding) :
                        new AstCondition.AndCondition(binding);
                return combinedPatternCondition
                        .withLhs(pattern2Ast(ruleContext, lhs, null))
                        .withRhs(pattern2Ast(ruleContext, rhs, null));
            }
        }

        return new AstCondition.SingleCondition(parent, condition.parseSingle(ruleContext, entry))
                .withPatternBinding(ruleContext, condition.getPatternBinding(ruleContext));
    }

    private ParsedCondition parseSingle(RuleGenerationContext ruleContext, Map.Entry entry) {
        String expressionName = (String) entry.getKey();
        if (expressionName.equals("Boolean")) {
            // "Boolean":true is the always true constraint
            return new ParsedCondition(fixedValue(true), Index.ConstraintType.EQUAL, fixedValue(entry.getValue()));
        }
        if (entry.getValue() instanceof String) {
            // the field name alone is the shortcut for fieldName == true
            return new ParsedCondition(mapEntry2Expr(ruleContext, entry).prototypeExpression, Index.ConstraintType.EQUAL, fixedValue(true));
        }

        Map<?,?> expression = (Map<?,?>) entry.getValue();

        ConstraintOperator operator = decodeOperation(expressionName);

        if (operator == ExistsField.INSTANCE) {
            return new ParsedCondition(thisPrototype(), operator, fixedValue(map2Expr(ruleContext, expression).getFieldName())).withNotPattern(expressionName.equals("IsNotDefinedExpression"));
        }

        ConditionExpression left = map2Expr(ruleContext, expression.get("lhs"));
        ConditionExpression right = map2Expr(ruleContext, expression.get("rhs"));
        return right.isBeta() ?
                new BetaParsedCondition(left.prototypeExpression, operator, right.betaVariable, right.prototypeExpression) :
                new ParsedCondition(left.prototypeExpression, operator, right.prototypeExpression).withImplicitPattern(hasImplicitPattern(ruleContext, left, right));
    }

    private boolean hasImplicitPattern(RuleGenerationContext ruleContext, ConditionExpression left, ConditionExpression right) {
        boolean hasImplicitPattern = left.field && right.field && !left.prototypeName.equals(right.prototypeName) && !ruleContext.isExistingBoundVariable(right.prototypeName);
        if (hasImplicitPattern && !ruleContext.hasOption(RuleConfigurationOption.ALLOW_IMPLICIT_JOINS)) {
            throw new UnsupportedOperationException("Cannot have an implicit pattern without using ALLOW_IMPLICIT_JOINS option");
        }
        return hasImplicitPattern;
    }

    private static ConditionExpression map2Expr(RuleGenerationContext ruleContext, Object expr) {
        if (expr instanceof String) {
            return createFieldExpression(ruleContext, (String)expr);
        }

        if (expr instanceof Collection) {
            List list = new ArrayList();
            for (Object item : (Collection)expr) {
                Map.Entry itemEntry = ((Map<?,?>) item).entrySet().iterator().next();
                if (isKnownType( (String) itemEntry.getKey() )) {
                    list.add( toJsonValue( (String) itemEntry.getKey(), itemEntry.getValue() ) );
                } else {
                    throw new UnsupportedOperationException("Unknown list item: " + itemEntry);
                }
            }
            return new ConditionExpression(fixedValue(list));
        }

        Map<?,?> exprMap = (Map) expr;
        assert(exprMap.size() == 1);
        Map.Entry entry = exprMap.entrySet().iterator().next();
        return mapEntry2Expr(ruleContext, expr, entry);
    }

    private static ConditionExpression mapEntry2Expr(RuleGenerationContext ruleContext, Map.Entry entry) {
        return mapEntry2Expr(ruleContext, null, entry);
    }

    private static ConditionExpression mapEntry2Expr(RuleGenerationContext ruleContext, Object expr, Map.Entry entry) {
        String key = (String) entry.getKey();
        Object value = entry.getValue();

        if (isKnownType(key)) {
            return new ConditionExpression(fixedValue(toJsonValue(key, value)));
        }

        if (value instanceof String) {
            String fieldName = ignoreKey(key) ? (String) value : key + "." + value;
            return createFieldExpression(ruleContext, fieldName);
        }

        if (value instanceof Map) {
            Map<?,?> expression = (Map<?,?>) value;
            return map2Expr(ruleContext, expression.get("lhs")).composeWith(decodeBinaryOperator(key), map2Expr(ruleContext, expression.get("rhs")));
        }

        throw new UnsupportedOperationException("Invalid expression: " + (expr != null ? expr : entry));
    }

    private static boolean isKnownType(String type) {
        return type.equals("Integer") || type.equals("Float") || type.equals("String") || type.equals("Boolean");
    }

    private static Object toJsonValue(String type, Object value) {
        if (type.equals("String")) {
            return value.toString();
        }
        return JSONObject.stringToValue(value.toString());
    }

    private static ConditionExpression createFieldExpression(RuleGenerationContext ruleContext, String fieldName) {
        int dotPos = fieldName.indexOf('.');
        String prototypeName = fieldName;
        PrototypeVariable betaVariable = null;
        if (dotPos > 0) {
            prototypeName = fieldName.substring(0, dotPos);
            PrototypePatternDef boundPattern = ruleContext.getBoundPattern(prototypeName);
            if ( boundPattern != null ) {
                fieldName = fieldName.substring(dotPos+1);
                betaVariable = (PrototypeVariable) boundPattern.getFirstVariable();
            }
        }
        return new ConditionExpression(fieldName2PrototypeExpression(fieldName), true, prototypeName, betaVariable);
    }

    private static boolean ignoreKey(String key) {
        return key.equalsIgnoreCase("fact") || key.equalsIgnoreCase("facts") || key.equalsIgnoreCase("event") || key.equalsIgnoreCase("events");
    }

    private static class ConditionExpression {
        private final PrototypeExpression prototypeExpression;
        private final boolean field;
        private final String prototypeName;
        private final PrototypeVariable betaVariable;

        private ConditionExpression(PrototypeExpression prototypeExpression) {
            this(prototypeExpression, false, null, null);
        }

        private ConditionExpression(PrototypeExpression prototypeExpression, boolean field, String prototypeName, PrototypeVariable betaVariable) {
            this.prototypeExpression = prototypeExpression;
            this.field = field;
            this.prototypeName = prototypeName;
            this.betaVariable = betaVariable;
        }

        public ConditionExpression composeWith(PrototypeExpression.BinaryOperation.Operator decodeBinaryOperator, ConditionExpression rhs) {
            PrototypeExpression composed = prototypeExpression.composeWith(decodeBinaryOperator, rhs.prototypeExpression);
            if (field) {
                return new ConditionExpression(composed, true, prototypeName, betaVariable);
            }
            if (rhs.field) {
                return new ConditionExpression(composed, true, prototypeName, betaVariable);
            }
            return new ConditionExpression(composed);
        }

        public boolean isBeta() {
            return betaVariable != null;
        }

        public String getFieldName() {
            return ((PrototypeExpression.PrototypeFieldValue) prototypeExpression).getFieldName();
        }

        @Override
        public String toString() {
            return "ConditionExpression{" +
                    "prototypeExpression=" + prototypeExpression +
                    ", field=" + field +
                    ", prototypeName='" + prototypeName + '\'' +
                    '}';
        }
    }

    private static ConstraintOperator decodeOperation(String expressionName) {
        switch (expressionName) {
            case "EqualsExpression":
                return Index.ConstraintType.EQUAL;
            case "NotEqualsExpression":
                return Index.ConstraintType.NOT_EQUAL;
            case "GreaterThanExpression":
                return Index.ConstraintType.GREATER_THAN;
            case "GreaterThanOrEqualToExpression":
                return Index.ConstraintType.GREATER_OR_EQUAL;
            case "LessThanExpression":
                return Index.ConstraintType.LESS_THAN;
            case "LessThanOrEqualToExpression":
                return Index.ConstraintType.LESS_OR_EQUAL;
            case "IsDefinedExpression":
            case "IsNotDefinedExpression":
                return ExistsField.INSTANCE;
            case "ListContainsItemExpression":
                return ListContainsConstraint.INSTANCE;
            case "ListNotContainsItemExpression":
                return ListNotContainsConstraint.INSTANCE;
            case "ItemInListExpression":
                return ItemInListConstraint.INSTANCE;
            case "ItemNotInListExpression":
                return ItemNotInListConstraint.INSTANCE;
        }
        throw new UnsupportedOperationException("Unrecognized operation type: " + expressionName);
    }

    private static PrototypeExpression.BinaryOperation.Operator decodeBinaryOperator(String operator) {
        switch (operator) {
            case "AdditionExpression":
                return PrototypeExpression.BinaryOperation.Operator.ADD;
            case "SubtractionExpression":
                return PrototypeExpression.BinaryOperation.Operator.SUB;
            case "MultiplicationExpression":
                return PrototypeExpression.BinaryOperation.Operator.MUL;
            case "DivisionExpression":
                return PrototypeExpression.BinaryOperation.Operator.DIV;
        }
        throw new UnsupportedOperationException("Unrecognized binary operator " + operator);
    }

    @Override
    public String toString() {
        return "MapCondition{" +
                "map=" + map +
                '}';
    }
}

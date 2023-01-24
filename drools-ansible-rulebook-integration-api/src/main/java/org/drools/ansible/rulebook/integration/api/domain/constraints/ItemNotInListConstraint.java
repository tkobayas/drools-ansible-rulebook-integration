package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.function.BiPredicate;

import org.drools.model.ConstraintOperator;

import static org.drools.ansible.rulebook.integration.api.domain.constraints.ListContainsConstraint.listContains;

public enum ItemNotInListConstraint implements ConstraintOperator {

    INSTANCE;

    public static final String EXPRESSION_NAME = "ItemNotInListExpression";

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        return (t, v) -> !listContains(v, t);
    }

    @Override
    public String toString() {
        return "ITEM_NOT_IN_LIST";
    }
}

package io.deephaven.api.expression;

import io.deephaven.api.filter.FilterIsNotNull;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;

/**
 * @see UnaryMinus
 * @see FilterNot
 * @see FilterIsNull
 * @see FilterIsNotNull
 */
public interface UnaryExpression extends Expression {

    Expression parent();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {

        void visit(UnaryMinus unaryMinus);

        void visit(FilterNot filterNot);

        void visit(FilterIsNull filterIsNull);

        void visit(FilterIsNotNull filterIsNotNull);
    }
}

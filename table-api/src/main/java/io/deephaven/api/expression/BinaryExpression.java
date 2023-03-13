package io.deephaven.api.expression;

import io.deephaven.api.filter.FilterComparison;

public interface BinaryExpression extends Expression {

    Expression lhs();

    Expression rhs();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {

        void visit(Plus plus);

        void visit(Minus minus);

        void visit(Multiply multiply);

        void visit(Divide divide);

        void visit(FilterComparison filterComparison);

        void visit(BinaryFunction binaryFunction);
    }
}

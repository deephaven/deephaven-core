package io.deephaven.api.expression;

import io.deephaven.api.filter.Filter;
import io.deephaven.api.value.Literal;

public abstract class ExpressionBasicVisitor implements
        Expression.Visitor,
        Filter.Visitor,
        NullaryExpression.Visitor,
        UnaryExpression.Visitor,
        BinaryExpression.Visitor,
        Literal.Visitor {

    @Override
    public final void visit(Filter filter) {
        filter.walk((Filter.Visitor) this);
    }

    @Override
    public final void visit(NullaryExpression nullaryExpression) {
        nullaryExpression.walk((NullaryExpression.Visitor) this);
    }

    @Override
    public final void visit(UnaryExpression unaryExpression) {
        unaryExpression.walk((UnaryExpression.Visitor) this);
    }

    @Override
    public final void visit(BinaryExpression binaryExpression) {
        binaryExpression.walk((BinaryExpression.Visitor) this);
    }

    @Override
    public final void visit(Literal literal) {
        literal.walk((Literal.Visitor) this);
    }
}

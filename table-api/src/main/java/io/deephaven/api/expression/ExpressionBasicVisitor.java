package io.deephaven.api.expression;

import io.deephaven.api.filter.Filter;
import io.deephaven.api.value.Literal;

public abstract class ExpressionBasicVisitor implements
        Expression.Visitor,
        Filter.Visitor,
        Literal.Visitor {

    @Override
    public final void visit(Filter filter) {
        filter.walk((Filter.Visitor) this);
    }

    @Override
    public final void visit(Literal literal) {
        literal.walk((Literal.Visitor) this);
    }
}

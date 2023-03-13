package io.deephaven.api.expression;

public abstract class UnaryExpressionBase implements UnaryExpression {

    @Override
    public final <V extends Expression.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

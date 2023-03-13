package io.deephaven.api.expression;

public abstract class BinaryExpressionBase implements BinaryExpression {

    @Override
    public final <V extends Expression.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

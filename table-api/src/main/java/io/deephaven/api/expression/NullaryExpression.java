package io.deephaven.api.expression;

import io.deephaven.api.ColumnName;
import io.deephaven.api.value.Literal;

public interface NullaryExpression extends Expression {

    <V extends Visitor> V walk(V visitor);

    interface Visitor {

        void visit(ColumnName columnName);

        void visit(Literal literal);
    }
}

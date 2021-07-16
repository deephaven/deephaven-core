package io.deephaven.api.value;

import io.deephaven.api.ColumnName;
import io.deephaven.api.expression.Expression;

/**
 * Represents a value.
 *
 * @see Expression
 */
public interface Value extends Expression {
    static Value of(long value) {
        return ValueLong.of(value);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        // TODO (deephaven-core#831): Add more table api Value structuring

        void visit(ColumnName x);

        void visit(long x);
    }
}

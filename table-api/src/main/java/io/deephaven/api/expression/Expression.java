package io.deephaven.api.expression;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;

/**
 * Represents an evaluate-able expression structure.
 *
 * @see io.deephaven.api.Selectable
 */
public interface Expression {

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ColumnName name);

        void visit(RawString rawString);
    }
}

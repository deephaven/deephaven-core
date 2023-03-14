/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.expression;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.value.Literal;

import java.io.Serializable;

/**
 * Represents an evaluate-able expression structure.
 *
 * @see Selectable
 */
public interface Expression extends Serializable {

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(Literal literal);

        void visit(ColumnName columnName);

        void visit(Filter filter);

        void visit(ExpressionFunction function);

        void visit(RawString rawString);
    }
}

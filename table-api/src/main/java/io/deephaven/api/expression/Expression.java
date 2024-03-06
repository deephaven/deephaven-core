//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.expression;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.literal.Literal;

/**
 * Represents an evaluate-able expression structure.
 *
 * @see Literal
 * @see ColumnName
 * @see Filter
 * @see Function
 * @see Method
 * @see RawString
 */
public interface Expression {

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(Literal literal);

        T visit(ColumnName columnName);

        T visit(Filter filter);

        T visit(Function function);

        T visit(Method method);

        T visit(RawString rawString);
    }
}

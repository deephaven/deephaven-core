/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.expression;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.literal.Literal;

import java.io.Serializable;

/**
 * Represents an evaluate-able expression structure.
 *
 * @see Literal
 * @see ColumnName
 * @see Filter
 * @see Function
 * @see Method
 * @see IfThenElse
 * @see RawString
 */
public interface Expression extends Serializable {

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(Literal literal);

        T visit(ColumnName columnName);

        T visit(Filter filter);

        T visit(Function function);

        T visit(Method method);

        T visit(IfThenElse ifThenElse);

        T visit(RawString rawString);
    }
}

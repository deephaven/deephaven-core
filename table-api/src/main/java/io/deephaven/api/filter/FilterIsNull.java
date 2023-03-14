/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Evaluates to {@code true} when the given {@link #expression() expression} evaluates to {@code null}.
 */
@Immutable
@SimpleStyle
public abstract class FilterIsNull extends FilterBase {

    public static FilterIsNull of(Expression expression) {
        return ImmutableFilterIsNull.of(expression);
    }

    /**
     * The expression.
     *
     * @return the expression
     */
    @Parameter
    public abstract Expression expression();

    /**
     * Equivalent to {@code Filter.isNotNull(expression())}.
     *
     * @return the inverse filter
     */
    @Override
    public final FilterIsNotNull inverse() {
        return Filter.isNotNull(expression());
    }

    @Override
    public final <V extends Filter.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

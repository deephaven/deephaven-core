//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
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
public abstract class FilterIsNaN extends FilterBase {

    public static FilterIsNaN of(Expression expression) {
        return ImmutableFilterIsNaN.of(expression);
    }

    /**
     * The expression.
     *
     * @return the expression
     */
    @Parameter
    public abstract Expression expression();

    @Override
    public final FilterNot<FilterIsNaN> invert() {
        return FilterNot.of(this);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}

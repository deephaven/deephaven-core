/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Evaluates to {@code true} when the given {@link #expression() expression} evaluates to not {@code null}.
 */
@Immutable
@SimpleStyle
public abstract class FilterIsNotNull extends FilterBase {

    public static FilterIsNotNull of(Expression expression) {
        return ImmutableFilterIsNotNull.of(expression);
    }

    /**
     * The expression.
     *
     * @return the expression
     */
    @Parameter
    public abstract Expression expression();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

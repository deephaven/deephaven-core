/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Optional;

/**
 * Evaluates to {@code true} when the given {@link #filter() filter} evaluates to {@code false}.
 */
@Immutable
@SimpleStyle
public abstract class FilterNot extends FilterBase {

    public static FilterNot of(Filter filter) {
        return ImmutableFilterNot.of(filter);
    }

    /**
     * The filter.
     *
     * @return the filter
     */
    @Parameter
    public abstract Filter filter();

    /**
     * Equivalent to {@code filter()}.
     *
     * @return the inverse filter
     */
    @Override
    public final Filter inverse() {
        return filter();
    }

    public final Optional<Filter> simplify() {
        final Filter filterInverse = filter().inverse();
        return equals(filterInverse) ? Optional.empty() : Optional.of(filterInverse);
    }

    @Override
    public final <V extends Filter.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

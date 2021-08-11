package io.deephaven.api.filter;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

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

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

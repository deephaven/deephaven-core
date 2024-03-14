//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.filter;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Evaluates to {@code true} when any of {@link #filters() filters} evaluates to {@code true}, and {@code false} when
 * none of the {@link #filters() filters} evaluates to {@code true}
 */
@Immutable
@BuildableStyle
public abstract class FilterOr extends FilterBase implements Iterable<Filter> {

    public static Builder builder() {
        return ImmutableFilterOr.builder();
    }

    public static FilterOr of(Filter... filters) {
        return builder().addFilters(filters).build();
    }

    public static FilterOr of(Iterable<? extends Filter> filters) {
        return builder().addAllFilters(filters).build();
    }

    /**
     * The filters.
     *
     * @return the filters
     */
    @Parameter
    public abstract List<Filter> filters();

    /**
     * Equivalent to an {@link FilterAnd and-filter} with all {@link #filters() filters} inverted.
     *
     * @return the inverse filter
     */
    @Override
    public FilterAnd invert() {
        final FilterAnd.Builder builder = FilterAnd.builder();
        for (Filter filter : filters()) {
            builder.addFilters(filter.invert());
        }
        return builder.build();
    }

    @Override
    public final <T> T walk(Filter.Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final Iterator<Filter> iterator() {
        return filters().iterator();
    }

    @Override
    public final void forEach(Consumer<? super Filter> action) {
        filters().forEach(action);
    }

    @Override
    public final Spliterator<Filter> spliterator() {
        return filters().spliterator();
    }

    @Check
    final void checkSize() {
        if (filters().size() < 2) {
            throw new IllegalArgumentException(
                    String.format("%s must have at least 2 filters", FilterOr.class));
        }
    }

    public interface Builder {

        Builder addFilters(Filter elements);

        Builder addFilters(Filter... elements);

        Builder addAllFilters(Iterable<? extends Filter> elements);

        FilterOr build();
    }
}

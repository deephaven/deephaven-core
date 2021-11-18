package io.deephaven.api.filter;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Evaluates to {@code true} when any of the given {@link #filters() filters} evaluates to {@code true}.
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

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
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

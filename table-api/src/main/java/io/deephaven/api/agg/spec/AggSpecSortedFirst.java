/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.SortColumn;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Specifies an aggregation that outputs the first value in the input column for each group, after sorting the group on
 * the {@link #columns() sort columns}.
 * 
 * @implNote The sorted-first aggregation only supports {@link SortColumn.Order#ASCENDING} columns at the moment.
 * @see <a href="https://github.com/deephaven/deephaven-core/issues/821">SortedFirst / SortedLast aggregations with sort
 *      direction</a>
 */
@Immutable
@BuildableStyle
public abstract class AggSpecSortedFirst extends AggSpecBase {

    public static Builder builder() {
        return ImmutableAggSpecSortedFirst.builder();
    }

    @Override
    public final String description() {
        return "first sorted by " + columns().stream().map(sc -> sc.column().name()).collect(Collectors.joining(", "));
    }

    /**
     * The columns to sort on to determine the order within each group.
     *
     * @return The sort columns
     */
    public abstract List<SortColumn> columns();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void nonEmptyColumns() {
        if (columns().isEmpty()) {
            throw new IllegalArgumentException("columns() must be non-empty");
        }
    }

    @Check
    final void checkSortOrder() {
        // TODO(deephaven-core#821): SortedFirst / SortedLast aggregations with sort direction
        if (!columns().stream().map(SortColumn::order).allMatch(AggSpecSortedFirst::isAscending)) {
            throw new IllegalArgumentException(
                    "Can only construct AggSpecSortedFirst with ascending, see https://github.com/deephaven/deephaven-core/issues/821");
        }
    }

    private static boolean isAscending(SortColumn.Order o) {
        return o == SortColumn.Order.ASCENDING;
    }

    public interface Builder {
        Builder addColumns(SortColumn element);

        Builder addColumns(SortColumn... elements);

        Builder addAllColumns(Iterable<? extends SortColumn> elements);

        AggSpecSortedFirst build();
    }
}

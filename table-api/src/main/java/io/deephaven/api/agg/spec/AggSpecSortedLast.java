package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.SortColumn;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * Note: the sorted-last aggregation only supports {@link SortColumn.Order#ASCENDING} columns at the moment.
 *
 * @see <a href="https://github.com/deephaven/deephaven-core/issues/821">SortedFirst / SortedLast aggregations with sort
 *      direction</a>
 */
@Immutable
@BuildableStyle
public abstract class AggSpecSortedLast extends AggSpecBase {

    public static Builder builder() {
        return ImmutableAggSpecSortedLast.builder();
    }

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
        if (!columns().stream().map(SortColumn::order).allMatch(AggSpecSortedLast::isAscending)) {
            throw new IllegalArgumentException(
                    "Can only construct AggSpecSortedLast with ascending, see https://github.com/deephaven/deephaven-core/issues/821");
        }
    }

    private static boolean isAscending(SortColumn.Order o) {
        return o == SortColumn.Order.ASCENDING;
    }

    public interface Builder {
        Builder addColumns(SortColumn element);

        Builder addColumns(SortColumn... elements);

        Builder addAllColumns(Iterable<? extends SortColumn> elements);

        AggSpecSortedLast build();
    }
}

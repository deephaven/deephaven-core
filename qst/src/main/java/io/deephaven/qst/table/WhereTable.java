package io.deephaven.qst.table;

import io.deephaven.api.TableOperations;
import io.deephaven.api.filter.Filter;
import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.filter.FilterHasRaw;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;

/**
 * @see TableOperations#where(Collection)
 */
@Immutable
@NodeStyle
public abstract class WhereTable extends TableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableWhereTable.builder();
    }

    public abstract TableSpec parent();

    public abstract List<Filter> filters();

    /**
     * Checks if any of the filters is, or contains, a {@linkplain io.deephaven.api.RawString raw-string} filter.
     *
     * @return true if there are any raw-string filters
     */
    public final boolean hasRawFilter() {
        return filters().stream().anyMatch(FilterHasRaw::of);
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {

        Builder parent(TableSpec parent);

        Builder addFilters(Filter filter);

        Builder addFilters(Filter... filter);

        Builder addAllFilters(Iterable<? extends Filter> filter);

        WhereTable build();
    }
}

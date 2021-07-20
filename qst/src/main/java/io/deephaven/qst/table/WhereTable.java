package io.deephaven.qst.table;

import io.deephaven.api.filter.Filter;
import io.deephaven.qst.NodeStyle;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@NodeStyle
public abstract class WhereTable extends TableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableWhereTable.builder();
    }

    public abstract Table parent();

    public abstract List<Filter> filters();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {

        Builder parent(Table parent);

        Builder addFilters(Filter filter);

        Builder addFilters(Filter... filter);

        Builder addAllFilters(Iterable<? extends Filter> filter);

        WhereTable build();
    }
}

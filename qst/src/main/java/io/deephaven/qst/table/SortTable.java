package io.deephaven.qst.table;

import io.deephaven.api.SortColumn;
import io.deephaven.qst.NodeStyle;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@NodeStyle
public abstract class SortTable extends TableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableSortTable.builder();
    }

    public abstract Table parent();

    public abstract List<SortColumn> columns();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {

        Builder parent(Table parent);

        Builder addColumns(SortColumn column);

        Builder addColumns(SortColumn... column);

        Builder addAllColumns(Iterable<? extends SortColumn> column);

        SortTable build();
    }
}

package io.deephaven.qst.table;

import io.deephaven.api.SortColumn;
import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;

/**
 * @see TableOperations#sort(Collection)
 */
@Immutable
@NodeStyle
public abstract class SortTable extends TableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableSortTable.builder();
    }

    public abstract TableSpec parent();

    public abstract List<SortColumn> columns();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {

        Builder parent(TableSpec parent);

        Builder addColumns(SortColumn column);

        Builder addColumns(SortColumn... column);

        Builder addAllColumns(Iterable<? extends SortColumn> column);

        SortTable build();
    }
}

package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;

/**
 * @see TableOperations#snapshot(Object, boolean, Collection)
 */
@Immutable
@NodeStyle
public abstract class SnapshotTable extends TableBase {

    public static Builder builder() {
        return ImmutableSnapshotTable.builder();
    }

    public abstract TableSpec base();

    public abstract TableSpec trigger();

    public abstract List<ColumnName> stampColumns();

    @Default
    public boolean doInitialSnapshot() {
        return true;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {

        Builder base(TableSpec base);

        Builder trigger(TableSpec trigger);

        Builder addStampColumns(ColumnName element);

        Builder addStampColumns(ColumnName... elements);

        Builder addAllStampColumns(Iterable<? extends ColumnName> elements);

        Builder doInitialSnapshot(boolean doInitialSnapshot);

        SnapshotTable build();
    }
}

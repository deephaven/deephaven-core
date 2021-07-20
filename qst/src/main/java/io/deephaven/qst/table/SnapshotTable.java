package io.deephaven.qst.table;

import io.deephaven.api.ColumnName;
import io.deephaven.qst.NodeStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@NodeStyle
public abstract class SnapshotTable extends TableBase {

    public static Builder builder() {
        return ImmutableSnapshotTable.builder();
    }

    public abstract Table base();

    public abstract Table trigger();

    public abstract List<ColumnName> stampColumns();

    @Default
    public boolean doInitialSnapshot() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {

        Builder base(Table base);

        Builder trigger(Table trigger);

        Builder addStampColumns(ColumnName element);

        Builder addStampColumns(ColumnName... elements);

        Builder addAllStampColumns(Iterable<? extends ColumnName> elements);

        Builder doInitialSnapshot(boolean doInitialSnapshot);

        SnapshotTable build();
    }
}

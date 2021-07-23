package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@NodeStyle
public abstract class ViewTable extends TableBase implements SelectableTable {

    public static Builder builder() {
        return ImmutableViewTable.builder();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends SelectableTable.Builder<ViewTable, Builder> {

    }
}

package io.deephaven.qst.table;

import io.deephaven.qst.NodeStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@NodeStyle
public abstract class SelectTable extends SelectableTableBase {

    public static Builder builder() {
        return ImmutableSelectTable.builder();
    }

    @Override
    final boolean skipNonEmptyCheck() {
        return true;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public final boolean isSelectAll() {
        return columns().isEmpty();
    }

    public interface Builder extends SelectableTable.Builder<SelectTable, Builder> {

    }
}

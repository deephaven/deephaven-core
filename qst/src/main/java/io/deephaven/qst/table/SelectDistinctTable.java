package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@NodeStyle
public abstract class SelectDistinctTable extends ByTableBase {

    public static Builder builder() {
        return ImmutableSelectDistinctTable.builder();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends ByTableBase.Builder<SelectDistinctTable, SelectDistinctTable.Builder> {

    }
}

package io.deephaven.qst.table;

import io.deephaven.qst.NodeStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@NodeStyle
public abstract class ReverseTable extends TableBase implements SingleParentTable {

    public static ReverseTable of(Table parent) {
        return ImmutableReverseTable.of(parent);
    }

    @Parameter
    public abstract Table parent();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

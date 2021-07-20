package io.deephaven.qst.table;

import io.deephaven.qst.NodeStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@NodeStyle
public abstract class TailTable extends TableBase implements SingleParentTable {

    public static TailTable of(Table parent, long size) {
        return ImmutableTailTable.of(parent, size);
    }

    @Parameter
    public abstract Table parent();

    @Parameter
    public abstract long size();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkSize() {
        if (size() <= 0) {
            throw new IllegalArgumentException("Must have positive size");
        }
    }
}

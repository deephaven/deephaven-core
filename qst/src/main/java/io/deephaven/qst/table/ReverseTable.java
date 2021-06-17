package io.deephaven.qst.table;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
public abstract class ReverseTable extends TableBase implements SingleParentTable {

    @Parameter
    public abstract Table parent();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

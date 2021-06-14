package io.deephaven.qst.table;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnMatch implements JoinMatch {

    @Parameter
    public abstract ColumnName left();

    @Parameter
    public abstract ColumnName right();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

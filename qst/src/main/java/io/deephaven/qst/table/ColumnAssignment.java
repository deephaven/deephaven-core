package io.deephaven.qst.table;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnAssignment implements JoinAddition {

    @Parameter
    public abstract ColumnName newColumn();

    @Parameter
    public abstract ColumnName existingColumn();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

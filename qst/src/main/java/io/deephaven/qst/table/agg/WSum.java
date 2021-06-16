package io.deephaven.qst.table.agg;

import io.deephaven.qst.table.ColumnName;
import io.deephaven.qst.table.JoinMatch;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class WSum implements Aggregation {

    @Parameter
    public abstract JoinMatch match();

    @Parameter
    public abstract ColumnName weight();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinMatch;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class WAvg implements Aggregation {

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

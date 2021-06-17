package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinMatch;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class Count implements Aggregation {

    public static Count of(ColumnName name) {
        return ImmutableCount.of(name);
    }

    public static Count of(String x) {
        return of(ColumnName.of(x));
    }

    @Parameter
    public abstract ColumnName column();

    public final JoinMatch match() {
        return column();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

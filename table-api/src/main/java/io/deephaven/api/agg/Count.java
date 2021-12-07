package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class Count implements Aggregation {

    public static Count of(ColumnName name) {
        return ImmutableCount.of(name);
    }

    public static Count of(String x) {
        return of(ColumnName.of(x));
    }

    @Parameter
    public abstract ColumnName column();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

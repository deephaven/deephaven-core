package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class WSum implements Aggregation {

    public static WSum of(ColumnName weight, Pair addition) {
        return ImmutableWSum.of(addition, weight);
    }

    @Parameter
    public abstract Pair pair();

    @Parameter
    public abstract ColumnName weight();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class WAvg implements Aggregation {

    public static WAvg of(ColumnName weight, Pair addition) {
        return ImmutableWAvg.of(addition, weight);
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

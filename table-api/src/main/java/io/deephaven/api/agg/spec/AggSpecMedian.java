package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class AggSpecMedian extends AggSpecBase {

    public static AggSpecMedian of() {
        return ImmutableAggSpecMedian.builder().build();
    }

    public static AggSpecMedian of(boolean averageMedian) {
        return ImmutableAggSpecMedian.builder().averageMedian(averageMedian).build();
    }

    @Default
    public boolean averageMedian() {
        return true;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}

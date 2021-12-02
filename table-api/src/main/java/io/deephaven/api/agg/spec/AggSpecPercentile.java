package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class AggSpecPercentile extends AggSpecBase {

    public static AggSpecPercentile of(double percentile) {
        return ImmutableAggSpecPercentile.builder().percentile(percentile).build();
    }

    public static AggSpecPercentile of(double percentile, boolean averageMedian) {
        return ImmutableAggSpecPercentile.builder().percentile(percentile).averageMedian(averageMedian).build();
    }

    public abstract double percentile();

    @Default
    public boolean averageMedian() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkPercentile() {
        if (percentile() < 0.0 || percentile() > 1.0) {
            throw new IllegalArgumentException("Percentile must be in range [0.0, 1.0]");
        }
    }
}

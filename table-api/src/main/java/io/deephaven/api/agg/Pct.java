package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class Pct implements Aggregation {

    public static Pct of(double percentile, Pair pair) {
        return ImmutablePct.builder().percentile(percentile).pair(pair).build();
    }

    public abstract Pair pair();

    public abstract double percentile();

    @Default
    public boolean averageMedian() {
        return false;
    }

    public Pct withAverage() {
        return ImmutablePct.builder().percentile(percentile()).pair(pair()).averageMedian(true)
                .build();
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

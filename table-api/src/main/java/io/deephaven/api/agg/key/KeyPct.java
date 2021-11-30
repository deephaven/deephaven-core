package io.deephaven.api.agg.key;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class KeyPct extends KeyBase {

    public static KeyPct of(double percentile) {
        return ImmutableKeyPct.builder().percentile(percentile).build();
    }

    public static KeyPct of(double percentile, boolean averageMedian) {
        return ImmutableKeyPct.builder().percentile(percentile).averageMedian(averageMedian).build();
    }

    public abstract double percentile();

    @Default
    public boolean averageMedian() {
        return false;
    }

    public KeyPct withAverage() {
        return ImmutableKeyPct.builder()
                .percentile(percentile())
                .averageMedian(true)
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

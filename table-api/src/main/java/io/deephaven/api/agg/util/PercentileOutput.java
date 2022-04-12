package io.deephaven.api.agg.util;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Percentile and output column pair, used when specifying that a given input column should be aggregated into multiple
 * exact or approximate percentiles.
 */
@Immutable
@SimpleStyle
public abstract class PercentileOutput {

    public static PercentileOutput of(double percentile, ColumnName output) {
        return ImmutablePercentileOutput.of(percentile, output);
    }

    public static PercentileOutput of(double percentile, String output) {
        return of(percentile, ColumnName.of(output));
    }

    /**
     * Percentile. Must be in range [0.0, 1.0].
     *
     * @return The percentile
     */
    @Parameter
    public abstract double percentile();

    /**
     * Output {@link ColumnName column name}.
     *
     * @return The output {@link ColumnName column name}
     */
    @Parameter
    public abstract ColumnName output();

    @Value.Check
    final void checkPercentile() {
        if (percentile() < 0.0 || percentile() > 1.0) {
            throw new IllegalArgumentException("Percentile must be in range [0.0, 1.0]");
        }
    }
}

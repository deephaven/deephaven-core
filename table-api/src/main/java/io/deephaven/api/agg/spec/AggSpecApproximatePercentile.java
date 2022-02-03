package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Specifies an aggregation that outputs a percentile approximated using a T-Digest with the specified
 * {@link #compression()}.
 * <p>
 * Efficiently supports multiple output percentiles based on a single input column.
 * <p>
 * May only be used on static or add-only tables.
 * <p>
 * Only supported for numeric types.
 */
@Immutable
@BuildableStyle
public abstract class AggSpecApproximatePercentile extends AggSpecBase {

    public static AggSpecApproximatePercentile of(double percentile) {
        return ImmutableAggSpecApproximatePercentile.builder().percentile(percentile).build();
    }

    public static AggSpecApproximatePercentile of(double percentile, double compression) {
        return ImmutableAggSpecApproximatePercentile.builder().percentile(percentile).compression(compression).build();
    }

    @Override
    public final String description() {
        return String.format("%.2f approximate percentile with compression %.2f", percentile(), compression());
    }

    /**
     * Percentile. Must be in range [0.0, 1.0].
     *
     * @return The percentile
     */
    @Parameter
    public abstract double percentile();

    /**
     * T-Digest compression factor. Must be greater than or equal to 1. Defaults to 100. 1000 is extremely large.
     * 
     * @return The T-Digest compression factor
     */
    @Default
    @Parameter
    public double compression() {
        return 100.0;
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

    @Check
    final void checkCompression() {
        if (compression() < 1.0) {
            throw new IllegalArgumentException("Compression must be greater than or equal to 1.0");
        }
    }
}

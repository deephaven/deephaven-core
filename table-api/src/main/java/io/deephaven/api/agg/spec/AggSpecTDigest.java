package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs a T-Digest (com.tdunning.math.stats.TDigest) with the specified
 * {@link #compression()}.
 * <p>
 * May be used to implement parallel percentile calculations by splitting inputs and accumulating results into a single
 * downstream TDigest.
 * <p>
 * May only be used on static or add-only tables.
 */
@Immutable
@BuildableStyle
public abstract class AggSpecTDigest extends AggSpecBase {

    public static AggSpecTDigest of() {
        return ImmutableAggSpecTDigest.builder().build();
    }

    public static AggSpecTDigest of(double compression) {
        return ImmutableAggSpecTDigest.builder().compression(compression).build();
    }

    @Override
    public final String description() {
        return String.format("TDigest with compression %.2f", compression());
    }

    /**
     * T-Digest compression factor. Must be greater than or equal to 1. Defaults to 100. 1000 is extremely large.
     * 
     * @return The T-Digest compression factor
     */
    @Default
    public double compression() {
        return 100.0;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkCompression() {
        if (compression() < 1.0) {
            throw new IllegalArgumentException("Compression must be greater than or equal to 1.0");
        }
    }
}

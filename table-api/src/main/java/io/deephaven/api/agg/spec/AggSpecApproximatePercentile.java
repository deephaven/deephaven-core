//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.OptionalDouble;

/**
 * Specifies an aggregation that outputs a percentile approximated using a T-Digest with the specified
 * {@link #compression()}.
 *
 * <p>
 * Efficiently supports multiple output percentiles based on a single input column.
 *
 * <p>
 * May only be used on static or add-only tables.
 *
 * <p>
 * Only supported for numeric types.
 */
@Immutable
@BuildableStyle
public abstract class AggSpecApproximatePercentile extends AggSpecBase {

    /**
     * Create a new AggSpecApproximatePercentile with {@code compression} chosen by the server.
     *
     * @param percentile the percentile
     * @return the agg spec
     */
    public static AggSpecApproximatePercentile of(double percentile) {
        return ImmutableAggSpecApproximatePercentile.builder()
                .percentile(percentile)
                .build();
    }

    /**
     * Create a new AggSpecApproximatePercentile.
     *
     * @param percentile the percentile
     * @param compression the compression
     * @return the agg spec
     */
    public static AggSpecApproximatePercentile of(double percentile, double compression) {
        return ImmutableAggSpecApproximatePercentile.builder()
                .percentile(percentile)
                .compression(compression)
                .build();
    }

    @Override
    public final String description() {
        if (compression().isPresent()) {
            return String.format("%.2f approximate percentile with compression %.2f", percentile(),
                    compression().getAsDouble());
        }
        return String.format("%.2f approximate percentile with default compression", percentile());
    }

    /**
     * Percentile. Must be in range [0.0, 1.0].
     *
     * @return The percentile
     */
    public abstract double percentile();

    /**
     * T-Digest compression factor. Must be greater than or equal to 1. 1000 is extremely large.
     *
     * <p>
     * When not specified, the engine will choose a compression value.
     *
     * @return The T-Digest compression factor if specified
     */
    public abstract OptionalDouble compression();

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
        if (compression().isPresent() && compression().getAsDouble() < 1.0) {
            throw new IllegalArgumentException("Compression must be greater than or equal to 1.0");
        }
    }
}

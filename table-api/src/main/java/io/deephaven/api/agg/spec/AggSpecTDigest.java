//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.OptionalDouble;

/**
 * Specifies an aggregation that outputs a T-Digest (com.tdunning.math.stats.TDigest) with the specified
 * {@link #compression()}.
 *
 * <p>
 * May be used to implement parallel percentile calculations by splitting inputs and accumulating results into a single
 * downstream TDigest.
 *
 * <p>
 * May only be used on static or add-only tables.
 */
@Immutable
@SimpleStyle
public abstract class AggSpecTDigest extends AggSpecBase {

    /**
     * Create a new AggSpecTDigest with {@code compression} chosen by the server.
     *
     * @return the agg spec
     */
    public static AggSpecTDigest of() {
        return ImmutableAggSpecTDigest.of(OptionalDouble.empty());
    }

    /**
     * Create a new AggSpecTDigest.
     *
     * @param compression the compression
     * @return the agg spec
     */
    public static AggSpecTDigest of(double compression) {
        return ImmutableAggSpecTDigest.of(OptionalDouble.of(compression));
    }

    @Override
    public final String description() {
        if (compression().isPresent()) {
            return String.format("TDigest with compression %.2f", compression().getAsDouble());
        }
        return "TDigest with default compression";
    }

    /**
     * T-Digest compression factor. Must be greater than or equal to 1. 1000 is extremely large.
     *
     * <p>
     * When not specified, the server will choose a compression value.
     *
     * @return The T-Digest compression factor if specified
     */
    @Parameter
    public abstract OptionalDouble compression();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkCompression() {
        if (compression().isPresent() && compression().getAsDouble() < 1.0) {
            throw new IllegalArgumentException("Compression must be greater than or equal to 1.0");
        }
    }
}

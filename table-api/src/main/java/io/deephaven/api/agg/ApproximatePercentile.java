package io.deephaven.api.agg;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.annotations.TupleStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Optional;

/**
 * Approximate percentile aggregation using a T-Digest for calculation. Efficiently supports multiple output percentiles
 * based on a single input column. May only be used on static or add-only tables.
 */
@Immutable
@BuildableStyle
public abstract class ApproximatePercentile implements Aggregation {

    public static ApproximatePercentile.Builder builder() {
        return ImmutableApproximatePercentile.builder();
    }

    @Immutable
    @TupleStyle
    public static abstract class PercentileOutput {

        public static PercentileOutput of(double percentile, ColumnName output) {
            return PercentileOutputTuple.of(percentile, output);
        }

        /**
         * Percentile. Must be in range [0.0, 1.0].
         *
         * @return The percentile
         */
        public abstract double percentile();

        /**
         * Output {@link ColumnName column name}.
         *
         * @return The output {@link ColumnName column name}
         */
        public abstract ColumnName output();

        @Check
        final void checkPercentile() {
            if (percentile() < 0.0 || percentile() > 1.0) {
                throw new IllegalArgumentException("Percentile must be in range [0.0, 1.0]");
            }
        }
    }

    /**
     * Input {@link ColumnName column name}.
     *
     * @return The input {@link ColumnName column name}
     */
    public abstract ColumnName input();

    /**
     * T-Digest compression factor. Must be greater than or equal to 1. Defaults to 100. 1000 is extremely large.
     * 
     * @return The T-Digest compression factor
     */
    @Default
    public double compression() {
        return 100.0;
    }

    /**
     * Optional {@link ColumnName column name} to expose the T-Digest used in approximate percentile calculation.
     *
     * @return The optional T-Digest {@link ColumnName column name}
     */
    public abstract Optional<ColumnName> digest();

    /**
     * Pairs linking desired approximate percentiles with their output {@link ColumnName column name}.
     *
     * @return The percentile-output pairs
     */
    public abstract List<PercentileOutput> percentileOutputs();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkCompression() {
        if (compression() < 1.0) {
            throw new IllegalArgumentException("Compression must be in greater than or equal to 1.0");
        }
    }

    @Check
    final void checkNonEmpty() {
        if (percentileOutputs().isEmpty()) {
            throw new IllegalArgumentException("ApproximatePercentile should have at least one percentile output");
        }
    }

    public interface Builder {
        ApproximatePercentile.Builder input(ColumnName input);

        ApproximatePercentile.Builder compression(double compression);

        ApproximatePercentile.Builder digest(ColumnName digest);

        default ApproximatePercentile.Builder addPercentileOutput(double percentile, ColumnName output) {
            return addPercentileOutputs(PercentileOutput.of(percentile, output));
        }

        ApproximatePercentile.Builder addPercentileOutputs(PercentileOutput percentileOutputs);

        ApproximatePercentile.Builder addPercentileOutputs(PercentileOutput... percentileOutputs);

        ApproximatePercentile.Builder addAllPercentileOutputs(Iterable<? extends PercentileOutput> elements);

        ApproximatePercentile build();
    }
}

package io.deephaven.api.updateby;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.updateby.spec.*;

import java.time.Duration;

/**
 * Defines an operation that can be applied to a table with Table#updateBy()}
 */
public interface UpdateByOperation {
    /**
     * Conjoin an {@link UpdateBySpec} with columns for it to be applied to so the engine can construct the proper
     * operators.
     * 
     * @param spec the {@link UpdateBySpec} that defines the operation to perform
     * @param columns the columns to apply the operation to.
     * @return a {@link ColumnUpdateOperation} that will be used to construct operations for each column
     */
    static ColumnUpdateOperation of(final UpdateBySpec spec, final String... columns) {
        return spec.clause(columns);
    }

    /**
     * Conjoin an {@link UpdateBySpec} with columns for it to be applied to so the engine can construct the proper
     * operators.
     *
     * @param spec the {@link UpdateBySpec} that defines the operation to perform
     * @param columns the columns to apply the operation to.
     * @return a {@link ColumnUpdateOperation} that will be used to construct operations for each column
     */
    static ColumnUpdateOperation of(final UpdateBySpec spec, final Pair... columns) {
        return spec.clause(columns);
    }

    /**
     * Create an {@link CumSumSpec cumulative sum} for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation CumSum(String... pairs) {
        return CumSumSpec.of().clause(pairs);
    }

    /**
     * Create an {@link CumProdSpec cumulative produce} for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation CumProd(String... pairs) {
        return CumProdSpec.of().clause(pairs);
    }

    /**
     * Create an {@link CumMinMaxSpec cumulative minimum} for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation CumMin(String... pairs) {
        return CumMinMaxSpec.of(false).clause(pairs);
    }

    /**
     * Create an {@link CumMinMaxSpec cumulative maximum} for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation CumMax(String... pairs) {
        return CumMinMaxSpec.of(true).clause(pairs);
    }

    /**
     * Create an {@link FillBySpec fill by} for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Fill(String... pairs) {
        return FillBySpec.of().clause(pairs);
    }

    /**
     * Create an {@link EmaSpec exponential moving average} for the supplied column name pairs, using ticks as the decay
     * unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / timeScaleTicks)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param timeScaleTicks the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(long timeScaleTicks, String... pairs) {
        return EmaSpec.ofTicks(timeScaleTicks).clause(pairs);
    }

    /**
     * Create an {@link EmaSpec exponential moving average} for the supplied column name pairs, using ticks as the decay
     * unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / timeScaleTicks)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timeScaleTicks the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(final OperationControl control, long timeScaleTicks, String... pairs) {
        return EmaSpec.ofTicks(control, timeScaleTicks).clause(pairs);
    }

    /**
     * Create an {@link EmaSpec exponential moving average} for the supplied column name pairs, using time as the decay
     * unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeScaleNanos)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeScaleNanos the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(String timestampColumn, long timeScaleNanos, String... pairs) {
        return EmaSpec.ofTime(timestampColumn, timeScaleNanos).clause(pairs);
    }

    /**
     * Create an {@link EmaSpec exponential moving average} for the supplied column name pairs, using time as the decay
     * unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeScaleNanos)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeScaleNanos the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(OperationControl control, String timestampColumn, long timeScaleNanos,
            String... pairs) {
        return EmaSpec.ofTime(control, timestampColumn, timeScaleNanos).clause(pairs);
    }

    /**
     * Create an {@link EmaSpec exponential moving average} for the supplied column name pairs, using time as the decay
     * unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeScaleNanos)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param emaDuration the decay rate as {@Link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(String timestampColumn, Duration emaDuration, String... pairs) {
        return EmaSpec.ofTime(timestampColumn, emaDuration).clause(pairs);
    }

    /**
     * Create an {@link EmaSpec exponential moving average} for the supplied column name pairs, using time as the decay
     * unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeScaleNanos)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param emaDuration the decay rate as {@Link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(OperationControl control, String timestampColumn, Duration emaDuration,
            String... pairs) {
        return EmaSpec.ofTime(control, timestampColumn, emaDuration).clause(pairs);
    }

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(ColumnUpdateOperation clause);
    }
}

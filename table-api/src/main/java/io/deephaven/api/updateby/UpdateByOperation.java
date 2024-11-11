//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby;

import io.deephaven.api.Pair;
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
     * Create a {@link CumSumSpec cumulative sum} for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation CumSum(String... pairs) {
        return CumSumSpec.of().clause(pairs);
    }

    /**
     * Create a {@link CumProdSpec cumulative product} for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation CumProd(String... pairs) {
        return CumProdSpec.of().clause(pairs);
    }

    /**
     * Create a {@link CumMinMaxSpec cumulative minimum} for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation CumMin(String... pairs) {
        return CumMinMaxSpec.of(false).clause(pairs);
    }

    /**
     * Create a {@link CumMinMaxSpec cumulative maximum} for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation CumMax(String... pairs) {
        return CumMinMaxSpec.of(true).clause(pairs);
    }

    /**
     * Create a {@link FillBySpec forward fill} for the supplied column name pairs.
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
     *     a = e^(-1 / tickDecay)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param tickDecay the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(double tickDecay, String... pairs) {
        return EmaSpec.ofTicks(tickDecay).clause(pairs);
    }

    /**
     * Create an {@link EmaSpec exponential moving average} for the supplied column name pairs, using ticks as the decay
     * unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / tickDecay)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param tickDecay the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(final OperationControl control, double tickDecay, String... pairs) {
        return EmaSpec.ofTicks(control, tickDecay).clause(pairs);
    }

    /**
     * Create an {@link EmaSpec exponential moving average} for the supplied column name pairs, using time as the decay
     * unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeDecay)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeDecay the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(String timestampColumn, long timeDecay, String... pairs) {
        return EmaSpec.ofTime(timestampColumn, timeDecay).clause(pairs);
    }

    /**
     * Create an {@link EmaSpec exponential moving average} for the supplied column name pairs, using time as the decay
     * unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeDecay)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeDecay the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(OperationControl control, String timestampColumn, long timeDecay, String... pairs) {
        return EmaSpec.ofTime(control, timestampColumn, timeDecay).clause(pairs);
    }

    /**
     * Create an {@link EmaSpec exponential moving average} for the supplied column name pairs, using time as the decay
     * unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / durationDecay)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param durationDecay the decay rate as {@link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(String timestampColumn, Duration durationDecay, String... pairs) {
        return EmaSpec.ofTime(timestampColumn, durationDecay).clause(pairs);
    }

    /**
     * Create an {@link EmaSpec exponential moving average} for the supplied column name pairs, using time as the decay
     * unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / durationDecay)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param durationDecay the decay rate as {@link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ema(OperationControl control, String timestampColumn, Duration durationDecay,
            String... pairs) {
        return EmaSpec.ofTime(control, timestampColumn, durationDecay).clause(pairs);
    }

    /**
     * Create an {@link EmsSpec exponential moving sum} for the supplied column name pairs, using ticks as the decay
     * unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / tickDecay)
     *     ems_next = a * ems_last + value
     * </pre>
     *
     * @param tickDecay the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ems(double tickDecay, String... pairs) {
        return EmsSpec.ofTicks(tickDecay).clause(pairs);
    }

    /**
     * Create an {@link EmsSpec exponential moving sum} for the supplied column name pairs, using ticks as the decay
     * unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / tickDecay)
     *     ems_next = a * ems_last + value
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param tickDecay the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ems(final OperationControl control, double tickDecay, String... pairs) {
        return EmsSpec.ofTicks(control, tickDecay).clause(pairs);
    }

    /**
     * Create an {@link EmsSpec exponential moving sum} for the supplied column name pairs, using time as the decay
     * unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeDecay)
     *     ems_next = a * ems_last + value
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeDecay the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ems(String timestampColumn, long timeDecay, String... pairs) {
        return EmsSpec.ofTime(timestampColumn, timeDecay).clause(pairs);
    }

    /**
     * Create an {@link EmsSpec exponential moving sum} for the supplied column name pairs, using time as the decay
     * unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeDecay)
     *     ems_next = a * ems_last + value
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeDecay the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ems(OperationControl control, String timestampColumn, long timeDecay, String... pairs) {
        return EmsSpec.ofTime(control, timestampColumn, timeDecay).clause(pairs);
    }

    /**
     * Create an {@link EmsSpec exponential moving sum} for the supplied column name pairs, using time as the decay
     * unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / durationDecay)
     *     ems_next = a * ems_last + value
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param durationDecay the decay rate as {@link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ems(String timestampColumn, Duration durationDecay, String... pairs) {
        return EmsSpec.ofTime(timestampColumn, durationDecay).clause(pairs);
    }

    /**
     * Create an {@link EmsSpec exponential moving sum} for the supplied column name pairs, using time as the decay
     * unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / durationDecay)
     *     ems_next = a * ems_last + value
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param durationDecay the decay rate as {@link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Ems(OperationControl control, String timestampColumn, Duration durationDecay,
            String... pairs) {
        return EmsSpec.ofTime(control, timestampColumn, durationDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving minimum} for the supplied column name pairs, using ticks as the
     * decay unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / tickDecay)
     *     em_val_next = min(a * em_val_last, value)
     * </pre>
     *
     * @param tickDecay the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMin(double tickDecay, String... pairs) {
        return EmMinMaxSpec.ofTicks(false, tickDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving minimum} for the supplied column name pairs, using ticks as the
     * decay unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / tickDecay)
     *     em_val_next = min(a * em_val_last, value)
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param tickDecay the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMin(final OperationControl control, double tickDecay, String... pairs) {
        return EmMinMaxSpec.ofTicks(control, false, tickDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving minimum} for the supplied column name pairs, using time as the
     * decay unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeDecay)
     *     em_val_next = min(a * em_val_last, value)
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeDecay the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMin(String timestampColumn, long timeDecay, String... pairs) {
        return EmMinMaxSpec.ofTime(false, timestampColumn, timeDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving minimum} for the supplied column name pairs, using time as the
     * decay unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeDecay)
     *     em_val_next = min(a * em_val_last, value)
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeDecay the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMin(OperationControl control, String timestampColumn, long timeDecay, String... pairs) {
        return EmMinMaxSpec.ofTime(control, false, timestampColumn, timeDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving minimum} for the supplied column name pairs, using time as the
     * decay unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / durationDecay)
     *     em_val_next = min(a * em_val_last, value)
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param durationDecay the decay rate as {@link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMin(String timestampColumn, Duration durationDecay, String... pairs) {
        return EmMinMaxSpec.ofTime(false, timestampColumn, durationDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving minimum} for the supplied column name pairs, using time as the
     * decay unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / durationDecay)
     *     em_val_next = min(a * em_val_last, value)
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param durationDecay the decay rate as {@link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMin(OperationControl control, String timestampColumn, Duration durationDecay,
            String... pairs) {
        return EmMinMaxSpec.ofTime(control, false, timestampColumn, durationDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving maximum} for the supplied column name pairs, using ticks as the
     * decay unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / tickDecay)
     *     em_val_next = max(a * em_val_last, value)
     * </pre>
     *
     * @param tickDecay the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMax(double tickDecay, String... pairs) {
        return EmMinMaxSpec.ofTicks(true, tickDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving maximum} for the supplied column name pairs, using ticks as the
     * decay unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / tickDecay)
     *     em_val_next = max(a * em_val_last, value)
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param tickDecay the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMax(final OperationControl control, double tickDecay, String... pairs) {
        return EmMinMaxSpec.ofTicks(control, true, tickDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving maximum} for the supplied column name pairs, using time as the
     * decay unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeDecay)
     *     em_val_next = max(a * em_val_last, value)
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeDecay the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMax(String timestampColumn, long timeDecay, String... pairs) {
        return EmMinMaxSpec.ofTime(true, timestampColumn, timeDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving maximum} for the supplied column name pairs, using time as the
     * decay unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeDecay)
     *     em_val_next = max(a * em_val_last, value)
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeDecay the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMax(OperationControl control, String timestampColumn, long timeDecay, String... pairs) {
        return EmMinMaxSpec.ofTime(control, true, timestampColumn, timeDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving maximum} for the supplied column name pairs, using time as the
     * decay unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / durationDecay)
     *     em_val_next = max(a * em_val_last, value)
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param durationDecay the decay rate as {@link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMax(String timestampColumn, Duration durationDecay, String... pairs) {
        return EmMinMaxSpec.ofTime(true, timestampColumn, durationDecay).clause(pairs);
    }

    /**
     * Create an {@link EmMinMaxSpec exponential moving maximum} for the supplied column name pairs, using time as the
     * decay unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / durationDecay)
     *     em_val_next = max(a * em_val_last, value)
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param durationDecay the decay rate as {@link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmMax(OperationControl control, String timestampColumn, Duration durationDecay,
            String... pairs) {
        return EmMinMaxSpec.ofTime(control, true, timestampColumn, durationDecay).clause(pairs);
    }

    /**
     * Create an {@link EmStdSpec exponential moving standard deviation} for the supplied column name pairs, using ticks
     * as the decay unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / tickDecay)
     *     variance = a * (prevVariance + (1 - a) * (x - prevEma)^2)
     *     ema = a * prevEma + x
     *     std = sqrt(variance)
     * </pre>
     *
     * @param tickDecay the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmStd(double tickDecay, String... pairs) {
        return EmStdSpec.ofTicks(tickDecay).clause(pairs);
    }

    /**
     * Create an {@link EmStdSpec exponential moving standard deviation} for the supplied column name pairs, using ticks
     * as the decay unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / tickDecay)
     *     variance = a * (prevVariance + (1 - a) * (x - prevEma)^2)
     *     ema = a * prevEma + x
     *     std = sqrt(variance)
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param tickDecay the decay rate in ticks
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmStd(final OperationControl control, double tickDecay, String... pairs) {
        return EmStdSpec.ofTicks(control, tickDecay).clause(pairs);
    }

    /**
     * Create an {@link EmStdSpec exponential moving standard deviation} for the supplied column name pairs, using time
     * as the decay unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeDecay)
     *     variance = a * (prevVariance + (1 - a) * (x - prevEma)^2)
     *     ema = a * prevEma + x
     *     std = sqrt(variance)
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeDecay the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmStd(String timestampColumn, long timeDecay, String... pairs) {
        return EmStdSpec.ofTime(timestampColumn, timeDecay).clause(pairs);
    }

    /**
     * Create an {@link EmStdSpec exponential moving standard deviation} for the supplied column name pairs, using time
     * as the decay unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / timeDecay)
     *     variance = a * (prevVariance + (1 - a) * (x - prevEma)^2)
     *     ema = a * prevEma + x
     *     std = sqrt(variance)
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param timeDecay the decay rate in nanoseconds
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmStd(OperationControl control, String timestampColumn, long timeDecay, String... pairs) {
        return EmStdSpec.ofTime(control, timestampColumn, timeDecay).clause(pairs);
    }

    /**
     * Create an {@link EmStdSpec exponential moving standard deviation} for the supplied column name pairs, using time
     * as the decay unit. Uses the default OperationControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / durationDecay)
     *     variance = a * (prevVariance + (1 - a) * (x - prevEma)^2)
     *     ema = a * prevEma + x
     *     std = sqrt(variance)
     * </pre>
     *
     * @param timestampColumn the column in the source table to use for timestamps
     * @param durationDecay the decay rate as {@link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmStd(String timestampColumn, Duration durationDecay, String... pairs) {
        return EmStdSpec.ofTime(timestampColumn, durationDecay).clause(pairs);
    }

    /**
     * Create an {@link EmStdSpec exponential moving standard deviation} for the supplied column name pairs, using time
     * as the decay unit.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-dt / durationDecay)
     *     variance = a * (prevVariance + (1 - a) * (x - prevEma)^2)
     *     ema = a * prevEma + x
     *     std = sqrt(variance)
     * </pre>
     *
     * @param control a {@link OperationControl control} object that defines how special cases should behave. See
     *        {@link OperationControl} for further details.
     * @param timestampColumn the column in the source table to use for timestamps
     * @param durationDecay the decay rate as {@link Duration duration}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation EmStd(OperationControl control, String timestampColumn, Duration durationDecay,
            String... pairs) {
        return EmStdSpec.ofTime(control, timestampColumn, durationDecay).clause(pairs);
    }

    /**
     * Create a {@link DeltaSpec delta} for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Delta(String... pairs) {
        return DeltaSpec.of().clause(pairs);
    }

    /**
     * Create a {@link DeltaSpec delta} for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation Delta(DeltaControl control, String... pairs) {
        return DeltaSpec.of(control).clause(pairs);
    }

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using ticks as the windowing
     * unit. Ticks are row counts and you may specify the previous window in number of rows to include. The current row
     * is considered to belong to the reverse window, so calling this with {@code revTicks = 1} will simply return the
     * current row. Specifying {@code revTicks = 10} will include the previous 9 rows to this one and this row for a
     * total of 10 rows.
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(long revTicks, String... pairs) {
        return RollingSumSpec.ofTicks(revTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using ticks as the windowing
     * unit. Ticks are row counts and you may specify the reverse and forward window in number of rows to include. The
     * current row is considered to belong to the reverse window but not the forward window. Also, negative values are
     * allowed and can be used to generate completely forward or completely reverse windows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revTicks = 1, fwdTicks = 0} - contains only the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 0} - contains 9 previous rows and the current row</li>
     * <li>{@code revTicks = 0, fwdTicks = 10} - contains the following 10 rows, excludes the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 10} - contains the previous 9 rows, the current row and the 10 rows
     * following</li>
     * <li>{@code revTicks = 10, fwdTicks = -5} - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = 11, fwdTicks = -1} - contains 10 rows, beginning at 10 rows before, ending at 1 row before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = -5, fwdTicks = 10} - contains 5 rows, beginning 5 rows following, ending at 10 rows
     * following the current row (inclusive)</li>
     * </ul>
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param fwdTicks the look-ahead window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(long revTicks, long fwdTicks, String... pairs) {
        return RollingSumSpec.ofTicks(revTicks, fwdTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using time as the windowing unit.
     * This function accepts {@link Duration duration} as the reverse window parameter. A row containing a {@code null}
     * in the timestamp column belongs to no window and will not have a value computed or be considered in the windows
     * of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m} - contains rows from 10m earlier through the current row timestamp (inclusive)</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(String timestampCol, Duration revDuration, String... pairs) {
        return RollingSumSpec.ofTime(timestampCol, revDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using time as the windowing unit.
     * This function accepts {@link Duration durations} as the reverse and forward window parameters. Negative values
     * are allowed and can be used to generate completely forward or completely reverse windows. A row containing a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m, fwdDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m, fwdDuration = 0m} - contains rows from 10m earlier through the current row
     * timestamp (inclusive)</li>
     * <li>{@code revDuration = 0m, fwdDuration = 10m} - contains rows from the current row through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = 10m} - contains rows from 10m earlier through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = -5m} - contains rows from 10m earlier through 5m before the current
     * row timestamp (inclusive), this is a purely backwards looking window</li>
     * <li>{@code revDuration = -5m, fwdDuration = 10m} - contains rows from 5m following through 10m following the
     * current row timestamp (inclusive), this is a purely forwards looking window</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param fwdDuration the look-ahead window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(String timestampCol, Duration revDuration, Duration fwdDuration,
            String... pairs) {
        return RollingSumSpec.ofTime(timestampCol, revDuration, fwdDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using time as the windowing unit.
     * This function accepts {@code nanoseconds} as the reverse window parameters. A row containing a {@code null} in
     * the timestamp column belongs to no window and will not have a value computed or be considered in the windows of
     * other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(String timestampCol, long revTime, String... pairs) {
        return RollingSumSpec.ofTime(timestampCol, revTime).clause(pairs);
    }

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using time as the windowing unit.
     * This function accepts {@code nanoseconds} as the reverse and forward window parameters. Negative values are
     * allowed and can be used to generate completely forward or completely reverse windows. A row containing a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param fwdTime the look-ahead window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(String timestampCol, long revTime, long fwdTime, String... pairs) {
        return RollingSumSpec.ofTime(timestampCol, revTime, fwdTime).clause(pairs);
    }

    /**
     * Create {@link RollingGroupSpec rolling groups} for the supplied column name pairs, using ticks as the windowing
     * unit. Uses the default OperationControl settings.
     *
     * @param prevTimeTicks the look-behind window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingGroup(long prevTimeTicks, String... pairs) {
        return RollingGroupSpec.ofTicks(prevTimeTicks).clause(pairs);
    }

    /**
     * Create {@link RollingGroupSpec rolling groups} for the supplied column name pairs, using ticks as the windowing
     * unit. Uses the default OperationControl settings.
     *
     * @param prevTimeTicks the look-behind window size (in rows/ticks)
     * @param fwdTimeTicks the look-ahead window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingGroup(long prevTimeTicks, long fwdTimeTicks, String... pairs) {
        return RollingGroupSpec.ofTicks(prevTimeTicks, fwdTimeTicks).clause(pairs);
    }

    /**
     * Create {@link RollingGroupSpec rolling groups} for the supplied column name pairs, using time as the windowing
     * unit. Uses the default OperationControl settings.
     *
     * @param prevWindowDuration the look-behind window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingGroup(String timestampCol, Duration prevWindowDuration, String... pairs) {
        return RollingGroupSpec.ofTime(timestampCol, prevWindowDuration).clause(pairs);
    }

    /**
     * Create {@link RollingGroupSpec rolling groups} for the supplied column name pairs, using time as the windowing
     * unit. Uses the default OperationControl settings.
     *
     * @param prevWindowDuration the look-behind window size (in Duration)
     * @param fwdWindowDuration the look-ahead window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingGroup(String timestampCol, Duration prevWindowDuration, Duration fwdWindowDuration,
            String... pairs) {
        return RollingGroupSpec.ofTime(timestampCol, prevWindowDuration, fwdWindowDuration).clause(pairs);
    }

    /**
     * Create {@link RollingGroupSpec rolling groups} for the supplied column name pairs, using time as the windowing
     * unit. Uses the default OperationControl settings.
     *
     * @param prevWindowNanos the look-behind window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingGroup(String timestampCol, long prevWindowNanos, String... pairs) {
        return RollingGroupSpec.ofTime(timestampCol, prevWindowNanos).clause(pairs);
    }

    /**
     * Create {@link RollingGroupSpec rolling groups} for the supplied column name pairs, using time as the windowing
     * unit. Uses the default OperationControl settings.
     *
     * @param prevWindowNanos the look-behind window size (in nanoseconds)
     * @param fwdWindowNanos the look-ahead window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingGroup(String timestampCol, long prevWindowNanos, long fwdWindowNanos,
            String... pairs) {
        return RollingGroupSpec.ofTime(timestampCol, prevWindowNanos, fwdWindowNanos).clause(pairs);
    }


    /**
     * Create a {@link RollingAvgSpec rolling average} for the supplied column name pairs, using ticks as the windowing
     * unit. Ticks are row counts and you may specify the previous window in number of rows to include. The current row
     * is considered to belong to the reverse window, so calling this with {@code revTicks = 1} will simply return the
     * current row. Specifying {@code revTicks = 10} will include the previous 9 rows to this one and this row for a
     * total of 10 rows.
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingAvg(long revTicks, String... pairs) {
        return RollingAvgSpec.ofTicks(revTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingAvgSpec rolling average} for the supplied column name pairs, using ticks as the windowing
     * unit. Ticks are row counts and you may specify the reverse and forward window in number of rows to include. The
     * current row is considered to belong to the reverse window but not the forward window. Also, negative values are
     * allowed and can be used to generate completely forward or completely reverse windows. Here are some examples of
     * window values:
     * <ul>
     * <li>{@code revTicks = 1, fwdTicks = 0} - contains only the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 0} - contains 9 previous rows and the current row</li>
     * <li>{@code revTicks = 0, fwdTicks = 10} - contains the following 10 rows, excludes the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 10} - contains the previous 9 rows, the current row and the 10 rows
     * following</li>
     * <li>{@code revTicks = 10, fwdTicks = -5} - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = 11, fwdTicks = -1} - contains 10 rows, beginning at 10 rows before, ending at 1 row before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = -5, fwdTicks = 10} - contains 5 rows, beginning 5 rows following, ending at 10 rows
     * following the current row (inclusive)</li>
     * </ul>
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param fwdTicks the look-ahead window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingAvg(long revTicks, long fwdTicks, String... pairs) {
        return RollingAvgSpec.ofTicks(revTicks, fwdTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingAvgSpec rolling average} for the supplied column name pairs, using time as the windowing
     * unit. This function accepts {@link Duration duration} as the reverse window parameter. A row containing a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     *
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m} - contains rows from 10m earlier through the current row timestamp (inclusive)</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingAvg(String timestampCol, Duration revDuration, String... pairs) {
        return RollingAvgSpec.ofTime(timestampCol, revDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingAvgSpec rolling average} for the supplied column name pairs, using time as the windowing
     * unit. This function accepts {@link Duration durations} as the reverse and forward window parameters. Negative
     * values are allowed and can be used to generate completely forward or completely reverse windows. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     *
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m, fwdDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m, fwdDuration = 0m} - contains rows from 10m earlier through the current row
     * timestamp (inclusive)</li>
     * <li>{@code revDuration = 0m, fwdDuration = 10m} - contains rows from the current row through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = 10m} - contains rows from 10m earlier through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = -5m} - contains rows from 10m earlier through 5m before the current
     * row timestamp (inclusive), this is a purely backwards looking window</li>
     * <li>{@code revDuration = -5m, fwdDuration = 10m} - contains rows from 5m following through 10m following the
     * current row timestamp (inclusive), this is a purely forwards looking window</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param fwdDuration the look-ahead window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingAvg(String timestampCol, Duration revDuration, Duration fwdDuration,
            String... pairs) {
        return RollingAvgSpec.ofTime(timestampCol, revDuration, fwdDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingAvgSpec rolling average} for the supplied column name pairs, using time as the windowing
     * unit. This function accepts {@code nanoseconds} as the reverse window parameters. A row containing a {@code null}
     * in the timestamp column belongs to no window and will not have a value computed or be considered in the windows
     * of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingAvg(String timestampCol, long revTime, String... pairs) {
        return RollingAvgSpec.ofTime(timestampCol, revTime).clause(pairs);
    }

    /**
     * Create a {@link RollingAvgSpec rolling average} for the supplied column name pairs, using time as the windowing
     * unit. This function accepts {@code nanoseconds} as the reverse and forward window parameters. Negative values are
     * allowed and can be used to generate completely forward or completely reverse windows. A row containing a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param fwdTime the look-ahead window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingAvg(String timestampCol, long revTime, long fwdTime, String... pairs) {
        return RollingAvgSpec.ofTime(timestampCol, revTime, fwdTime).clause(pairs);
    }


    /**
     * Create a {@link RollingMinMaxSpec rolling minimum} for the supplied column name pairs, using ticks as the
     * windowing unit. Ticks are row counts and you may specify the previous window in number of rows to include. The
     * current row is considered to belong to the reverse window, so calling this with {@code revTicks = 1} will simply
     * return the current row. Specifying {@code revTicks = 10} will include the previous 9 rows to this one and this
     * row for a total of 10 rows.
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMin(long revTicks, String... pairs) {
        return RollingMinMaxSpec.ofTicks(false, revTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingMinMaxSpec rolling minimum} for the supplied column name pairs, using ticks as the
     * windowing unit. Ticks are row counts and you may specify the reverse and forward window in number of rows to
     * include. The current row is considered to belong to the reverse window but not the forward window. Also, negative
     * values are allowed and can be used to generate completely forward or completely reverse windows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revTicks = 1, fwdTicks = 0} - contains only the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 0} - contains 9 previous rows and the current row</li>
     * <li>{@code revTicks = 0, fwdTicks = 10} - contains the following 10 rows, excludes the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 10} - contains the previous 9 rows, the current row and the 10 rows
     * following</li>
     * <li>{@code revTicks = 10, fwdTicks = -5} - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = 11, fwdTicks = -1} - contains 10 rows, beginning at 10 rows before, ending at 1 row before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = -5, fwdTicks = 10} - contains 5 rows, beginning 5 rows following, ending at 10 rows
     * following the current row (inclusive)</li>
     * </ul>
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param fwdTicks the look-ahead window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMin(long revTicks, long fwdTicks, String... pairs) {
        return RollingMinMaxSpec.ofTicks(false, revTicks, fwdTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingMinMaxSpec rolling minimum} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@link Duration duration} as the reverse window parameter. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m} - contains rows from 10m earlier through the current row timestamp (inclusive)</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMin(String timestampCol, Duration revDuration, String... pairs) {
        return RollingMinMaxSpec.ofTime(false, timestampCol, revDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingMinMaxSpec rolling minimum} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@link Duration durations} as the reverse and forward window parameters.
     * Negative values are allowed and can be used to generate completely forward or completely reverse windows. A row
     * containing a {@code null} in the timestamp column belongs to no window and will not have a value computed or be
     * considered in the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m, fwdDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m, fwdDuration = 0m} - contains rows from 10m earlier through the current row
     * timestamp (inclusive)</li>
     * <li>{@code revDuration = 0m, fwdDuration = 10m} - contains rows from the current row through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = 10m} - contains rows from 10m earlier through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = -5m} - contains rows from 10m earlier through 5m before the current
     * row timestamp (inclusive), this is a purely backwards looking window</li>
     * <li>{@code revDuration = -5m, fwdDuration = 10m} - contains rows from 5m following through 10m following the
     * current row timestamp (inclusive), this is a purely forwards looking window</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param fwdDuration the look-ahead window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMin(String timestampCol, Duration revDuration, Duration fwdDuration,
            String... pairs) {
        return RollingMinMaxSpec.ofTime(false, timestampCol, revDuration, fwdDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingMinMaxSpec rolling minimum} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@code nanoseconds} as the reverse window parameters. A row containing a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMin(String timestampCol, long revTime, String... pairs) {
        return RollingMinMaxSpec.ofTime(false, timestampCol, revTime).clause(pairs);
    }

    /**
     * Create a {@link RollingMinMaxSpec rolling minimum} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@code nanoseconds} as the reverse and forward window parameters. Negative
     * values are allowed and can be used to generate completely forward or completely reverse windows. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param fwdTime the look-ahead window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMin(String timestampCol, long revTime, long fwdTime, String... pairs) {
        return RollingMinMaxSpec.ofTime(false, timestampCol, revTime, fwdTime).clause(pairs);
    }

    /**
     * Create a {@link RollingMinMaxSpec rolling maximum} for the supplied column name pairs, using ticks as the
     * windowing unit. Ticks are row counts and you may specify the previous window in number of rows to include. The
     * current row is considered to belong to the reverse window, so calling this with {@code revTicks = 1} will simply
     * return the current row. Specifying {@code revTicks = 10} will include the previous 9 rows to this one and this
     * row for a total of 10 rows.
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMax(long revTicks, String... pairs) {
        return RollingMinMaxSpec.ofTicks(true, revTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingMinMaxSpec rolling maximum} for the supplied column name pairs, using ticks as the
     * windowing unit. Ticks are row counts and you may specify the reverse and forward window in number of rows to
     * include. The current row is considered to belong to the reverse window but not the forward window. Also, negative
     * values are allowed and can be used to generate completely forward or completely reverse windows. Here are some
     * examples of window values:
     * <ul>
     * <li>{@code revTicks = 1, fwdTicks = 0} - contains only the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 0} - contains 9 previous rows and the current row</li>
     * <li>{@code revTicks = 0, fwdTicks = 10} - contains the following 10 rows, excludes the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 10} - contains the previous 9 rows, the current row and the 10 rows
     * following</li>
     * <li>{@code revTicks = 10, fwdTicks = -5} - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = 11, fwdTicks = -1} - contains 10 rows, beginning at 10 rows before, ending at 1 row before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = -5, fwdTicks = 10} - contains 5 rows, beginning 5 rows following, ending at 10 rows
     * following the current row (inclusive)</li>
     * </ul>
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param fwdTicks the look-ahead window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMax(long revTicks, long fwdTicks, String... pairs) {
        return RollingMinMaxSpec.ofTicks(true, revTicks, fwdTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingMinMaxSpec rolling maximum} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@link Duration duration} as the reverse window parameter. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m} - contains rows from 10m earlier through the current row timestamp (inclusive)</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMax(String timestampCol, Duration revDuration, String... pairs) {
        return RollingMinMaxSpec.ofTime(true, timestampCol, revDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingMinMaxSpec rolling maximum} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@link Duration durations} as the reverse and forward window parameters.
     * Negative values are allowed and can be used to generate completely forward or completely reverse windows. A row
     * containing a {@code null} in the timestamp column belongs to no window and will not have a value computed or be
     * considered in the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m, fwdDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m, fwdDuration = 0m} - contains rows from 10m earlier through the current row
     * timestamp (inclusive)</li>
     * <li>{@code revDuration = 0m, fwdDuration = 10m} - contains rows from the current row through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = 10m} - contains rows from 10m earlier through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = -5m} - contains rows from 10m earlier through 5m before the current
     * row timestamp (inclusive), this is a purely backwards looking window</li>
     * <li>{@code revDuration = -5m, fwdDuration = 10m} - contains rows from 5m following through 10m following the
     * current row timestamp (inclusive), this is a purely forwards looking window</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param fwdDuration the look-ahead window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMax(String timestampCol, Duration revDuration, Duration fwdDuration,
            String... pairs) {
        return RollingMinMaxSpec.ofTime(true, timestampCol, revDuration, fwdDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingMinMaxSpec rolling maximum} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@code nanoseconds} as the reverse window parameters. A row containing a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMax(String timestampCol, long revTime, String... pairs) {
        return RollingMinMaxSpec.ofTime(true, timestampCol, revTime).clause(pairs);
    }

    /**
     * Create a {@link RollingMinMaxSpec rolling maximum} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@code nanoseconds} as the reverse and forward window parameters. Negative
     * values are allowed and can be used to generate completely forward or completely reverse windows. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param fwdTime the look-ahead window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingMax(String timestampCol, long revTime, long fwdTime, String... pairs) {
        return RollingMinMaxSpec.ofTime(true, timestampCol, revTime, fwdTime).clause(pairs);
    }


    /**
     * Create a {@link RollingProductSpec rolling product} for the supplied column name pairs, using ticks as the
     * windowing unit. Ticks are row counts and you may specify the reverse and forward window in number of rows to
     * include. The current row is considered to belong to the reverse window but not the forward window. Also, negative
     * values are allowed and can be used to generate completely forward or completely reverse windows. Here are some
     * examples of window values:
     * <ul>
     * <li>{@code revTicks = 1, fwdTicks = 0} - contains only the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 0} - contains 9 previous rows and the current row</li>
     * <li>{@code revTicks = 0, fwdTicks = 10} - contains the following 10 rows, excludes the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 10} - contains the previous 9 rows, the current row and the 10 rows
     * following</li>
     * <li>{@code revTicks = 10, fwdTicks = -5} - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = 11, fwdTicks = -1} - contains 10 rows, beginning at 10 rows before, ending at 1 row before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = -5, fwdTicks = 10} - contains 5 rows, beginning 5 rows following, ending at 10 rows
     * following the current row (inclusive)</li>
     * </ul>
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param fwdTicks the look-ahead window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingProduct(long revTicks, long fwdTicks, String... pairs) {
        return RollingProductSpec.ofTicks(revTicks, fwdTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingProductSpec rolling product} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@link Duration duration} as the reverse window parameter. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     *
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m} - contains rows from 10m earlier through the current row timestamp (inclusive)</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingProduct(String timestampCol, Duration revDuration, String... pairs) {
        return RollingProductSpec.ofTime(timestampCol, revDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingProductSpec rolling product} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@link Duration durations} as the reverse and forward window parameters.
     * Negative values are allowed and can be used to generate completely forward or completely reverse windows. A row
     * containing a {@code null} in the timestamp column belongs to no window and will not have a value computed or be
     * considered in the windows of other rows.
     *
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m, fwdDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m, fwdDuration = 0m} - contains rows from 10m earlier through the current row
     * timestamp (inclusive)</li>
     * <li>{@code revDuration = 0m, fwdDuration = 10m} - contains rows from the current row through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = 10m} - contains rows from 10m earlier through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = -5m} - contains rows from 10m earlier through 5m before the current
     * row timestamp (inclusive), this is a purely backwards looking window</li>
     * <li>{@code revDuration = -5m, fwdDuration = 10m} - contains rows from 5m following through 10m following the
     * current row timestamp (inclusive), this is a purely forwards looking window</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param fwdDuration the look-ahead window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingProduct(String timestampCol, Duration revDuration, Duration fwdDuration,
            String... pairs) {
        return RollingProductSpec.ofTime(timestampCol, revDuration, fwdDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingProductSpec rolling product} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@code nanoseconds} as the reverse window parameters. A row containing a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingProduct(String timestampCol, long revTime, String... pairs) {
        return RollingProductSpec.ofTime(timestampCol, revTime).clause(pairs);
    }

    /**
     * Create a {@link RollingProductSpec rolling product} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@code nanoseconds} as the reverse and forward window parameters. Negative
     * values are allowed and can be used to generate completely forward or completely reverse windows. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param fwdTime the look-ahead window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingProduct(String timestampCol, long revTime, long fwdTime, String... pairs) {
        return RollingProductSpec.ofTime(timestampCol, revTime, fwdTime).clause(pairs);
    }

    /**
     * Create a {@link RollingCountSpec rolling count} for the supplied column name pairs, using ticks as the windowing
     * unit. Ticks are row counts and you may specify the previous window in number of rows to include. The current row
     * is considered to belong to the reverse window, so calling this with {@code revTicks = 1} will simply return the
     * current row. Specifying {@code revTicks = 10} will include the previous 9 rows to this one and this row for a
     * total of 10 rows.
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingCount(long revTicks, String... pairs) {
        return RollingCountSpec.ofTicks(revTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingCountSpec rolling count} for the supplied column name pairs, using ticks as the windowing
     * unit. Ticks are row counts and you may specify the reverse and forward window in number of rows to include. The
     * current row is considered to belong to the reverse window but not the forward window. Also, negative values are
     * allowed and can be used to generate completely forward or completely reverse windows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revTicks = 1, fwdTicks = 0} - contains only the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 0} - contains 9 previous rows and the current row</li>
     * <li>{@code revTicks = 0, fwdTicks = 10} - contains the following 10 rows, excludes the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 10} - contains the previous 9 rows, the current row and the 10 rows
     * following</li>
     * <li>{@code revTicks = 10, fwdTicks = -5} - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = 11, fwdTicks = -1} - contains 10 rows, beginning at 10 rows before, ending at 1 row before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = -5, fwdTicks = 10} - contains 5 rows, beginning 5 rows following, ending at 10 rows
     * following the current row (inclusive)</li>
     * </ul>
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param fwdTicks the look-ahead window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingCount(long revTicks, long fwdTicks, String... pairs) {
        return RollingCountSpec.ofTicks(revTicks, fwdTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingCountSpec rolling count} for the supplied column name pairs, using time as the windowing
     * unit. This function accepts {@link Duration duration} as the reverse window parameter. A row containing a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m} - contains rows from 10m earlier through the current row timestamp (inclusive)</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingCount(String timestampCol, Duration revDuration, String... pairs) {
        return RollingCountSpec.ofTime(timestampCol, revDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingCountSpec rolling count} for the supplied column name pairs, using time as the windowing
     * unit. This function accepts {@link Duration durations} as the reverse and forward window parameters. Negative
     * values are allowed and can be used to generate completely forward or completely reverse windows. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m, fwdDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m, fwdDuration = 0m} - contains rows from 10m earlier through the current row
     * timestamp (inclusive)</li>
     * <li>{@code revDuration = 0m, fwdDuration = 10m} - contains rows from the current row through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = 10m} - contains rows from 10m earlier through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = -5m} - contains rows from 10m earlier through 5m before the current
     * row timestamp (inclusive), this is a purely backwards looking window</li>
     * <li>{@code revDuration = -5m, fwdDuration = 10m} - contains rows from 5m following through 10m following the
     * current row timestamp (inclusive), this is a purely forwards looking window</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param fwdDuration the look-ahead window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingCount(String timestampCol, Duration revDuration, Duration fwdDuration,
            String... pairs) {
        return RollingCountSpec.ofTime(timestampCol, revDuration, fwdDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingCountSpec rolling count} for the supplied column name pairs, using time as the windowing
     * unit. This function accepts {@code nanoseconds} as the reverse window parameters. A row containing a {@code null}
     * in the timestamp column belongs to no window and will not have a value computed or be considered in the windows
     * of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingCount(String timestampCol, long revTime, String... pairs) {
        return RollingCountSpec.ofTime(timestampCol, revTime).clause(pairs);
    }

    /**
     * Create a {@link RollingCountSpec rolling count} for the supplied column name pairs, using time as the windowing
     * unit. This function accepts {@code nanoseconds} as the reverse and forward window parameters. Negative values are
     * allowed and can be used to generate completely forward or completely reverse windows. A row containing a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param fwdTime the look-ahead window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingCount(String timestampCol, long revTime, long fwdTime, String... pairs) {
        return RollingCountSpec.ofTime(timestampCol, revTime, fwdTime).clause(pairs);
    }


    /**
     * Create a {@link RollingStdSpec rolling sample standard deviation} for the supplied column name pairs, using ticks
     * as the windowing unit. Ticks are row counts and you may specify the previous window in number of rows to include.
     * The current row is considered to belong to the reverse window, so calling this with {@code revTicks = 1} will
     * simply return the current row. Specifying {@code revTicks = 10} will include the previous 9 rows to this one and
     * this row for a total of 10 rows.
     *
     * Sample standard deviation is computed using Bessel's correction
     * (https://en.wikipedia.org/wiki/Bessel%27s_correction), which ensures that the sample variance will be an unbiased
     * estimator of population variance.
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingStd(long revTicks, String... pairs) {
        return RollingStdSpec.ofTicks(revTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingStdSpec rolling sample standard deviation} for the supplied column name pairs, using ticks
     * as the windowing unit. Ticks are row counts and you may specify the reverse and forward window in number of rows
     * to include. The current row is considered to belong to the reverse window but not the forward window. Also,
     * negative values are allowed and can be used to generate completely forward or completely reverse windows. Here
     * are some examples of window values:
     * <ul>
     * <li>{@code revTicks = 1, fwdTicks = 0} - contains only the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 0} - contains 9 previous rows and the current row</li>
     * <li>{@code revTicks = 0, fwdTicks = 10} - contains the following 10 rows, excludes the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 10} - contains the previous 9 rows, the current row and the 10 rows
     * following</li>
     * <li>{@code revTicks = 10, fwdTicks = -5} - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = 11, fwdTicks = -1} - contains 10 rows, beginning at 10 rows before, ending at 1 row before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = -5, fwdTicks = 10} - contains 5 rows, beginning 5 rows following, ending at 10 rows
     * following the current row (inclusive)</li>
     * </ul>
     *
     * Sample standard deviation is computed using
     * <a href="https://en.wikipedia.org/wiki/Bessel%27s_correction">Bessel's correction</a>, which ensures that the
     * sample variance will be an unbiased estimator of population variance.
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param fwdTicks the look-ahead window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingStd(long revTicks, long fwdTicks, String... pairs) {
        return RollingStdSpec.ofTicks(revTicks, fwdTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingStdSpec rolling sample standard deviation} for the supplied column name pairs, using time
     * as the windowing unit. This function accepts {@link Duration duration} as the reverse window parameter. A row
     * containing a {@code null} in the timestamp column belongs to no window and will not have a value computed or be
     * considered in the windows of other rows.
     *
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m} - contains rows from 10m earlier through the current row timestamp (inclusive)</li>
     * </ul>
     *
     * Sample standard deviation is computed using
     * <a href="https://en.wikipedia.org/wiki/Bessel%27s_correction">Bessel's correction</a>, which ensures that the
     * sample variance will be an unbiased estimator of population variance.
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingStd(String timestampCol, Duration revDuration, String... pairs) {
        return RollingStdSpec.ofTime(timestampCol, revDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingStdSpec rolling sample standard deviation} for the supplied column name pairs, using time
     * as the windowing unit. This function accepts {@link Duration durations} as the reverse and forward window
     * parameters. Negative values are allowed and can be used to generate completely forward or completely reverse
     * windows. A row containing a {@code null} in the timestamp column belongs to no window and will not have a value
     * computed or be considered in the windows of other rows.
     *
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m, fwdDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m, fwdDuration = 0m} - contains rows from 10m earlier through the current row
     * timestamp (inclusive)</li>
     * <li>{@code revDuration = 0m, fwdDuration = 10m} - contains rows from the current row through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = 10m} - contains rows from 10m earlier through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = -5m} - contains rows from 10m earlier through 5m before the current
     * row timestamp (inclusive), this is a purely backwards looking window</li>
     * <li>{@code revDuration = -5m, fwdDuration = 10m} - contains rows from 5m following through 10m following the
     * current row timestamp (inclusive), this is a purely forwards looking window</li>
     * </ul>
     *
     * Sample standard deviation is computed using Bessel's correction
     * (https://en.wikipedia.org/wiki/Bessel%27s_correction), which ensures that the sample variance will be an unbiased
     * estimator of population variance.
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param fwdDuration the look-ahead window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingStd(String timestampCol, Duration revDuration, Duration fwdDuration,
            String... pairs) {
        return RollingStdSpec.ofTime(timestampCol, revDuration, fwdDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingStdSpec rolling sample standard deviation} for the supplied column name pairs, using time
     * as the windowing unit. This function accepts {@code nanoseconds} as the reverse window parameters. A row
     * containing a {@code null} in the timestamp column belongs to no window and will not have a value computed or be
     * considered in the windows of other rows.
     *
     * Sample standard deviation is computed using Bessel's correction
     * (https://en.wikipedia.org/wiki/Bessel%27s_correction), which ensures that the sample variance will be an unbiased
     * estimator of population variance.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingStd(String timestampCol, long revTime, String... pairs) {
        return RollingStdSpec.ofTime(timestampCol, revTime).clause(pairs);
    }

    /**
     * Create a {@link RollingStdSpec rolling sample standard deviation} for the supplied column name pairs, using time
     * as the windowing unit. This function accepts {@code nanoseconds} as the reverse and forward window parameters.
     * Negative values are allowed and can be used to generate completely forward or completely reverse windows. A row
     * containing a {@code null} in the timestamp column belongs to no window and will not have a value computed or be
     * considered in the windows of other rows.
     *
     * Sample standard deviation is computed using Bessel's correction
     * (https://en.wikipedia.org/wiki/Bessel%27s_correction), which ensures that the sample variance will be an unbiased
     * estimator of population variance.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param fwdTime the look-ahead window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingStd(String timestampCol, long revTime, long fwdTime, String... pairs) {
        return RollingStdSpec.ofTime(timestampCol, revTime, fwdTime).clause(pairs);
    }


    /**
     * Create a {@link RollingWAvgSpec rolling weighted average} for the supplied column name pairs, using ticks as the
     * windowing unit. Ticks are row counts and you may specify the previous window in number of rows to include. The
     * current row is considered to belong to the reverse window, so calling this with {@code revTicks = 1} will simply
     * return the current row. Specifying {@code revTicks = 10} will include the previous 9 rows to this one and this
     * row for a total of 10 rows.
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingWAvg(long revTicks, String weightCol, String... pairs) {
        return RollingWAvgSpec.ofTicks(revTicks, weightCol).clause(pairs);
    }

    /**
     * Create a {@link RollingWAvgSpec rolling weighted average} for the supplied column name pairs, using ticks as the
     * windowing unit. Ticks are row counts and you may specify the reverse and forward window in number of rows to
     * include. The current row is considered to belong to the reverse window but not the forward window. Also, negative
     * values are allowed and can be used to generate completely forward or completely reverse windows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revTicks = 1, fwdTicks = 0} - contains only the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 0} - contains 9 previous rows and the current row</li>
     * <li>{@code revTicks = 0, fwdTicks = 10} - contains the following 10 rows, excludes the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 10} - contains the previous 9 rows, the current row and the 10 rows
     * following</li>
     * <li>{@code revTicks = 10, fwdTicks = -5} - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = 11, fwdTicks = -1} - contains 10 rows, beginning at 10 rows before, ending at 1 row before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = -5, fwdTicks = 10} - contains 5 rows, beginning 5 rows following, ending at 10 rows
     * following the current row (inclusive)</li>
     * </ul>
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param fwdTicks the look-ahead window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingWAvg(long revTicks, long fwdTicks, String weightCol, String... pairs) {
        return RollingWAvgSpec.ofTicks(revTicks, fwdTicks, weightCol).clause(pairs);
    }

    /**
     * Create a {@link RollingWAvgSpec rolling weighted average} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@link Duration duration} as the reverse window parameter. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m} - contains rows from 10m earlier through the current row timestamp (inclusive)</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingWAvg(String timestampCol, Duration revDuration, String weightCol, String... pairs) {
        return RollingWAvgSpec.ofTime(timestampCol, revDuration, weightCol).clause(pairs);
    }

    /**
     * Create a {@link RollingWAvgSpec rolling weighted average} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@link Duration durations} as the reverse and forward window parameters.
     * Negative values are allowed and can be used to generate completely forward or completely reverse windows. A row
     * containing a {@code null} in the timestamp column belongs to no window and will not have a value computed or be
     * considered in the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m, fwdDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m, fwdDuration = 0m} - contains rows from 10m earlier through the current row
     * timestamp (inclusive)</li>
     * <li>{@code revDuration = 0m, fwdDuration = 10m} - contains rows from the current row through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = 10m} - contains rows from 10m earlier through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = -5m} - contains rows from 10m earlier through 5m before the current
     * row timestamp (inclusive), this is a purely backwards looking window</li>
     * <li>{@code revDuration = -5m, fwdDuration = 10m} - contains rows from 5m following through 10m following the
     * current row timestamp (inclusive), this is a purely forwards looking window</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param fwdDuration the look-ahead window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingWAvg(String timestampCol, Duration revDuration, Duration fwdDuration,
            String weightCol, String... pairs) {
        return RollingWAvgSpec.ofTime(timestampCol, revDuration, fwdDuration, weightCol).clause(pairs);
    }

    /**
     * Create a {@link RollingWAvgSpec rolling weighted average} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@code nanoseconds} as the reverse window parameters. A row containing a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingWAvg(String timestampCol, long revTime, String weightCol, String... pairs) {
        return RollingWAvgSpec.ofTime(timestampCol, revTime, weightCol).clause(pairs);
    }

    /**
     * Create a {@link RollingWAvgSpec rolling weighted average} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@code nanoseconds} as the reverse and forward window parameters. Negative
     * values are allowed and can be used to generate completely forward or completely reverse windows. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param fwdTime the look-ahead window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingWAvg(String timestampCol, long revTime, long fwdTime, String weightCol,
            String... pairs) {
        return RollingWAvgSpec.ofTime(timestampCol, revTime, fwdTime, weightCol).clause(pairs);
    }


    /**
     * Create a {@link RollingFormulaSpec rolling formula} for the supplied column name pairs, using ticks as the
     * windowing unit. Ticks are row counts and you may specify the previous window in number of rows to include. The
     * current row is considered to belong to the reverse window, so calling this with {@code revTicks = 1} will simply
     * return the current row. Specifying {@code revTicks = 10} will include the previous 9 rows to this one and this
     * row for a total of 10 rows.
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param formula the user-defined formula to apply to each group. This formula can contain any combination of the
     *        following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     * @param paramToken the parameter name for the input column's vector within the formula. If formula is
     *        <em>max(each)</em>, then <em>each</em> is the formula_param.
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(long revTicks, String formula, String paramToken, String... pairs) {
        return RollingFormulaSpec.ofTicks(revTicks, formula, paramToken).clause(pairs);
    }

    /**
     * Create a {@link RollingFormulaSpec rolling formula} for the supplied column name pairs, using ticks as the
     * windowing unit. Ticks are row counts and you may specify the reverse and forward window in number of rows to
     * include. The current row is considered to belong to the reverse window but not the forward window. Also, negative
     * values are allowed and can be used to generate completely forward or completely reverse windows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revTicks = 1, fwdTicks = 0} - contains only the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 0} - contains 9 previous rows and the current row</li>
     * <li>{@code revTicks = 0, fwdTicks = 10} - contains the following 10 rows, excludes the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 10} - contains the previous 9 rows, the current row and the 10 rows
     * following</li>
     * <li>{@code revTicks = 10, fwdTicks = -5} - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = 11, fwdTicks = -1} - contains 10 rows, beginning at 10 rows before, ending at 1 row before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = -5, fwdTicks = 10} - contains 5 rows, beginning 5 rows following, ending at 10 rows
     * following the current row (inclusive)</li>
     * </ul>
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param fwdTicks the look-ahead window size (in rows/ticks)
     * @param formula the user-defined formula to apply to each group. This formula can contain any combination of the
     *        following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     * @param paramToken the parameter name for the input column's vector within the formula. If formula is
     *        <em>max(each)</em>, then <em>each</em> is the formula_param.
     * @param pairs the input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(long revTicks, long fwdTicks, String formula, String paramToken,
            String... pairs) {
        return RollingFormulaSpec.ofTicks(revTicks, fwdTicks, formula, paramToken).clause(pairs);
    }

    /**
     * Create a {@link RollingFormulaSpec rolling formula} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@link Duration duration} as the reverse window parameter. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m} - contains rows from 10m earlier through the current row timestamp (inclusive)</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param formula the user-defined {@link RollingFormulaSpec#formula() formula} to apply to each group. This formula
     *        can contain any combination of the following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     * @param paramToken the {@link RollingFormulaSpec#paramToken() parameter token} for the input column's vector
     *        within the formula. If formula is <em>max(each)</em>, then <em>each</em> is the formula_param.
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(String timestampCol, Duration revDuration, String formula,
            String paramToken, String... pairs) {
        return RollingFormulaSpec.ofTime(timestampCol, revDuration, formula, paramToken).clause(pairs);
    }

    /**
     * Create a {@link RollingFormulaSpec rolling formula} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@link Duration durations} as the reverse and forward window parameters.
     * Negative values are allowed and can be used to generate completely forward or completely reverse windows. A row
     * containing a {@code null} in the timestamp column belongs to no window and will not have a value computed or be
     * considered in the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m, fwdDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m, fwdDuration = 0m} - contains rows from 10m earlier through the current row
     * timestamp (inclusive)</li>
     * <li>{@code revDuration = 0m, fwdDuration = 10m} - contains rows from the current row through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = 10m} - contains rows from 10m earlier through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = -5m} - contains rows from 10m earlier through 5m before the current
     * row timestamp (inclusive), this is a purely backwards looking window</li>
     * <li>{@code revDuration = -5m, fwdDuration = 10m} - contains rows from 5m following through 10m following the
     * current row timestamp (inclusive), this is a purely forwards looking window</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param fwdDuration the look-ahead window size (in Duration)
     * @param formula the user-defined {@link RollingFormulaSpec#formula() formula} to apply to each group. This formula
     *        can contain any combination of the following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     * @param paramToken the {@link RollingFormulaSpec#paramToken() parameter token} for the input column's vector
     *        within the formula. If formula is <em>max(each)</em>, then <em>each</em> is the formula_param.
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(String timestampCol, Duration revDuration, Duration fwdDuration,
            String formula, String paramToken, String... pairs) {
        return RollingFormulaSpec.ofTime(timestampCol, revDuration, fwdDuration, formula, paramToken).clause(pairs);
    }

    /**
     * Create a {@link RollingFormulaSpec rolling formula} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@code nanoseconds} as the reverse window parameters. A row containing a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param formula the user-defined formula to apply to each group. This formula can contain any combination of the
     *        following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     * @param paramToken the parameter name for the input column's vector within the formula. If formula is
     *        <em>max(each)</em>, then <em>each</em> is the formula_param.
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(String timestampCol, long revTime, String formula, String paramToken,
            String... pairs) {
        return RollingFormulaSpec.ofTime(timestampCol, revTime, formula, paramToken).clause(pairs);
    }

    /**
     * Create a {@link RollingFormulaSpec rolling formula} for the supplied column name pairs, using time as the
     * windowing unit. This function accepts {@code nanoseconds} as the reverse and forward window parameters. Negative
     * values are allowed and can be used to generate completely forward or completely reverse windows. A row containing
     * a {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered
     * in the windows of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param fwdTime the look-ahead window size (in nanoseconds)
     * @param formula the user-defined formula to apply to each group. This formula can contain any combination of the
     *        following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     * @param paramToken the parameter name for the input column's vector within the formula. If formula is
     *        <em>max(each)</em>, then <em>each</em> is the formula_param.
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(String timestampCol, long revTime, long fwdTime, String formula,
            String paramToken,
            String... pairs) {
        return RollingFormulaSpec.ofTime(timestampCol, revTime, fwdTime, formula, paramToken).clause(pairs);
    }

    // New methods for the multi-column formula

    /**
     * Create a {@link RollingFormulaSpec rolling formula} using ticks as the windowing unit. Ticks are row counts and
     * you may specify the previous window in number of rows to include. The current row is considered to belong to the
     * reverse window, so calling this with {@code revTicks = 1} will simply return the current row. Specifying
     * {@code revTicks = 10} will include the previous 9 rows to this one and this row for a total of 10 rows.
     * <p>
     * The provided {@code formula} should specify the output column name. Some examples of formula are:
     *
     * <pre>
     * sum_AB = sum(col_A) + sum(col_B)
     * max_AB = max(col_A) + max(col_B)
     * values = col_A + col_B
     * </pre>
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param formula the user-defined formula to apply to each group. This formula includes the output column name and
     *        can contain any combination of the following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     *
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(long revTicks, String formula) {
        return RollingFormulaSpec.ofTicks(revTicks, formula).clause();
    }

    /**
     * Create a {@link RollingFormulaSpec rolling formula} using ticks as the windowing unit. Ticks are row counts and
     * you may specify the reverse and forward window in number of rows to include. The current row is considered to
     * belong to the reverse window but not the forward window. Also, negative values are allowed and can be used to
     * generate completely forward or completely reverse windows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revTicks = 1, fwdTicks = 0} - contains only the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 0} - contains 9 previous rows and the current row</li>
     * <li>{@code revTicks = 0, fwdTicks = 10} - contains the following 10 rows, excludes the current row</li>
     * <li>{@code revTicks = 10, fwdTicks = 10} - contains the previous 9 rows, the current row and the 10 rows
     * following</li>
     * <li>{@code revTicks = 10, fwdTicks = -5} - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = 11, fwdTicks = -1} - contains 10 rows, beginning at 10 rows before, ending at 1 row before
     * the current row (inclusive)</li>
     * <li>{@code revTicks = -5, fwdTicks = 10} - contains 5 rows, beginning 5 rows following, ending at 10 rows
     * following the current row (inclusive)</li>
     * </ul>
     *
     * <p>
     * The provided {@code formula} should specify the output column name. Some examples of formula are:
     *
     * <pre>
     * sum_AB = sum(col_A) + sum(col_B)
     * max_AB = max(col_A) + max(col_B)
     * values = col_A + col_B
     * </pre>
     *
     * @param revTicks the look-behind window size (in rows/ticks)
     * @param fwdTicks the look-ahead window size (in rows/ticks)
     * @param formula the user-defined formula to apply to each group. This formula can contain any combination of the
     *        following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(long revTicks, long fwdTicks, String formula) {
        return RollingFormulaSpec.ofTicks(revTicks, fwdTicks, formula).clause();
    }

    /**
     * Create a {@link RollingFormulaSpec rolling formula} using time as the windowing unit. This function accepts
     * {@link Duration duration} as the reverse window parameter. A row containing a {@code null} in the timestamp
     * column belongs to no window and will not have a value computed or be considered in the windows of other rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m} - contains rows from 10m earlier through the current row timestamp (inclusive)</li>
     * </ul>
     *
     * <p>
     * The provided {@code formula} should specify the output column name. Some examples of formula are:
     *
     * <pre>
     * sum_AB = sum(col_A) + sum(col_B)
     * max_AB = max(col_A) + max(col_B)
     * values = col_A + col_B
     * </pre>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param formula the user-defined {@link RollingFormulaSpec#formula() formula} to apply to each group. This formula
     *        can contain any combination of the following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(String timestampCol, Duration revDuration, String formula) {
        return RollingFormulaSpec.ofTime(timestampCol, revDuration, formula).clause();
    }

    /**
     * Create a {@link RollingFormulaSpec rolling formula} using time as the windowing unit. This function accepts
     * {@link Duration durations} as the reverse and forward window parameters. Negative values are allowed and can be
     * used to generate completely forward or completely reverse windows. A row containing a {@code null} in the
     * timestamp column belongs to no window and will not have a value computed or be considered in the windows of other
     * rows.
     * <p>
     * Here are some examples of window values:
     * <ul>
     * <li>{@code revDuration = 0m, fwdDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code revDuration = 10m, fwdDuration = 0m} - contains rows from 10m earlier through the current row
     * timestamp (inclusive)</li>
     * <li>{@code revDuration = 0m, fwdDuration = 10m} - contains rows from the current row through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = 10m} - contains rows from 10m earlier through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code revDuration = 10m, fwdDuration = -5m} - contains rows from 10m earlier through 5m before the current
     * row timestamp (inclusive), this is a purely backwards looking window</li>
     * <li>{@code revDuration = -5m, fwdDuration = 10m} - contains rows from 5m following through 10m following the
     * current row timestamp (inclusive), this is a purely forwards looking window</li>
     * </ul>
     *
     * <p>
     * The provided {@code formula} should specify the output column name. Some examples of formula are:
     *
     * <pre>
     * sum_AB = sum(col_A) + sum(col_B)
     * max_AB = max(col_A) + max(col_B)
     * values = col_A + col_B
     * </pre>
     *
     * @param timestampCol the name of the timestamp column
     * @param revDuration the look-behind window size (in Duration)
     * @param fwdDuration the look-ahead window size (in Duration)
     * @param formula the user-defined {@link RollingFormulaSpec#formula() formula} to apply to each group. This formula
     *        can contain any combination of the following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(String timestampCol, Duration revDuration, Duration fwdDuration,
            String formula) {
        return RollingFormulaSpec.ofTime(timestampCol, revDuration, fwdDuration, formula).clause();
    }

    /**
     * Create a {@link RollingFormulaSpec rolling formula} using time as the windowing unit. This function accepts
     * {@code nanoseconds} as the reverse window parameters. A row containing a {@code null} in the timestamp column
     * belongs to no window and will not have a value computed or be considered in the windows of other rows.
     *
     * <p>
     * The provided {@code formula} should specify the output column name. Some examples of formula are:
     *
     * <pre>
     * sum_AB = sum(col_A) + sum(col_B)
     * max_AB = max(col_A) + max(col_B)
     * values = col_A + col_B
     * </pre>
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param formula the user-defined formula to apply to each group. This formula can contain any combination of the
     *        following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(String timestampCol, long revTime, String formula) {
        return RollingFormulaSpec.ofTime(timestampCol, revTime, formula).clause();
    }

    /**
     * Create a {@link RollingFormulaSpec rolling formula} using time as the windowing unit. This function accepts
     * {@code nanoseconds} as the reverse and forward window parameters. Negative values are allowed and can be used to
     * generate completely forward or completely reverse windows. A row containing a {@code null} in the timestamp
     * column belongs to no window and will not have a value computed or be considered in the windows of other rows.
     *
     * <p>
     * The provided {@code formula} should specify the output column name. Some examples of formula are:
     *
     * <pre>
     * sum_AB = sum(col_A) + sum(col_B)
     * max_AB = max(col_A) + max(col_B)
     * values = col_A + col_B
     * </pre>
     *
     * @param timestampCol the name of the timestamp column
     * @param revTime the look-behind window size (in nanoseconds)
     * @param fwdTime the look-ahead window size (in nanoseconds)
     * @param formula the user-defined formula to apply to each group. This formula can contain any combination of the
     *        following:
     *        <ul>
     *        <li>Built-in functions such as min, max, etc.</li>
     *        <li>Mathematical arithmetic such as *, +, /, etc.</li>
     *        <li>User-defined functions</li>
     *        </ul>
     * @return The aggregation
     */
    static UpdateByOperation RollingFormula(String timestampCol, long revTime, long fwdTime, String formula) {
        return RollingFormulaSpec.ofTime(timestampCol, revTime, fwdTime, formula).clause();
    }


    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(ColumnUpdateOperation clause);
    }
}

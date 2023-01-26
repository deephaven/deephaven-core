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

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using ticks as the windowing
     * unit. Ticks are row counts and you may specify the previous window in number of rows to include. The current row
     * is considered to belong to the reverse window, so calling this with {@code prevTicks = 1} will simply return the
     * current row. Specifying {@code prevTicks = 10} will include the previous 9 rows to this one and this row for a
     * total of 10 rows.
     *
     * @param prevTicks the look-behind window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(long prevTicks, String... pairs) {
        return RollingSumSpec.ofTicks(prevTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using ticks as the windowing
     * unit. Ticks are row counts and you may specify the previous and forward window in number of rows to include. The
     * current row is considered to belong to the reverse window but not the forward window. Also, negative values are
     * allowed and can be used to generate completely forward or completely reverse windows. Here are some examples of
     * window values:
     * <ul>
     * <li>{@code prevTicks = 1, fwdTicks = 0} - contains only the current row</li>
     * <li>{@code prevTicks = 10, fwdTicks = 0} - contains 9 previous rows and the current row</li>
     * <li>{@code prevTicks = 0, fwdTicks = 10} - contains the following 10 rows, excludes the current row</li>
     * <li>{@code prevTicks = 10, fwdTicks = 10} - contains the previous 9 rows, the current row and the 10 rows
     * following</li>
     * <li>{@code prevTicks = 10, fwdTicks = -5} - contains 5 rows, beginning at 9 rows before, ending at 5 rows before
     * the current row (inclusive)</li>
     * <li>{@code prevTicks = 11, fwdTicks = -1} - contains 10 rows, beginning at 10 rows before, ending at 1 row before
     * the current row (inclusive)</li>
     * <li>{@code prevTicks = -5, fwdTicks = 10} - contains 5 rows, beginning 5 rows following, ending at 10 rows
     * following the current row (inclusive)</li>
     * </ul>
     *
     * @param prevTicks the look-behind window size (in rows/ticks)
     * @param fwdTicks the look-ahead window size (in rows/ticks)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(long prevTicks, long fwdTicks, String... pairs) {
        return RollingSumSpec.ofTicks(prevTicks, fwdTicks).clause(pairs);
    }

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using time as the windowing unit.
     * This function accepts {@link Duration duration} as the prev window parameter. A row that contains a {@code null}
     * in the timestamp column belongs to no window and will not have a value computed or be considered in the windows
     * of other rows.
     *
     * Here are some examples of window values:
     * <ul>
     * <li>{@code prevDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code prevDuration = 10m} - contains rows from 10m earlier through the current row timestamp
     * (inclusive)</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param prevDuration the look-behind window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(String timestampCol, Duration prevDuration, String... pairs) {
        return RollingSumSpec.ofTime(timestampCol, prevDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using time as the windowing unit.
     * This function accepts {@link Duration durations} as the prev and forward window parameters. Negative values are
     * allowed and can be used to generate completely forward or completely reverse windows. A row that contains a
     * {@code null} in the timestamp column belongs to no window and will not have a value computed or be considered in
     * the windows of other rows.
     *
     * Here are some examples of window values:
     * <ul>
     * <li>{@code prevDuration = 0m, fwdDuration = 0m} - contains rows that exactly match the current row timestamp</li>
     * <li>{@code prevDuration = 10m, fwdDuration = 0m} - contains rows from 10m earlier through the current row
     * timestamp (inclusive)</li>
     * <li>{@code prevDuration = 0m, fwdDuration = 10m} - contains rows from the current row through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code prevDuration = 10m, fwdDuration = 10m} - contains rows from 10m earlier through 10m following the
     * current row timestamp (inclusive)</li>
     * <li>{@code prevDuration = 10m, fwdDuration = -5m} - contains rows from 10m earlier through 5m before the current
     * row timestamp (inclusive), this is a purely backwards looking window</li>
     * <li>{@code prevDuration = -5m, fwdDuration = 10m} - contains rows from 5m following through 10m following the
     * current row timestamp (inclusive), this is a purely forwards looking window</li>
     * </ul>
     *
     * @param timestampCol the name of the timestamp column
     * @param prevDuration the look-behind window size (in Duration)
     * @param fwdDuration the look-ahead window size (in Duration)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(String timestampCol, Duration prevDuration, Duration fwdDuration,
            String... pairs) {
        return RollingSumSpec.ofTime(timestampCol, prevDuration, fwdDuration).clause(pairs);
    }

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using time as the windowing unit.
     * This function accepts {@code nanoseconds} as the prev window parameters. A row that contains a {@code null} in
     * the timestamp column belongs to no window and will not have a value computed or be considered in the windows of
     * other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param prevNanos the look-behind window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(String timestampCol, long prevNanos, String... pairs) {
        return RollingSumSpec.ofTime(timestampCol, prevNanos).clause(pairs);
    }

    /**
     * Create a {@link RollingSumSpec rolling sum} for the supplied column name pairs, using time as the windowing unit.
     * This function accepts {@code nanoseconds} as the prev and forward window parameters. Negative values are allowed
     * and can be used to generate completely forward or completely reverse windows. A row that contains a {@code null}
     * in the timestamp column belongs to no window and will not have a value computed or be considered in the windows
     * of other rows.
     *
     * @param timestampCol the name of the timestamp column
     * @param prevNanos the look-behind window size (in nanoseconds)
     * @param fwdNanos the look-ahead window size (in nanoseconds)
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static UpdateByOperation RollingSum(String timestampCol, long prevNanos, long fwdNanos, String... pairs) {
        return RollingSumSpec.ofTime(timestampCol, prevNanos, fwdNanos).clause(pairs);
    }

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(ColumnUpdateOperation clause);
    }
}

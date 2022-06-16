package io.deephaven.engine.table;

import io.deephaven.api.ColumnName;
import io.deephaven.engine.table.updateBySpec.*;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collection;

/**
 * Defines an operation that can be applied to a table with {@link Table#updateBy(Collection, String...)}
 */
public interface UpdateByClause {
    /**
     * Conjoin an {@link UpdateBySpec} with columns for it to be applied to so the engine can construct the proper
     * operators.
     * 
     * @param spec the {@link UpdateBySpec} that defines the operation to perform
     * @param columns the columns to apply the operation to.
     * @return a {@link ColumnUpdateClause} that will be used to construct operations for each column
     */
    static ColumnUpdateClause of(@NotNull final UpdateBySpec spec, final String... columns) {
        return ImmutableColumnUpdateClause.builder().spec(spec).addColumns(columns).build();
    }

    // region Specs
    /**
     * Simply wrap the input specs as a collection suitable for
     * {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}. This is functionally equivalent to
     * {@link Arrays#asList(Object[])}.
     *
     * @param operations the operations to wrap.
     * @return a collection for use with {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static Collection<UpdateByClause> of(final @NotNull UpdateByClause... operations) {
        return Arrays.asList(operations);
    }

    /**
     * Create a forward fill operation for the specified columns.
     *
     * @param columnsToSum the columns to fill.
     * @return a new {@link UpdateByClause} for performing a forward fill with
     *         {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static UpdateByClause sum(final String... columnsToSum) {
        return of(CumSumSpec.of(), columnsToSum);
    }

    /**
     * Create a forward fill operation for the specified columns.
     *
     * @param columnsToFill the columns to fill.
     * @return a new {@link UpdateByClause} for performing a forward fill with
     *         {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static UpdateByClause fill(final String... columnsToFill) {
        return of(FillBySpec.of(), columnsToFill);
    }

    /**
     * Create a Cumulative Minimum of the specified columns.
     * 
     * @param columns the columns to find the min
     * @return a new {@link UpdateByClause} for performing a cumulative min with
     *         {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static UpdateByClause min(final String... columns) {
        return of(CumMinMaxSpec.of(false), columns);
    }

    /**
     * Create a Cumulative Maximum of the specified columns.
     * 
     * @param columns the columns to find the min
     * @return a new {@link UpdateByClause} for performing a cumulative max with
     *         {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static UpdateByClause max(final String... columns) {
        return of(CumMinMaxSpec.of(true), columns);
    }

    /**
     * Create a Cumulative Product of the specified columns.
     * 
     * @param columns the columns to find the min
     * @return a new {@link UpdateByClause} for performing a cumulative produce with
     *         {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static UpdateByClause prod(final String... columns) {
        return of(CumProdSpec.of(), columns);
    }

    /**
     * Create an Exponential Moving Average of the specified columns, using ticks as the decay unit.
     * <p>
     * The formula used is
     * </p>
     * 
     * <pre>
     *     a = e^(-1 / timeScaleTicks)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param timeScaleTicks the decay rate (tau) in ticks
     * @param control a {@link EmaControl.Builder control} object that defines how special cases should behave. See
     *        {@link EmaControl} for further details.
     * @param columns the columns to apply the EMA to.
     * @return a new {@link UpdateByClause} for performing an EMA with
     *         {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static UpdateByClause ema(final long timeScaleTicks, @NotNull final EmaControl.Builder control,
            final String... columns) {
        return ema(timeScaleTicks, control.build(), columns);
    }

    /**
     * Create an Exponential Moving Average of the specified columns, using ticks as the decay unit.
     * <p>
     * The formula used is
     * </p>
     * 
     * <pre>
     *     a = e^(-1 / timeScaleTicks)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param timeScaleTicks the decay rate (tau) in ticks
     * @param control a {@link EmaControl control} object that defines how special cases should behave. See
     *        {@link EmaControl} for further details.
     * @param columns the columns to apply the EMA to.
     * @return a new {@link UpdateByClause} for performing an EMA with
     *         {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static UpdateByClause ema(final long timeScaleTicks, @NotNull final EmaControl control, final String... columns) {
        return of(EmaSpec.ofTicks(control, timeScaleTicks), columns);
    }


    /**
     * Create an Exponential Moving Average of the specified columns, using ticks as the decay unit. Uses the default
     * EmaControl settings.
     * <p>
     * The formula used is
     * </p>
     *
     * <pre>
     *     a = e^(-1 / timeScaleTicks)
     *     ema_next = a * ema_last + (1 - a) * value
     * </pre>
     *
     * @param timeScaleTicks the decay rate (tau) in ticks
     * @param columns the columns to apply the EMA to.
     * @return a new {@link UpdateByClause} for performing an EMA with
     *         {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static UpdateByClause ema(final long timeScaleTicks, final String... columns) {
        return ema(timeScaleTicks, EmaControl.DEFAULT, columns);
    }

    /**
     * <p>
     * Create an Exponential Moving Average of the specified columns, using time as the decay unit.
     * </p>
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
     * @param timeScaleNanos the decay rate (tau) in nanoseconds
     * @param control a {@link EmaControl.Builder control} object that defines how special cases should behave. See
     *        {@link EmaControl} for further details.
     * @param columns the columns to apply the EMA to.
     * @return a new {@link UpdateByClause} for performing an EMA with
     *         {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static UpdateByClause ema(@NotNull final String timestampColumn, final long timeScaleNanos,
            @NotNull final EmaControl.Builder control, final String... columns) {
        return ema(timestampColumn, timeScaleNanos, control.build(), columns);
    }

    /**
     * Create an Exponential Moving Average of the specified columns, using time as the decay unit.
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
     * @param timeScaleNanos the decay rate (tau) in nanoseconds
     * @param control a {@link EmaControl control} object that defines how special cases should behave. See
     *        {@link EmaControl} for further details.
     * @param columns the columns to apply the EMA to.
     * @return a new {@link UpdateByClause} for performing an EMA with
     *         {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static UpdateByClause ema(@NotNull final String timestampColumn, final long timeScaleNanos,
            @NotNull final EmaControl control, final String... columns) {
        return of(EmaSpec.ofTime(control, timestampColumn, timeScaleNanos), columns);
    }

    /**
     * Create an Exponential Moving Average of the specified columns, using time as the decay unit. Uses the default
     * EmaControl settings.
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
     * @param timeScaleNanos the decay rate (tau) in nanoseconds
     * @param columns the columns to apply the EMA to.
     * @return a new {@link UpdateByClause} for performing an EMA with
     *         {@link Table#updateBy(UpdateByControl, Collection, MatchPair...)}
     */
    static UpdateByClause ema(@NotNull final String timestampColumn, final long timeScaleNanos,
            final String... columns) {
        return of(EmaSpec.ofTime(EmaControl.DEFAULT, timestampColumn, timeScaleNanos), columns);
    }
    // endregion

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(ColumnUpdateClause clause);
    }
}

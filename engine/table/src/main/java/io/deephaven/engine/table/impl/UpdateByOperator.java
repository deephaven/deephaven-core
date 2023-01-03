package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * An operator that performs a specific computation for {@link Table#updateBy}. When adding implementations of this
 * interface, the pattern of calls will be as follows.
 *
 * <ol>
 * <li>{@link UpdateByCumulativeOperator#initializeUpdate(UpdateContext, long, long)} for cumulative operators or
 * {@link UpdateByWindowedOperator#initializeUpdate(UpdateContext)} for windowed operators</li>
 * <li>{@link io.deephaven.engine.table.impl.UpdateByCumulativeOperator.Context#accumulate(RowSequence, Chunk[], LongChunk, int)}
 * for cumulative operators or
 * {@link UpdateByWindowedOperator.Context#accumulate(RowSequence, Chunk[], IntChunk, IntChunk, int)} for windowed
 * operators</li>
 * <li>{@link #finishUpdate(UpdateContext)}</li>
 * </ol>
 */
public abstract class UpdateByOperator {
    public static UpdateByOperator[] ZERO_LENGTH_OP_ARRAY = new UpdateByOperator[0];

    protected final MatchPair pair;
    protected final String[] affectingColumns;
    protected final UpdateBy.UpdateByRedirectionHelper redirHelper;

    // these will be used by the timestamp-aware operators (EMA for example)
    protected OperationControl control;
    protected long reverseTimeScaleUnits;
    protected long forwardTimeScaleUnits;
    protected String timestampColumnName;

    /**
     * The input modifiedColumnSet for this operator
     */
    protected ModifiedColumnSet inputModifiedColumnSet;
    /**
     * The output modifiedColumnSet for this operator
     */
    protected ModifiedColumnSet outputModifiedColumnSet;

    /**
     * A context item for use with updateBy operators
     */
    public interface UpdateContext extends SafeCloseable {

        void setValuesChunk(@NotNull Chunk<? extends Values> valuesChunk);

        void setTimestampChunk(@NotNull LongChunk<? extends Values> valuesChunk);

        /**
         * Add a value to the operators current data set
         *
         * @param key the row key associated with the value
         * @param pos the index in the associated chunk where this value can be found. Depending on the usage, might be
         *        a values chunk (for cumulative operators) or an influencer values chunk (for windowed). It is the task
         *        of the operator to pull the data from the chunk and use it properly
         */
        void push(long key, int pos);

        /**
         * Remove a value from the operators current data set. This is only valid for windowed operators since
         * cumulative operators only append values
         */
        void pop();

        /**
         * Write the current value for this row to the output chunk
         */
        void writeToOutputChunk(int outIdx);

        /**
         * Reset the operator data values to a known state. This may occur during initialization or when a windowed
         * operator has an empty window
         */
        void reset();

        /**
         * Write the output chunk to the output column
         */
        void writeToOutputColumn(@NotNull final RowSequence inputKeys);
    }


    protected UpdateByOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final OperationControl control,
            @Nullable final String timestampColumnName,
            final long reverseTimeScaleUnits,
            final long forwardTimeScaleUnits,
            @NotNull final UpdateBy.UpdateByRedirectionHelper redirHelper) {
        this.pair = pair;
        this.affectingColumns = affectingColumns;
        this.redirHelper = redirHelper;
        this.timestampColumnName = timestampColumnName;
        this.control = control;
        this.reverseTimeScaleUnits = reverseTimeScaleUnits;
        this.forwardTimeScaleUnits = forwardTimeScaleUnits;
    }

    /**
     * Get the names of the input column(s) for this operator.
     *
     * @return the names of the input column
     */
    @NotNull
    public String[] getInputColumnNames() {
        return new String[] {pair.rightColumn};
    }

    /**
     * Get the name of the timestamp column for this operator (or null if the operator does not require timestamp data).
     *
     * @return the name of the timestamp column
     */
    @Nullable
    public String getTimestampColumnName() {
        return timestampColumnName;
    }

    /**
     * Get the value of the backward-looking window (might be nanoseconds or ticks).
     */
    public long getPrevWindowUnits() {
        return reverseTimeScaleUnits;
    }

    /**
     * Get the value of the forward-looking window (might be nanoseconds or ticks).
     */
    public long getFwdWindowUnits() {
        return forwardTimeScaleUnits;
    }

    /**
     * Get an array of column names that, when modified, affect the result of this computation.
     *
     * @return an array of column names that affect this operator.
     */
    @NotNull
    public String[] getAffectingColumnNames() {
        return affectingColumns;
    }

    /**
     * Get an array of the output column names.
     *
     * @return the output column names.
     */
    @NotNull
    public String[] getOutputColumnNames() {
        return new String[] {pair.leftColumn};
    }

    /**
     * Get a map of outputName to output {@link ColumnSource} for this operation.
     *
     * @return a map of output column name to output column source
     */
    @NotNull
    public abstract Map<String, ColumnSource<?>> getOutputColumns();

    /**
     * Indicate that the operation should start tracking previous values for ticking updates.
     */
    public abstract void startTrackingPrev();

    /**
     * Make an {@link UpdateContext} suitable for use with updates.
     *
     * @param chunkSize The expected size of chunks that will be provided during the update,
     * @return a new context
     */
    @NotNull
    public abstract UpdateContext makeUpdateContext(final int chunkSize);

    /**
     * Perform any bookkeeping required at the end of a single part of the update. This is always preceded with a call
     * to {@code #initializeUpdate(UpdateContext)} (specialized for each type of operator)
     *
     * @param context the context object
     */
    public abstract void finishUpdate(@NotNull final UpdateContext context);

    /**
     * Apply a shift to the operation.
     */
    public abstract void applyOutputShift(@NotNull final RowSet subRowSetToShift, final long delta);

    /**
     * Prepare this operator output column for parallel updated.
     */
    public abstract void prepareForParallelPopulation(final RowSet changedRows);

    /**
     * Create the modified column set for the input columns of this operator.
     */
    public void createInputModifiedColumnSet(@NotNull final QueryTable source) {
        inputModifiedColumnSet = source.newModifiedColumnSet(getAffectingColumnNames());
    }

    /**
     * Create the modified column set for the output columns from this operator.
     */
    public void createOutputModifiedColumnSet(@NotNull final QueryTable result) {
        outputModifiedColumnSet = result.newModifiedColumnSet(getOutputColumnNames());
    }

    /**
     * Return the modified column set for the input columns of this operator.
     */
    public ModifiedColumnSet getInputModifiedColumnSet() {
        return inputModifiedColumnSet;
    }

    /**
     * Return the modified column set for the output columns from this operator.
     */
    public ModifiedColumnSet getOutputModifiedColumnSet() {
        return outputModifiedColumnSet;
    }

}

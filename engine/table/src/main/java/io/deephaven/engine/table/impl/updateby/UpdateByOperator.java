package io.deephaven.engine.table.impl.updateby;

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
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.Map;

/**
 * An operator that performs a specific computation for {@link Table#updateBy}. When adding implementations of this
 * interface, the pattern of calls will be as follows.
 *
 * <ol>
 * <li>{@link UpdateByOperator#initializeCumulative(Context, long, long)} for cumulative operators or
 * {@link UpdateByOperator#initializeRolling(Context)} (Context)} for windowed operators</li>
 * <li>{@link UpdateByOperator.Context#accumulateCumulative(RowSequence, Chunk[], LongChunk, int)} for cumulative
 * operators or {@link UpdateByOperator.Context#accumulateRolling(RowSequence, Chunk[], IntChunk, IntChunk, int)} for
 * windowed operators</li>
 * <li>{@link #finishUpdate(UpdateByOperator.Context)}</li>
 * </ol>
 */
public abstract class UpdateByOperator {
    protected static UpdateByOperator[] ZERO_LENGTH_OP_ARRAY = new UpdateByOperator[0];

    protected final MatchPair pair;
    protected final String[] affectingColumns;
    protected final RowRedirection rowRedirection;

    protected final long reverseWindowScaleUnits;
    protected final long forwardWindowScaleUnits;
    protected final String timestampColumnName;

    final boolean isWindowed;

    /**
     * The input modifiedColumnSet for this operator
     */
    ModifiedColumnSet inputModifiedColumnSet;
    /**
     * The output modifiedColumnSet for this operator
     */
    ModifiedColumnSet outputModifiedColumnSet;

    /**
     * A context item for use with updateBy operators
     */
    public abstract static class Context implements SafeCloseable {
        protected final Chunk<? extends Values>[] chunkArr;
        protected int nullCount = 0;

        public Context(int chunkCount) {
            chunkArr = new Chunk[chunkCount];
        }

        public boolean isValueValid(long atKey) {
            throw new UnsupportedOperationException(
                    "isValueValid() must be overridden by time-aware cumulative operators");
        }

        @Override
        public void close() {}

        protected abstract void setValuesChunk(@NotNull Chunk<? extends Values> valuesChunk);

        /**
         * Add values to the operators current data set
         *
         * @param key the row key associated with the value
         * @param pos the index in the associated chunk where this value can be found. Depending on the usage, might be
         *        a values chunk (for cumulative operators) or an influencer values chunk (for windowed). It is the task
         *        of the operator to pull the data from the chunk and use it properly
         * @param count the number of items to push from the chunk
         */
        protected abstract void push(long key, int pos, int count);

        /**
         * Remove values from the operators current data set. This is only valid for windowed operators as cumulative
         * operators only append values
         *
         * @param count the number of items to pop from the data set
         */
        protected void pop(int count) {
            throw new UnsupportedOperationException("pop() must be overriden by rolling operators");
        }

        public abstract void accumulateCumulative(RowSequence inputKeys,
                Chunk<? extends Values>[] valueChunkArr,
                LongChunk<? extends Values> tsChunk,
                int len);

        public abstract void accumulateRolling(RowSequence inputKeys,
                Chunk<? extends Values>[] influencerValueChunkArr,
                IntChunk<? extends Values> pushChunk,
                IntChunk<? extends Values> popChunk,
                int len);

        /**
         * Write the current value for this row to the output chunk
         */
        protected abstract void writeToOutputChunk(int outIdx);


        /**
         * Write the output chunk to the output column
         */
        protected abstract void writeToOutputColumn(@NotNull final RowSequence inputKeys);

        /**
         * Reset the operator data values to a known state. This may occur during initialization or when a windowed
         * operator has an empty window
         */
        @OverridingMethodsMustInvokeSuper
        protected abstract void reset();
    }


    protected UpdateByOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits,
            final boolean isWindowed) {
        this.pair = pair;
        this.affectingColumns = affectingColumns;
        this.rowRedirection = rowRedirection;
        this.timestampColumnName = timestampColumnName;
        this.reverseWindowScaleUnits = reverseWindowScaleUnits;
        this.forwardWindowScaleUnits = forwardWindowScaleUnits;
        this.isWindowed = isWindowed;
    }

    /**
     * Initialize the bucket context for a cumulative operator
     */
    public void initializeCumulative(@NotNull final Context context, final long firstUnmodifiedKey,
            long firstUnmodifiedTimestamp) {}

    /**
     * Initialize the bucket context for a windowed operator
     */
    public void initializeRolling(@NotNull final Context context) {}

    /**
     * Get the names of the input column(s) for this operator.
     *
     * @return the names of the input column
     */
    @NotNull
    protected String[] getInputColumnNames() {
        return new String[] {pair.rightColumn};
    }

    /**
     * Get the name of the timestamp column for this operator (or null if the operator does not require timestamp data).
     *
     * @return the name of the timestamp column
     */
    @Nullable
    protected String getTimestampColumnName() {
        return timestampColumnName;
    }

    /**
     * Get the value of the backward-looking window (might be nanoseconds or ticks).
     */
    protected long getPrevWindowUnits() {
        return reverseWindowScaleUnits;
    }

    /**
     * Get the value of the forward-looking window (might be nanoseconds or ticks).
     */
    protected long getFwdWindowUnits() {
        return forwardWindowScaleUnits;
    }

    /**
     * Get an array of column names that, when modified, affect the result of this computation.
     *
     * @return an array of column names that affect this operator.
     */
    @NotNull
    protected String[] getAffectingColumnNames() {
        return affectingColumns;
    }

    /**
     * Get an array of the output column names.
     *
     * @return the output column names.
     */
    @NotNull
    protected String[] getOutputColumnNames() {
        return new String[] {pair.leftColumn};
    }

    /**
     * Get a map of outputName to output {@link ColumnSource} for this operation.
     *
     * @return a map of output column name to output column source
     */
    @NotNull
    protected abstract Map<String, ColumnSource<?>> getOutputColumns();

    /**
     * Indicate that the operation should start tracking previous values for ticking updates.
     */
    protected abstract void startTrackingPrev();

    /**
     * Make an {@link Context} suitable for use with updates.
     *
     * @param chunkSize The expected size of chunks that will be provided during the update,
     * @param chunkCount The number of chunks that will be provided during the update,
     * @return a new context
     */
    @NotNull
    public abstract Context makeUpdateContext(final int chunkSize, final int chunkCount);

    /**
     * Perform any bookkeeping required at the end of a single part of the update. This is always preceded with a call
     * to {@code #initializeUpdate(Context)} (specialized for each type of operator)
     *
     * @param context the context object
     */
    protected void finishUpdate(@NotNull final Context context) {}

    /**
     * Apply a shift to the operation.
     */
    protected abstract void applyOutputShift(@NotNull final RowSet subRowSetToShift, final long delta);

    /**
     * Prepare this operator output column for parallel updated.
     */
    protected abstract void prepareForParallelPopulation(final RowSet changedRows);

    /**
     * Create the modified column set for the input columns of this operator.
     */
    protected void createInputModifiedColumnSet(@NotNull final QueryTable source) {
        inputModifiedColumnSet = source.newModifiedColumnSet(getAffectingColumnNames());
    }

    /**
     * Create the modified column set for the output columns from this operator.
     */
    protected void createOutputModifiedColumnSet(@NotNull final QueryTable result) {
        outputModifiedColumnSet = result.newModifiedColumnSet(getOutputColumnNames());
    }

    /**
     * Return the modified column set for the input columns of this operator.
     */
    protected ModifiedColumnSet getInputModifiedColumnSet() {
        return inputModifiedColumnSet;
    }

    /**
     * Return the modified column set for the output columns from this operator.
     */
    protected ModifiedColumnSet getOutputModifiedColumnSet() {
        return outputModifiedColumnSet;
    }

    /**
     * Clear the output rows by setting value to NULL. Dense sources will apply removes to the inner source.
     */
    protected abstract void clearOutputRows(RowSet toClear);
}

package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.updateby.UpdateByWindow;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;

/**
 * An operator that performs a specific computation for {@link Table#updateBy}. When adding implementations of this
 * interface, the pattern of calls will be as follows.
 *
 * <ol>
 * <li>Reprocess
 * <ul>
 * <li>{@link #initializeFor(UpdateContext, RowSet, UpdateBy.UpdateType)}</li>
 * <li>{@link #reprocessChunkBucketed(UpdateContext, RowSequence, Chunk, LongChunk, IntChunk, IntChunk, IntChunk)}</li>
 * <li>{@link #finishFor(UpdateContext, UpdateBy.UpdateType)}</li>
 * </ul>
 * </li>
 * </ol>
 *
 * <p>
 * Additionally, implementations are responsible for notifying the update model if any rows have been modified beyond
 * what was passed through in the upstream update via the {@link #anyModified(UpdateContext)} and
 * {@link #getAdditionalModifications(UpdateContext)} methods
 * </p>
 */
public interface UpdateByOperator {
    UpdateByWindow[] ZERO_LENGTH_WINDOW_ARRAY = new UpdateByWindow[0];
    UpdateByOperator[] ZERO_LENGTH_OP_ARRAY = new UpdateByOperator[0];

    /**
     * A context item for use with {@link Table#updateBy(UpdateByControl, Collection, String...)} for non-bucketed
     * updates.
     */
    interface UpdateContext extends SafeCloseable {
    }

    /**
     * Add a value to the operators current data set
     *
     * @param context the operator context for this action
     * @param key the row key associated with the value
     * @param pos the index in the associated chunk where this value can be found. Depending on the usage, might be a
     *        values chunk (for cumulative operators) or an influencer values chunk (for windowed). It is the task of
     *        the operator to pull the data from the chunk and use it properly
     */
    void push(UpdateContext context, long key, int pos);

    /**
     * Remove a value from the operators current data set. This is only valid for windowed operators since cumulative
     * operators only append values
     *
     * @param context the operator context for this action
     */
    void pop(UpdateContext context);

    /**
     * Reset the operator data values to a known state. This may occur during initialization or when a windowed operator
     * has an empty window
     *
     * @param context the operator context for this action
     */
    void reset(UpdateContext context);

    /**
     * Get the name of the input column this operator depends on.
     *
     * @return the name of the input column
     */
    @NotNull
    String getInputColumnName();

    /**
     * Get the name of the timestamp column this operator depends on.
     *
     * @return the name of the input column
     */
    @Nullable
    String getTimestampColumnName();

    /**
     * Get the value of the backward-looking window (might be nanos or ticks).
     *
     * @return the name of the input column
     */
    long getPrevWindowUnits();

    /**
     * Get the value of the forward-looking window (might be nanos or ticks).
     *
     * @return the name of the input column
     */
    long getFwdWindowUnits();

    /**
     * Get an array of column names that, when modified, affect the result of this computation.
     *
     * @return an array of column names that affect this operator.
     */
    @NotNull
    String[] getAffectingColumnNames();

    /**
     * Get an array of the output column names.
     *
     * @return the output column names.
     */
    @NotNull
    String[] getOutputColumnNames();

    /**
     * Get a map of outputName to output {@link ColumnSource} for this operation.
     *
     * @return a map of output column name to output column source
     */
    @NotNull
    Map<String, ColumnSource<?>> getOutputColumns();

    /**
     * Indicate that the operation should start tracking previous values for ticking updates.
     */
    void startTrackingPrev();

    /**
     * Make an {@link UpdateContext} suitable for use with non-bucketed updates.
     *
     * @param chunkSize The expected size of chunks that will be provided during the update,
     * @param inputSource
     * @return a new context
     */
    @NotNull
    UpdateContext makeUpdateContext(final int chunkSize, ColumnSource<?> inputSource);

    /**
     * Perform and bookkeeping required at the end of a single part of the update. This is always preceded with a call
     * to {@code #initializeUpdate(UpdateContext)} (specialized for each type of operator)
     *
     * @param context the context object
     */
    void finishUpdate(@NotNull final UpdateContext context);

    /**
     * Apply a shift to the operation.
     *
     */
    void applyOutputShift(@NotNull final RowSet subIndexToShift, final long delta);

    /**
     * Process a chunk of data for an updateBy table.
     * 
     * @param context the context object
     * @param inputKeys the keys contained in the chunk
     * @param keyChunk a {@link LongChunk} containing the keys if requested
     * @param posChunk a {@link LongChunk} containing the positions if requested
     * @param valuesChunk the current chunk of working values.
     * @param timestampValuesChunk a {@link LongChunk} containing the working timestamps if requested
     */
    void processChunk(@NotNull final UpdateContext context,
            @NotNull final RowSequence inputKeys,
            @Nullable final LongChunk<OrderedRowKeys> keyChunk,
            @Nullable final LongChunk<OrderedRowKeys> posChunk,
            @Nullable final Chunk<Values> valuesChunk,
            @Nullable final LongChunk<? extends Values> timestampValuesChunk);
}

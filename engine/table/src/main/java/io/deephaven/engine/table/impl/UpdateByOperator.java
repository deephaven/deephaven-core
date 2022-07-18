package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
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
 * <li>Removes
 * <ul>
 * <li>{@link #initializeFor(UpdateContext, RowSet, UpdateBy.UpdateType)}</li>
 * <li>{@link #removeChunk(UpdateContext, LongChunk, Chunk, long)}</li>
 * <li>{@link #finishFor(UpdateContext, UpdateBy.UpdateType)}</li>
 * </ul>
 * </li>
 * <li>Shifts
 * <ul>
 * <li>{@link #initializeFor(UpdateContext, RowSet, UpdateBy.UpdateType)}</li>
 * <li>{@link #applyShift(UpdateContext, RowSet, RowSetShiftData)}</li>
 * <li>{@link #finishFor(UpdateContext, UpdateBy.UpdateType)}</li>
 * </ul>
 * </li>
 * <li>Modifies
 * <ul>
 * <li>{@link #initializeFor(UpdateContext, RowSet, UpdateBy.UpdateType)}</li>
 * <li>{@link #modifyChunk(UpdateContext, LongChunk, LongChunk, Chunk, Chunk, long)}</li>
 * <li>{@link #finishFor(UpdateContext, UpdateBy.UpdateType)}</li>
 * </ul>
 * </li>
 * <li>Adds
 * <ul>
 * <li>{@link #initializeFor(UpdateContext, RowSet, UpdateBy.UpdateType)}</li>
 * <li>{@link #addChunk(UpdateContext, RowSequence, LongChunk, Chunk, long)}</li>
 * <li>{@link #finishFor(UpdateContext, UpdateBy.UpdateType)}</li>
 * </ul>
 * </li>
 *
 * <li>Reprocess
 * <ul>
 * <li>{@link #resetForReprocess(UpdateContext, RowSet, long)}</li>
 * <li>{@link #initializeFor(UpdateContext, RowSet, UpdateBy.UpdateType)}</li>
 * <li>{@link #reprocessChunk(UpdateContext, RowSequence, Chunk, LongChunk, IntChunk, IntChunk, IntChunk)}</li>
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
    UpdateByOperator[] ZERO_LENGTH_OP_ARRAY = new UpdateByOperator[0];

    /**
     * Check if the specified {@link TableUpdate} was append-only given the last known key within some other index.
     *
     * @param update the update to check
     * @param lastKnownKey the last known key from some other index.
     * @return if the update was append-only given the last known key
     */
    static boolean isAppendOnly(@NotNull final TableUpdate update, final long lastKnownKey) {
        return update.removed().isEmpty() &&
                update.modified().isEmpty() &&
                update.shifted().empty() &&
                update.added().firstRowKey() > lastKnownKey;
    }

    /**
     * Find the smallest valued key that participated in the upstream {@link TableUpdate}.
     *
     * @param upstream the update
     * @return the smallest key that participated in any part of the update.
     */
    static long determineSmallestVisitedKey(@NotNull final TableUpdate upstream, @NotNull final RowSet affected) {
        return determineSmallestVisitedKey(upstream.added(),
                upstream.modified(),
                upstream.removed(),
                upstream.shifted(),
                affected);
    }

    /**
     * Find the smallest valued key that participated in the upstream {@link TableUpdate}.
     *
     * @param added the added rows
     * @param modified the modified rows
     * @param removed the removed rows
     * @param shifted the shifted rows
     *
     * @return the smallest key that participated in any part of the update.
     */
    static long determineSmallestVisitedKey(@NotNull final RowSet added,
            @NotNull final RowSet modified,
            @NotNull final RowSet removed,
            @NotNull final RowSetShiftData shifted,
            @NotNull final RowSet affectedIndex) {

        long smallestModifiedKey = Long.MAX_VALUE;
        if (removed.isNonempty()) {
            smallestModifiedKey = removed.firstRowKey();
        }

        if (added.isNonempty()) {
            smallestModifiedKey = Math.min(smallestModifiedKey, added.firstRowKey());
        }

        if (modified.isNonempty()) {
            smallestModifiedKey = Math.min(smallestModifiedKey, modified.firstRowKey());
        }

        if (shifted.nonempty()) {
            final long firstModKey = modified.isEmpty() ? Long.MAX_VALUE : modified.firstRowKey();
            boolean modShiftFound = !modified.isEmpty();
            boolean affectedFound = false;
            try (final RowSequence.Iterator it = affectedIndex.getRowSequenceIterator()) {
                for (int shiftIdx = 0; shiftIdx < shifted.size() && (!modShiftFound || !affectedFound); shiftIdx++) {
                    final long shiftStart = shifted.getBeginRange(shiftIdx);
                    final long shiftEnd = shifted.getEndRange(shiftIdx);
                    final long shiftDelta = shifted.getShiftDelta(shiftIdx);

                    if (!affectedFound) {
                        if (it.advance(shiftStart + shiftDelta)) {
                            final long maybeAffectedKey = it.peekNextKey();
                            if (maybeAffectedKey <= shiftEnd + shiftDelta) {
                                affectedFound = true;
                                final long keyToCompare =
                                        shiftDelta > 0 ? maybeAffectedKey - shiftDelta : maybeAffectedKey;
                                smallestModifiedKey = Math.min(smallestModifiedKey, keyToCompare);
                            }
                        } else {
                            affectedFound = true;
                        }
                    }

                    if (!modShiftFound) {
                        if (firstModKey <= (shiftEnd + shiftDelta)) {
                            modShiftFound = true;
                            // If the first modified key is in the range we should include it
                            if (firstModKey >= (shiftStart + shiftDelta)) {
                                smallestModifiedKey = Math.min(smallestModifiedKey, firstModKey - shiftDelta);
                            } else {
                                // Otherwise it's not included in any shifts, and since shifts can't reorder rows
                                // it is the smallest possible value and we've already accounted for it above.
                                break;
                            }
                        }
                    }
                }
            }
        }

        return smallestModifiedKey == Long.MAX_VALUE ? RowSet.NULL_ROW_KEY : smallestModifiedKey;
    }

    /**
     * A context item for use with {@link Table#updateBy(UpdateByControl, Collection, String...)} for non-bucketed
     * updates.
     */
    interface UpdateContext extends SafeCloseable {
    }

    /**
     * Notify the operator of the current maximum bucket.
     * 
     * @param capacity the capacity
     */
    void setBucketCapacity(final int capacity);

    /**
     * Get the name of the input column this operator depends on.
     *
     * @return the name of the input column
     */
    @NotNull
    String getInputColumnName();

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
     * @return a new context
     */
    @NotNull
    UpdateContext makeUpdateContext(final int chunkSize);

    /**
     * Initialize the operator for an update cycle. This is invoked before any other update processing occurs.
     *
     * @param context the context object
     * @param upstream the upstream update to process
     * @param resultSourceIndex the result index of the source table
     * @param usingBuckets if the update is bucketed or not
     * @param isUpstreamAppendOnly if the upstream update was detected to be append-only.
     */
    void initializeForUpdate(@NotNull final UpdateContext context,
            @NotNull final TableUpdate upstream,
            @NotNull final RowSet resultSourceIndex,
            final boolean usingBuckets,
            final boolean isUpstreamAppendOnly);

    /**
     * <p>
     * Initialize the context for the specified stage of the update process. This will always be followed by a call to
     * {@link #finishFor(UpdateContext, UpdateBy.UpdateType)} at the end of each successful update.
     * </p>
     *
     * @param context the context object
     * @param updateIndex the index of rows associated with the update.
     * @param type the type of update being applied
     */
    void initializeFor(@NotNull final UpdateContext context,
            @NotNull final RowSet updateIndex,
            @NotNull final UpdateBy.UpdateType type);

    /**
     * Perform and bookkeeping required at the end of a single part of the update. This is always preceded with a call
     * to
     *
     * {@link #initializeFor(UpdateContext, RowSet, UpdateBy.UpdateType)}
     *
     * @param context the context object
     * @param type the type of update being applied
     */
    void finishFor(@NotNull final UpdateContext context, @NotNull final UpdateBy.UpdateType type);

    /**
     * Get an index of rows that were modified beyond the input set of modifications from the upstream. This is invoked
     * once at the end of a complete update cycle (that is, after all adds, removes, modifies and shifts have been
     * processed) if {@link #anyModified(UpdateContext)} has returned true.
     *
     * @param context the context object
     * @return a {@link RowSet index} of additional rows that were modified
     */
    @NotNull
    RowSet getAdditionalModifications(@NotNull final UpdateContext context);

    /**
     * Check if the update has modified any rows for this operator. This is invoked once at the end of a complete update
     * cycle (that is, after all adds, removes, modifies and shifts have been processed).
     *
     * @param context the context object
     * @return true if the update modified any rows.
     */
    boolean anyModified(@NotNull final UpdateContext context);

    /**
     * Query if the operator requires key values for the current stage. This method will always be invoked after an
     * appropriate invocation of {@link #initializeFor(UpdateContext, RowSet, UpdateBy.UpdateType)}
     *
     * @return true if the operator requires keys for this operation
     */
    boolean requiresKeys();

    /**
     * Query if the operator requires values for the current stage.
     *
     * @param context the context object
     * @return true if values are required for compuitation
     */
    boolean requiresValues(@NotNull final UpdateContext context);

    /**
     * Query if this operator can process the update normally, or if it can only reprocess. This method is guaranteed to
     * be invoked after {@link #initializeForUpdate(UpdateContext, TableUpdate, RowSet, boolean, boolean)} so the
     * operator is aware of the upstream {@link TableUpdate}.
     *
     * @param context the context
     * @return true if this operator can process the update normally.
     */
    boolean canProcessNormalUpdate(@NotNull final UpdateContext context);

    /**
     * Set the chunk size to be used for operations. This is used during the {@link UpdateBy.UpdateType#Reprocess} phase
     * when the chunks allocated during the normal processing phase may not be large enough.
     *
     * @param context the context object
     * @param chunkSize the new chunk size
     */
    void setChunkSize(@NotNull final UpdateContext context, final int chunkSize);


    /**
     * Called when some buckets have been completely emptied. Operators can use this to reset internal states.
     *
     * @param removedBuckets an index of removed bucket positions.
     */
    void onBucketsRemoved(@NotNull final RowSet removedBuckets);

    /**
     * Add a chunk of non-bucketed items to the operation.
     *
     * @param context the context object
     * @param keyChunk a chunk of keys for the rows being added. If the operator returns {@code false} for
     *        {@link #requiresKeys()} this will be null.
     * @param values the chunk of values for the rows being added
     * @param bucketPosition the group position
     */
    void addChunk(@NotNull final UpdateContext context,
            @NotNull final RowSequence inputKeys,
            @Nullable final LongChunk<OrderedRowKeys> keyChunk,
            @NotNull final Chunk<Values> values,
            final long bucketPosition);

    /**
     * Add a chunk of bucketed items to the operation.
     *
     * @param context the context object
     * @param values the value chunk
     * @param keyChunk a chunk of keys for the rows being added
     * @param bucketPositions a chunk of hash bucket positions for each key
     * @param runLengths the runLengths of each run of bucket values
     * @param startPositions the start position of a run within the chunk
     */
    void addChunk(@NotNull final UpdateContext context,
            @NotNull final Chunk<Values> values,
            @NotNull final LongChunk<? extends RowKeys> keyChunk,
            @NotNull final IntChunk<RowKeys> bucketPositions,
            @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> runLengths);

    /**
     * Modify a chunk of values with the operation.
     *
     * @param context the context object
     * @param prevKeyChunk a chunk of pre-shift keys. This will be equal to {@code keyChunk} if no shift is present
     * @param keyChunk a chunk of post-shift space keys for the update.
     * @param prevValuesChunk a chunk of previous values for the update
     * @param postValuesChunk a chunk of current values for the update
     * @param bucketPosition the position of the current group being processed
     */
    void modifyChunk(@NotNull final UpdateContext context,
            @Nullable final LongChunk<OrderedRowKeys> prevKeyChunk,
            @Nullable final LongChunk<OrderedRowKeys> keyChunk,
            @NotNull final Chunk<Values> prevValuesChunk,
            @NotNull final Chunk<Values> postValuesChunk,
            final long bucketPosition);

    /**
     * Remove a chunk of values from the operation.
     *
     * @param context the context object
     * @param keyChunk a chunk of keys being removed.
     * @param prevValuesChunk the chunk of values being removed
     * @param bucketPosition the position of the current group being processed
     */
    void removeChunk(@NotNull final UpdateContext context,
            @Nullable final LongChunk<OrderedRowKeys> keyChunk,
            @NotNull final Chunk<Values> prevValuesChunk,
            final long bucketPosition);

    /**
     * Apply a shift to the operation.
     *
     * @param context the context object
     * @param prevIndex the pre-shifted index
     * @param shifted the shifts being applied
     */
    void applyShift(@NotNull final UpdateContext context,
            @NotNull final RowSet prevIndex,
            @NotNull final RowSetShiftData shifted);

    /**
     * Apply a shift to the operation.
     *
     * @param context the context object
     */
    void applyOutputShift(@NotNull final UpdateContext context,
            @NotNull final RowSet subIndexToShift,
            final long delta);

    /**
     * Reprocess a chunk of data for a non-bucketed table.
     *
     * @param context the context object
     * @param inputKeys the keys contained in the chunk
     * @param keyChunk a {@link LongChunk} containing the keys if requested by {@link #requiresKeys()} or null.
     * @param valuesChunk the current chunk of working values.
     * @param postUpdateSourceIndex the resulting source index af
     */
    void reprocessChunk(@NotNull final UpdateContext context,
            @NotNull final RowSequence inputKeys,
            @Nullable final LongChunk<OrderedRowKeys> keyChunk,
            @NotNull final Chunk<Values> valuesChunk,
            @NotNull final RowSet postUpdateSourceIndex);

    /**
     * Reprocess a chunk of data for a bucketed table.
     *
     * @param context the context object
     * @param inputKeys the keys contained in the chunk
     * @param values the current chunk of working values.
     * @param keyChunk a {@link LongChunk} containing the keys.
     * @param bucketPositions a {@link IntChunk} containing the bucket position of each key. Parallel to `keyChunk` and
     *        `values
     * @param runStartPositions the starting positions of each run within the key and value chunk
     * @param runLengths the run runLengths of each run in the key and value chunks. Parallel to `runStartPositions`
     */
    void reprocessChunk(@NotNull final UpdateContext context,
            @NotNull final RowSequence inputKeys,
            @NotNull final Chunk<Values> values,
            @NotNull final LongChunk<? extends RowKeys> keyChunk,
            @NotNull final IntChunk<RowKeys> bucketPositions,
            @NotNull final IntChunk<ChunkPositions> runStartPositions,
            @NotNull final IntChunk<ChunkLengths> runLengths);

    /**
     * Reset the operator to the state at the `firstModifiedKey` for non-bucketed operation. This is invoked immediately
     * prior to calls to {@link #reprocessChunk(UpdateContext, RowSequence, LongChunk, Chunk, RowSet)}. <br>
     * <br>
     * A `firstUnmodifiedKey` of {@link RowSet#NULL_ROW_KEY} indicates that the entire table needs to be recomputed.
     *
     * @param context the context object
     * @param sourceIndex the current index of the source table
     * @param firstUnmodifiedKey the first unmodified key after which we will reprocess rows.
     */
    void resetForReprocess(@NotNull final UpdateContext context,
            @NotNull final RowSet sourceIndex,
            final long firstUnmodifiedKey);

    /**
     * Reset the operator to the state at the `firstModifiedKey` for the specified bucket. This is invoked immediately
     * prior to calls to
     * {@link #reprocessChunk(UpdateContext, RowSequence, Chunk, LongChunk, IntChunk, IntChunk, IntChunk)}.
     *
     * @param context the context object
     * @param bucketIndex the current index of the specified bucket
     * @param firstUnmodifiedKey the first unmodified key in the bucket after which we will reprocess rows.
     */
    void resetForReprocess(@NotNull final UpdateContext context,
            @NotNull final RowSet bucketIndex,
            final long bucketPosition,
            final long firstUnmodifiedKey);
}

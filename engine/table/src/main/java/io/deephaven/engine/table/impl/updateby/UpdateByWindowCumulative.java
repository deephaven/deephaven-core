//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;
import java.util.stream.IntStream;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * This is the specialization of {@link UpdateByWindow} that handles `cumulative` operators. These operators do not
 * maintain a window of data and can be computed from the previous value and the current value.
 */
class UpdateByWindowCumulative extends UpdateByWindow {

    UpdateByWindowCumulative(UpdateByOperator[] operators, int[][] operatorSourceSlots,
            @Nullable String timestampColumnName) {
        super(operators, operatorSourceSlots, timestampColumnName);
    }

    @Override
    UpdateByWindow copy() {
        final UpdateByOperator[] copiedOperators = new UpdateByOperator[this.operators.length];
        for (int ii = 0; ii < copiedOperators.length; ii++) {
            copiedOperators[ii] = this.operators[ii].copy();
        }

        return new UpdateByWindowCumulative(copiedOperators, operatorInputSourceSlots, timestampColumnName);
    }

    @Override
    void prepareWindowBucket(UpdateByWindowBucketContext context) {
        // working chunk size need not be larger than affectedRows.size()
        context.workingChunkSize = Math.toIntExact(Math.min(context.workingChunkSize, context.affectedRows.size()));
    }

    @Override
    UpdateByWindowBucketContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final TrackingRowSet timestampValidRowSet,
            final boolean timestampsModified,
            final int chunkSize,
            final boolean isInitializeStep,
            final Object[] bucketKeyValues) {
        return new UpdateByWindowBucketContext(
                sourceRowSet,
                timestampColumnSource,
                timestampSsa,
                timestampValidRowSet,
                timestampsModified,
                chunkSize,
                isInitializeStep,
                bucketKeyValues);
    }

    @Override
    void computeAffectedRowsAndOperators(UpdateByWindowBucketContext context, @NotNull TableUpdate upstream) {
        if (upstream.empty() || context.sourceRowSet.isEmpty()) {
            // No further work will be done on this context
            finalizeWindowBucket(context);
            return;
        }

        // all rows are affected on the initial step
        if (context.initialStep) {
            context.affectedRows = context.sourceRowSet.copy();
            context.influencerRows = context.affectedRows;

            // mark all operators as affected by this update
            context.dirtyOperatorIndices = IntStream.range(0, operators.length).toArray();
            context.dirtyOperators = new BitSet(operators.length);
            context.dirtyOperators.set(0, operators.length);

            context.isDirty = true;
            return;
        }

        // determine which operators are affected by this update
        processUpdateForContext(context, upstream);

        if (!context.isDirty) {
            // No further work will be done on this context
            finalizeWindowBucket(context);
            return;
        }

        // we can ignore modifications if they do not affect our input columns
        final boolean inputModified = context.inputModified
                || (timestampColumnName != null && context.timestampsModified);
        long smallestModifiedKey = smallestAffectedKey(upstream, context.sourceRowSet, inputModified);

        context.affectedRows = smallestModifiedKey == Long.MAX_VALUE
                ? RowSetFactory.empty()
                : context.sourceRowSet.subSetByKeyRange(smallestModifiedKey, context.sourceRowSet.lastRowKey());

        if (context.affectedRows.isEmpty()) {
            // No further work will be done on this context
            finalizeWindowBucket(context);
            context.isDirty = false;
            return;
        }

        context.influencerRows = context.affectedRows;
    }

    @Override
    void processWindowBucketOperatorSet(final UpdateByWindowBucketContext context,
            final int[] opIndices,
            final int[] srcIndices,
            final UpdateByOperator.Context[] winOpContexts,
            final Chunk<? extends Values>[] chunkArr,
            final ChunkSource.GetContext[] chunkContexts,
            final boolean initialStep) {
        Assert.neqNull(context.inputSources, "assignInputSources() must be called before processRow()");

        // Identify an operator to use for determining initialization state.
        final UpdateByOperator firstOp;
        final UpdateByOperator.Context firstOpCtx;

        if (initialStep || timestampColumnName == null) {
            // We can use any operator. When initialStep==true, we start from the beginning.
            firstOp = operators[opIndices[0]];
            firstOpCtx = winOpContexts[0];
        } else {
            // Use the first time-sensitive operator if one exists, or first overall otherwise.
            final int match = IntStream.range(0, opIndices.length)
                    .filter(ii -> operators[opIndices[ii]].timestampColumnName != null)
                    .findAny()
                    .orElse(0);
            firstOp = operators[opIndices[match]];
            firstOpCtx = winOpContexts[match];
        }

        try (final RowSequence.Iterator affectedIt = context.affectedRows.getRowSequenceIterator();
                ChunkSource.GetContext tsGetContext =
                        context.timestampColumnSource == null ? null
                                : context.timestampColumnSource.makeGetContext(context.workingChunkSize)) {

            final long rowKey;
            final long timestamp;

            if (initialStep) {
                // We are always at the beginning of the RowSet at creation phase.
                rowKey = NULL_ROW_KEY;
                timestamp = NULL_LONG;
            } else {
                // Find the key before the first affected row.
                final long pos = context.sourceRowSet.find(context.affectedRows.firstRowKey());
                final long keyBefore = pos == 0 ? NULL_ROW_KEY : context.sourceRowSet.get(pos - 1);

                // Preload that data for these operators.
                if (firstOp.timestampColumnName == null || keyBefore == NULL_ROW_KEY) {
                    // This operator doesn't care about timestamps or we know we are at the beginning of the rowset
                    rowKey = keyBefore;
                    timestamp = NULL_LONG;
                } else {
                    // This operator cares about timestamps, so make sure it is starting from a valid value and
                    // valid timestamp by looking backward until the conditions are met.
                    long potentialResetTimestamp = context.timestampColumnSource.getLong(keyBefore);

                    if (potentialResetTimestamp == NULL_LONG || !firstOpCtx.isValueValid(keyBefore)) {
                        try (final RowSet.SearchIterator rIt = context.sourceRowSet.reverseIterator()) {
                            if (rIt.advance(keyBefore)) {
                                while (rIt.hasNext()) {
                                    final long nextKey = rIt.nextLong();
                                    potentialResetTimestamp = context.timestampColumnSource.getLong(nextKey);
                                    if (potentialResetTimestamp != NULL_LONG &&
                                            firstOpCtx.isValueValid(nextKey)) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    rowKey = keyBefore;
                    timestamp = potentialResetTimestamp;
                }
            }

            // Call the specialized version of `intializeUpdate()` for these operators.
            for (int ii = 0; ii < opIndices.length; ii++) {
                final int opIdx = opIndices[ii];
                if (!context.dirtyOperators.get(opIdx)) {
                    // Skip if not dirty.
                    continue;
                }
                UpdateByOperator cumOp = operators[opIdx];
                cumOp.initializeCumulativeWithKeyValues(winOpContexts[ii], rowKey, timestamp, context.sourceRowSet,
                        context.bucketKeyValues);
            }

            while (affectedIt.hasMore()) {
                final RowSequence affectedRs = affectedIt.getNextRowSequenceWithLength(context.workingChunkSize);

                // Create the timestamp chunk if needed.
                LongChunk<? extends Values> tsChunk = context.timestampColumnSource == null ? null
                        : context.timestampColumnSource.getChunk(tsGetContext, affectedRs).asLongChunk();

                // Prep the chunk array needed by the accumulate call.
                for (int ii = 0; ii < srcIndices.length; ii++) {
                    int srcIdx = srcIndices[ii];
                    chunkArr[ii] = context.inputSources[srcIdx].getChunk(chunkContexts[ii], affectedRs);
                }

                // Make the specialized call for windowed operators.
                for (int ii = 0; ii < opIndices.length; ii++) {
                    final int opIdx = opIndices[ii];
                    if (!context.dirtyOperators.get(opIdx)) {
                        // Skip if not dirty.
                        continue;
                    }
                    winOpContexts[ii].accumulateCumulative(
                            affectedRs,
                            chunkArr,
                            tsChunk,
                            affectedRs.intSize());
                }
            }

            // Finalize the operator.
            for (int ii = 0; ii < opIndices.length; ii++) {
                final int opIdx = opIndices[ii];
                if (!context.dirtyOperators.get(opIdx)) {
                    // Skip if not dirty.
                    continue;
                }
                UpdateByOperator cumOp = operators[opIdx];
                cumOp.finishUpdate(winOpContexts[ii]);
            }
        }
    }

    /**
     * Find the smallest valued key that participated in the upstream {@link TableUpdate}.
     *
     * @param upstream the {@link TableUpdate update} from upstream
     * @param affectedRowSet the {@link TrackingRowSet rowset} for the current bucket
     * @param inputModified whether the input columns for this window were modified
     *
     * @return the smallest key that participated in any part of the update. This will be the minimum of the first key
     *         of each of added, modified and removed (post-shift) rows.
     */
    private static long smallestAffectedKey(
            @NotNull final TableUpdate upstream,
            @NotNull final TrackingRowSet affectedRowSet,
            final boolean inputModified) {

        long smallestModifiedKey = Long.MAX_VALUE;
        if (upstream.removed().isNonempty()) {
            // removed rows aren't represented in the shift data, so choose the row immediately preceding the first
            // removed as the removed candidate for smallestAffectedKey
            final long pos = affectedRowSet.findPrev(upstream.removed().firstRowKey());
            if (pos == 0) {
                // the first row was removed, recompute everything
                return affectedRowSet.firstRowKey();
            }

            // get the key previous to this one and shift to post-space
            smallestModifiedKey = affectedRowSet.getPrev(pos - 1);
            if (upstream.shifted().nonempty()) {
                smallestModifiedKey = upstream.shifted().apply(smallestModifiedKey);
            }

            // tighten this up more by advancing one key in the post-shift space. This leaves us with first key
            // following the first remove
            if (smallestModifiedKey < affectedRowSet.lastRowKey()) {
                smallestModifiedKey = affectedRowSet.get(affectedRowSet.find(smallestModifiedKey) + 1);
            } else {
                // all removes are after the end of the current rowset
                smallestModifiedKey = Long.MAX_VALUE;
            }
        }

        if (upstream.added().isNonempty()) {
            smallestModifiedKey = Math.min(smallestModifiedKey, upstream.added().firstRowKey());
        }

        // consider the modifications only when input columns were modified
        if (upstream.modified().isNonempty() && inputModified) {
            smallestModifiedKey = Math.min(smallestModifiedKey, upstream.modified().firstRowKey());
        }

        return smallestModifiedKey;
    }
}

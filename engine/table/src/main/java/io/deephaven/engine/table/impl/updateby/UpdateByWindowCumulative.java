package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
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

    private void makeOperatorContexts(UpdateByWindowBucketContext context) {
        // working chunk size need not be larger than affectedRows.size()
        context.workingChunkSize = Math.toIntExact(Math.min(context.workingChunkSize, context.affectedRows.size()));

        // create contexts for the affected operators
        for (int opIdx : context.dirtyOperatorIndices) {
            context.opContext[opIdx] = operators[opIdx].makeUpdateContext(context.workingChunkSize,
                    operatorInputSourceSlots[opIdx].length);
        }
    }

    @Override
    UpdateByWindowBucketContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final TrackingRowSet timestampValidRowSet,
            final boolean timestampsModified,
            final int chunkSize,
            final boolean isInitializeStep) {
        return new UpdateByWindowBucketContext(sourceRowSet, timestampColumnSource, timestampSsa, timestampValidRowSet,
                timestampsModified, chunkSize, isInitializeStep);
    }

    @Override
    void computeAffectedRowsAndOperators(UpdateByWindowBucketContext context, @NotNull TableUpdate upstream) {
        if (upstream.empty() || context.sourceRowSet.isEmpty()) {
            return;
        }

        // all rows are affected on the initial step
        if (context.initialStep) {
            context.affectedRows = context.sourceRowSet.copy();
            context.influencerRows = context.affectedRows;

            // mark all operators as affected by this update
            context.dirtyOperatorIndices = IntStream.range(0, operators.length).toArray();
            context.dirtySourceIndices = getUniqueSourceIndices();

            makeOperatorContexts(context);
            context.isDirty = !upstream.empty();
            return;
        }

        // determine which operators are affected by this update
        context.isDirty = false;
        boolean allAffected = upstream.added().isNonempty() ||
                upstream.removed().isNonempty();

        if (allAffected) {
            // mark all operators as affected by this update
            context.dirtyOperatorIndices = IntStream.range(0, operators.length).toArray();
            context.dirtySourceIndices = getUniqueSourceIndices();
            context.isDirty = true;
        } else {
            // determine which operators are affected by this update
            BitSet dirtyOperators = new BitSet();
            BitSet dirtySourceIndices = new BitSet();

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                UpdateByOperator op = operators[opIdx];
                if (upstream.modifiedColumnSet().nonempty() && (op.getInputModifiedColumnSet() == null
                        || upstream.modifiedColumnSet().containsAny(op.getInputModifiedColumnSet()))) {
                    dirtyOperators.set(opIdx);
                    Arrays.stream(operatorInputSourceSlots[opIdx]).forEach(dirtySourceIndices::set);
                }
            }
            context.isDirty = !dirtyOperators.isEmpty();
            context.dirtyOperatorIndices = dirtyOperators.stream().toArray();
            context.dirtySourceIndices = dirtySourceIndices.stream().toArray();
        }

        if (!context.isDirty) {
            return;
        }

        long smallestModifiedKey = smallestAffectedKey(upstream, context.sourceRowSet);

        context.affectedRows = smallestModifiedKey == Long.MAX_VALUE
                ? RowSetFactory.empty()
                : context.sourceRowSet.subSetByKeyRange(smallestModifiedKey, context.sourceRowSet.lastRowKey());
        context.influencerRows = context.affectedRows;

        makeOperatorContexts(context);
    }

    @Override
    void processRows(UpdateByWindowBucketContext context, final boolean initialStep) {
        Assert.neqNull(context.inputSources, "assignInputSources() must be called before processRow()");

        if (initialStep) {
            // always at the beginning of the RowSet at creation phase
            for (int opIdx : context.dirtyOperatorIndices) {
                UpdateByCumulativeOperator cumOp = (UpdateByCumulativeOperator) operators[opIdx];
                cumOp.initializeUpdate(context.opContext[opIdx], NULL_ROW_KEY, NULL_LONG);
            }
        } else {
            // find the key before the first affected row
            final long pos = context.sourceRowSet.find(context.affectedRows.firstRowKey());
            final long keyBefore = pos == 0 ? NULL_ROW_KEY : context.sourceRowSet.get(pos - 1);

            // and preload that data for these operators
            for (int opIdx : context.dirtyOperatorIndices) {
                UpdateByCumulativeOperator cumOp = (UpdateByCumulativeOperator) operators[opIdx];
                if (cumOp.getTimestampColumnName() == null || keyBefore == NULL_ROW_KEY) {
                    // this operator doesn't care about timestamps or we know we are at the beginning of the rowset
                    cumOp.initializeUpdate(context.opContext[opIdx], keyBefore, NULL_LONG);
                } else {
                    // this operator cares about timestamps, so make sure it is starting from a valid value and
                    // valid timestamp by looking backward until the conditions are met
                    UpdateByCumulativeOperator.Context cumOpContext =
                            (UpdateByCumulativeOperator.Context) context.opContext[opIdx];
                    long potentialResetTimestamp = context.timestampColumnSource.getLong(keyBefore);

                    if (potentialResetTimestamp == NULL_LONG || !cumOpContext.isValueValid(keyBefore)) {
                        try (final RowSet.SearchIterator rIt = context.sourceRowSet.reverseIterator()) {
                            if (rIt.advance(keyBefore)) {
                                while (rIt.hasNext()) {
                                    final long nextKey = rIt.nextLong();
                                    potentialResetTimestamp = context.timestampColumnSource.getLong(nextKey);
                                    if (potentialResetTimestamp != NULL_LONG &&
                                            cumOpContext.isValueValid(nextKey)) {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    // call the specialized version of `intializeUpdate()` for these operators
                    cumOp.initializeUpdate(context.opContext[opIdx], keyBefore, potentialResetTimestamp);
                }
            }
        }

        try (final RowSequence.Iterator it = context.affectedRows.getRowSequenceIterator();
                ChunkSource.GetContext tsGetCtx =
                        context.timestampColumnSource == null ? null
                                : context.timestampColumnSource.makeGetContext(context.workingChunkSize)) {
            while (it.hasMore()) {
                final RowSequence rs = it.getNextRowSequenceWithLength(context.workingChunkSize);
                final int size = rs.intSize();
                Arrays.fill(context.inputSourceChunks, null);

                // create the timestamp chunk if needed
                LongChunk<? extends Values> tsChunk = context.timestampColumnSource == null ? null
                        : context.timestampColumnSource.getChunk(tsGetCtx, rs).asLongChunk();

                for (int opIdx : context.dirtyOperatorIndices) {
                    UpdateByCumulativeOperator.Context opCtx =
                            (UpdateByCumulativeOperator.Context) context.opContext[opIdx];
                    // prep the chunk array needed by the accumulate call
                    final int[] srcIndices = operatorInputSourceSlots[opIdx];
                    for (int ii = 0; ii < srcIndices.length; ii++) {
                        int srcIdx = srcIndices[ii];
                        // chunk prep
                        prepareValuesChunkForSource(context, srcIdx, rs);
                        opCtx.chunkArr[ii] = context.inputSourceChunks[srcIdx];
                    }

                    // make the specialized call for cumulative operators
                    ((UpdateByCumulativeOperator.Context) context.opContext[opIdx]).accumulate(
                            rs,
                            opCtx.chunkArr,
                            tsChunk,
                            size);
                }
            }
        }

        // call `finishUpdate()` function for each operator
        for (int opIdx : context.dirtyOperatorIndices) {
            operators[opIdx].finishUpdate(context.opContext[opIdx]);
        }
    }


    /**
     * Find the smallest valued key that participated in the upstream {@link TableUpdate}.
     *
     * @param upstream the {@link TableUpdate update} from upstream
     * @param affectedRowSet the {@link TrackingRowSet rowset} for the current bucket
     *
     * @return the smallest key that participated in any part of the update. This will be the minimum of the first key
     *         of each of added, modified and removed (post-shift) rows.
     */
    private static long smallestAffectedKey(@NotNull TableUpdate upstream, @NotNull TrackingRowSet affectedRowSet) {

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
            }
        }

        if (upstream.added().isNonempty()) {
            smallestModifiedKey = Math.min(smallestModifiedKey, upstream.added().firstRowKey());
        }

        if (upstream.modified().isNonempty()) {
            smallestModifiedKey = Math.min(smallestModifiedKey, upstream.modified().firstRowKey());
        }

        return smallestModifiedKey;
    }
}

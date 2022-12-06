package io.deephaven.engine.table.impl.updateby;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.UpdateByCumulativeOperator;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.stream.IntStream;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * This is the specialization of {@link UpdateByWindow} that handles `cumulative` operators. These operators do not
 * maintain a window of data and can be computed from the previous value and the current value.
 */
public class UpdateByWindowCumulative extends UpdateByWindow {

    public UpdateByWindowCumulative(UpdateByOperator[] operators, int[][] operatorSourceSlots,
            @Nullable String timestampColumnName) {
        super(operators, operatorSourceSlots, timestampColumnName);
    }

    @Override
    protected void makeOperatorContexts(UpdateByWindowContext context) {
        // working chunk size need not be larger than affectedRows.size()
        context.workingChunkSize = Math.min(context.workingChunkSize, context.affectedRows.intSize());

        // create contexts for the affected operators
        for (int opIdx : context.dirtyOperatorIndices) {
            context.opContext[opIdx] = operators[opIdx].makeUpdateContext(context.workingChunkSize);
        }
    }

    public UpdateByWindowContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final int chunkSize,
            final boolean isInitializeStep) {
        return new UpdateByWindowContext(sourceRowSet, timestampColumnSource, timestampSsa,
                chunkSize,
                isInitializeStep) {};
    }

    @Override
    public void computeAffectedRowsAndOperators(UpdateByWindowContext context, @NotNull TableUpdate upstream) {

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
            TIntArrayList dirtyOperatorList = new TIntArrayList(operators.length);
            TIntHashSet inputSourcesSet = new TIntHashSet(getUniqueSourceIndices().length);
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                UpdateByOperator op = operators[opIdx];
                if (upstream.modifiedColumnSet().nonempty() && (op.getInputModifiedColumnSet() == null
                        || upstream.modifiedColumnSet().containsAny(op.getInputModifiedColumnSet()))) {
                    dirtyOperatorList.add(opIdx);
                    inputSourcesSet.addAll(operatorInputSourceSlots[opIdx]);
                    context.isDirty = true;
                }
            }
            context.dirtyOperatorIndices = dirtyOperatorList.toArray();
            context.dirtySourceIndices = inputSourcesSet.toArray();
        }

        if (!context.isDirty) {
            return;
        }

        long smallestModifiedKey = smallestAffectedKey(upstream.added(), upstream.modified(), upstream.removed(),
                upstream.shifted(), context.sourceRowSet);

        context.affectedRows = smallestModifiedKey == Long.MAX_VALUE
                ? RowSetFactory.empty()
                : context.sourceRowSet.subSetByKeyRange(smallestModifiedKey, context.sourceRowSet.lastRowKey());
        context.influencerRows = context.affectedRows;

        makeOperatorContexts(context);
    }

    @Override
    public void processRows(UpdateByWindowContext context, final boolean initialStep) {
        Assert.neqNull(context.inputSources, "assignInputSources() must be called before processRow()");

        // find the key before the first affected row
        final long keyBefore;
        try (final RowSet.SearchIterator rIt = context.sourceRowSet.reverseIterator()) {
            rIt.advance(context.affectedRows.firstRowKey());
            if (rIt.hasNext()) {
                keyBefore = rIt.nextLong();
            } else {
                keyBefore = NULL_ROW_KEY;
            }
        }

        // and preload that data for these operators
        for (int opIdx : context.dirtyOperatorIndices) {
            UpdateByCumulativeOperator cumOp = (UpdateByCumulativeOperator) operators[opIdx];
            if (cumOp.getTimestampColumnName() == null || keyBefore == NULL_ROW_KEY) {
                // this operator doesn't care about timestamps or we know we are at the beginning of the rowset
                cumOp.initializeUpdate(context.opContext[opIdx], keyBefore, NULL_LONG);
            } else {
                // this operator cares about timestamps, so make sure it is starting from a valid value and
                // valid timestamp by moving backward until the conditions are met
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

        try (final RowSequence.Iterator it = context.affectedRows.getRowSequenceIterator();
                ChunkSource.GetContext tsGetCtx =
                        context.timestampColumnSource == null ? null
                                : context.timestampColumnSource.makeGetContext(context.workingChunkSize)) {
            while (it.hasMore()) {
                final RowSequence rs = it.getNextRowSequenceWithLength(context.workingChunkSize);
                final int size = rs.intSize();
                Arrays.fill(context.inputSourceChunkPopulated, false);

                // create the timestamp chunk if needed
                LongChunk<? extends Values> tsChunk = context.timestampColumnSource == null ? null
                        : context.timestampColumnSource.getChunk(tsGetCtx, rs).asLongChunk();

                for (int opIdx : context.dirtyOperatorIndices) {
                    // prep the chunk array needed by the accumulate call
                    final int[] srcIndices = operatorInputSourceSlots[opIdx];
                    Chunk<? extends Values>[] chunkArr = new Chunk[srcIndices.length];
                    for (int ii = 0; ii < srcIndices.length; ii++) {
                        int srcIdx = srcIndices[ii];
                        // chunk prep
                        prepareValuesChunkForSource(context, srcIdx, rs);
                        chunkArr[ii] = context.inputSourceChunks[srcIdx];
                    }

                    // make the specialized call for cumulative operators
                    ((UpdateByCumulativeOperator.Context) context.opContext[opIdx]).accumulate(
                            rs,
                            chunkArr,
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
     * @param added the added rows
     * @param modified the modified rows
     * @param removed the removed rows
     * @param shifted the shifted rows
     *
     * @return the smallest key that participated in any part of the update.
     */
    private static long smallestAffectedKey(@NotNull final RowSet added,
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
                for (int shiftIdx = 0; shiftIdx < shifted.size()
                        && (!modShiftFound || !affectedFound); shiftIdx++) {
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

        return smallestModifiedKey;
    }
}

package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.UpdateByCumulativeOperator;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_LONG;

// this class is currently too big, should specialize into CumWindow, TickWindow, TimeWindow to simplify implementation
public class UpdateByWindowCumulative extends UpdateByWindow {
    public class UpdateByWindowCumulativeContext extends UpdateByWindowContext {
        public UpdateByWindowCumulativeContext(final TrackingRowSet sourceRowSet, final ColumnSource<?>[] inputSources,
                @Nullable final ColumnSource<?> timestampColumnSource,
                @Nullable final LongSegmentedSortedArray timestampSsa, final int chunkSize, final boolean initialStep) {
            super(sourceRowSet, inputSources, timestampColumnSource, timestampSsa, chunkSize, initialStep);
        }

        @Override
        public void close() {
            super.close();
        }

        @Override
        public boolean computeAffectedRowsAndOperators(@NotNull final TableUpdate upstream) {
            // all rows are affected on the initial step
            if (initialStep) {
                affectedRows = sourceRowSet.copy();
                influencerRows = affectedRows;

                // mark all operators as affected by this update
                Arrays.fill(opAffected, true);

                makeOperatorContexts();
                return true;
            }

            // determine which operators are affected by this update
            boolean anyAffected = false;
            boolean allAffected = upstream.added().isNonempty() ||
                    upstream.removed().isNonempty();

            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                opAffected[opIdx] = allAffected
                        || (upstream.modifiedColumnSet().nonempty() && (operatorInputModifiedColumnSets[opIdx] == null
                                || upstream.modifiedColumnSet().containsAny(operatorInputModifiedColumnSets[opIdx])));
                if (opAffected[opIdx]) {
                    anyAffected = true;
                }
            }

            if (!anyAffected) {
                return false;
            }

            long smallestModifiedKey = smallestAffectedKey(upstream.added(), upstream.modified(), upstream.removed(),
                    upstream.shifted(), sourceRowSet);

            affectedRows = smallestModifiedKey == Long.MAX_VALUE
                    ? RowSetFactory.empty()
                    : sourceRowSet.subSetByKeyRange(smallestModifiedKey, sourceRowSet.lastRowKey());
            influencerRows = affectedRows;

            makeOperatorContexts();
            return true;
        }

        @Override
        public void processRows() {
            if (trackModifications) {
                modifiedBuilder = RowSetFactory.builderSequential();
            }

            // find the key before the first affected row
            final long keyBefore;
            try (final RowSet.SearchIterator rIt = sourceRowSet.reverseIterator()) {
                rIt.advance(affectedRows.firstRowKey());
                if (rIt.hasNext()) {
                    keyBefore = rIt.nextLong();
                } else {
                    keyBefore = NULL_ROW_KEY;
                }
            }

            // and preload that data for these operators
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    UpdateByCumulativeOperator cumOp = (UpdateByCumulativeOperator) operators[opIdx];
                    if (cumOp.getTimestampColumnName() == null || keyBefore == NULL_ROW_KEY) {
                        // this operator doesn't care about timestamps or we know we are at the beginning of the rowset
                        cumOp.initializeUpdate(opContext[opIdx], keyBefore, NULL_LONG);
                    } else {
                        // this operator cares about timestamps, so make sure it is starting from a valid value and
                        // valid timestamp by moving backward until the conditions are met
                        UpdateByCumulativeOperator.Context cumOpContext =
                                (UpdateByCumulativeOperator.Context) opContext[opIdx];
                        long potentialResetTimestamp = timestampColumnSource.getLong(keyBefore);

                        if (potentialResetTimestamp == NULL_LONG || !cumOpContext.isValueValid(keyBefore)) {
                            try (final RowSet.SearchIterator rIt = sourceRowSet.reverseIterator()) {
                                if (rIt.advance(keyBefore)) {
                                    while (rIt.hasNext()) {
                                        final long nextKey = rIt.nextLong();
                                        potentialResetTimestamp = timestampColumnSource.getLong(nextKey);
                                        if (potentialResetTimestamp != NULL_LONG &&
                                                cumOpContext.isValueValid(nextKey)) {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        // call the specialized version of `intializeUpdate()` for these operators
                        cumOp.initializeUpdate(opContext[opIdx], keyBefore, potentialResetTimestamp);
                    }
                }
            }

            try (final RowSequence.Iterator it = affectedRows.getRowSequenceIterator();
                    ChunkSource.GetContext tsGetCtx =
                            timestampColumnSource == null ? null : timestampColumnSource.makeGetContext(chunkSize)) {
                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(chunkSize);
                    final int size = rs.intSize();
                    Arrays.fill(inputSourceChunkPopulated, false);

                    // create the timestamp chunk if needed
                    LongChunk<? extends Values> tsChunk = timestampColumnSource == null ? null
                            : timestampColumnSource.getChunk(tsGetCtx, rs).asLongChunk();

                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                        if (opAffected[opIdx]) {
                            final int srcIdx = operatorSourceSlots[opIdx];

                            // chunk prep
                            prepareValuesChunkForSource(srcIdx, rs);

                            // make the specialized call for cumulative operators
                            ((UpdateByCumulativeOperator.Context) opContext[opIdx]).accumulate(
                                    rs,
                                    inputSourceChunks[srcIdx],
                                    tsChunk,
                                    size);
                        }
                    }

                    // all these rows were modified
                    if (modifiedBuilder != null) {
                        modifiedBuilder.appendRowSequence(rs);
                    }
                }
            }

            // call `finishUpdate()` function for each operator
            for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                if (opAffected[opIdx]) {
                    operators[opIdx].finishUpdate(opContext[opIdx]);
                }
            }

            if (trackModifications) {
                newModified = modifiedBuilder.build();
            }
        }
    }

    public UpdateByWindowContext makeWindowContext(final TrackingRowSet sourceRowSet,
            final ColumnSource<?>[] inputSources,
            final ColumnSource<?> timestampColumnSource,
            final LongSegmentedSortedArray timestampSsa,
            final int chunkSize,
            final boolean isInitializeStep) {
        return new UpdateByWindowCumulativeContext(sourceRowSet, inputSources, timestampColumnSource, timestampSsa,
                chunkSize,
                isInitializeStep);
    }

    public void startTrackingModifications(@NotNull final QueryTable source, @NotNull final QueryTable result) {
        trackModifications = true;
        for (int opIdx = 0; opIdx < operators.length; opIdx++) {
            operatorInputModifiedColumnSets[opIdx] =
                    source.newModifiedColumnSet(operators[opIdx].getAffectingColumnNames());
            operatorOutputModifiedColumnSets[opIdx] =
                    result.newModifiedColumnSet(operators[opIdx].getOutputColumnNames());
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

    public UpdateByWindowCumulative(UpdateByOperator[] operators, int[] operatorSourceSlots,
            @Nullable String timestampColumnName) {
        super(operators, operatorSourceSlots, timestampColumnName);
    }

    @Override
    public int hashCode() {
        return hashCode(false,
                timestampColumnName,
                0L,
                0L);
    }
}

//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ResettableWritableLongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.BitShiftingColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupableColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.mutable.MutableLong;
import io.deephaven.vector.Vector;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * The ungroup operation implements {@link Table#ungroup()}.
 */
public class UngroupOperation implements QueryTable.MemoizableOperation<QueryTable> {
    static final int CHUNK_SIZE =
            Configuration.getInstance().getIntegerWithDefault("UngroupOperation.chunkSize", 1 << 10);

    private final QueryTable parent;
    private final boolean nullFill;
    private final String[] columnsToUngroupBy;
    /**
     * A modified column set indicating that any of our sizes may have changed.
     */
    private final ModifiedColumnSet allUngroupColumns;
    /**
     * An array parallel to columnsToUngroupBy of a modified column set for each input.
     */
    private final ModifiedColumnSet[] ungroupColumnModifiedColumnSets;

    final ColumnSource<?>[] ungroupSources;
    final UngroupableColumnSource[] ungroupableColumnSources;
    final boolean anyUngroupable;
    final boolean allUngroupable;
    final UngroupSizeKernel[] sizeKernels;

    public UngroupOperation(final QueryTable parent, final boolean nullFill, final String[] columnsToUngroupBy) {
        this.parent = parent;
        this.nullFill = nullFill;
        this.columnsToUngroupBy = columnsToUngroupBy;

        ungroupSources = new ColumnSource[columnsToUngroupBy.length];
        ungroupableColumnSources = new UngroupableColumnSource[columnsToUngroupBy.length];
        ungroupColumnModifiedColumnSets = new ModifiedColumnSet[columnsToUngroupBy.length];
        sizeKernels = new UngroupSizeKernel[columnsToUngroupBy.length];

        boolean localAnyUngroupable = false;
        boolean localAllUngroupable = true;

        for (int columnIndex = 0; columnIndex < columnsToUngroupBy.length; columnIndex++) {
            final String name = columnsToUngroupBy[columnIndex];
            final ColumnSource<?> column = parent.getColumnSource(name);
            if (UngroupableColumnSource.isUngroupable(column)) {
                ungroupableColumnSources[columnIndex] = (UngroupableColumnSource) column;
                localAnyUngroupable = true;
            } else {
                localAllUngroupable = false;
            }
            ungroupSources[columnIndex] = column;
            if (column.getType().isArray()) {
                sizeKernels[columnIndex] = UngroupSizeKernel.ArraySizeKernel.INSTANCE;
            } else if (Vector.class.isAssignableFrom(column.getType())) {
                sizeKernels[columnIndex] = UngroupSizeKernel.VectorSizeKernel.INSTANCE;
            } else {
                throw new InvalidColumnException("Column " + name + " is not an array or Vector");
            }
            ungroupColumnModifiedColumnSets[columnIndex] = parent.newModifiedColumnSet(columnsToUngroupBy[columnIndex]);
        }
        allUngroupColumns = parent.newModifiedColumnSet(columnsToUngroupBy);
        anyUngroupable = localAnyUngroupable;
        allUngroupable = localAllUngroupable;
    }

    @Override
    public MemoizedOperationKey getMemoizedOperationKey() {
        return MemoizedOperationKey.ungroup(nullFill, columnsToUngroupBy);
    }

    @Override
    public String getDescription() {
        return "ungroup(" + (nullFill ? "true, " : "") + Arrays.toString(columnsToUngroupBy) + ")";
    }

    @Override
    public String getLogPrefix() {
        return "ungroup";
    }

    @Override
    public Result<QueryTable> initialize(final boolean usePrev, final long beforeClock) {

        final long[] sizes = new long[parent.intSize("ungroup")];
        final TrackingRowSet parentRowset = parent.getRowSet();

        final RowSet initRowSet = usePrev ? parentRowset.prev() : parentRowset;

        final long maxSize = computeMaxSize(initRowSet, sizes, usePrev, null);
        final int initialBase = Math.max(determineRequiredBase(maxSize), QueryTable.minimumUngroupBase);

        final CrossJoinShiftState shiftState = new CrossJoinShiftState(initialBase, true);

        final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();
        final Set<String> ungroupNames = new HashSet<>(Arrays.asList(columnsToUngroupBy));
        for (final Map.Entry<String, ColumnSource<?>> es : parent.getColumnSourceMap().entrySet()) {
            final ColumnSource<?> column = es.getValue();
            final String name = es.getKey();
            final ColumnSource<?> result;
            if (ungroupNames.contains(name)) {
                final UngroupedColumnSource<?> ungroupedSource = UngroupedColumnSource.getColumnSource(column);
                ungroupedSource.initializeBase(initialBase);
                result = ungroupedSource;
            } else {
                result = BitShiftingColumnSource.maybeWrap(shiftState, column);
            }
            resultMap.put(name, result);
        }
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        getUngroupRowset(sizes, builder, initialBase, initRowSet);
        final TrackingWritableRowSet resultRowset = builder.build().toTracking();
        final QueryTable result = new QueryTable(resultRowset, resultMap);
        if (parent.isBlink()) {
            result.setAttribute(BaseTable.BLINK_TABLE_ATTRIBUTE, Boolean.TRUE);
        }

        final BaseTable.ListenerImpl listener;
        if (parent.isRefreshing()) {
            QueryTable.startTrackingPrev(resultMap.values());
            listener = new UngroupListener(result, shiftState, parentRowset, resultMap);
        } else {
            listener = null;
        }
        return new Result<>(result, listener);
    }

    /**
     * For a given maximum size of each row, determine the necessary base.
     * 
     * @param maxSize the maximum size of the rows
     * @return the required base to accomodate maxSize
     */
    private static int determineRequiredBase(final long maxSize) {
        if (maxSize == 0) {
            // to avoid edge cases we always assign one value; even though we should have an empty rowset
            return 1;
        }
        // if we have a size of e.g. 4, then we should determine how many leading zeros are present for 3; because we
        // should allow our ranges to be contiguous
        return 64 - Long.numberOfLeadingZeros(maxSize - 1);
    }

    /**
     * Compute the maximum size for the given input rowset.
     * 
     * @param rowSet the rowset to compute output sizes for
     * @param sizes an array to fill with sizes; if computeSizeForColumn is not-null, then this array already contains
     *        the expected sizes for each element.
     * @param usePrev should we use previous values for the input columns
     * @param computeSizeForColumn an array of columns that we must compute sizes for, or {@code null} if we must
     *        compute sizes for all array columns
     * @return the maximum size of an element, not valid when {@code computeSizeForColumn != null}
     */
    private long computeMaxSize(final RowSet rowSet,
            final long[] sizes,
            final boolean usePrev,
            @Nullable final boolean[] computeSizeForColumn) {
        long maxSize = 0;

        final int chunkSize = (int) Math.min(CHUNK_SIZE, rowSet.size());
        String referenceColumn = null;

        final ChunkSource.GetContext[] getContexts =
                allUngroupable ? null : new ChunkSource.GetContext[columnsToUngroupBy.length];
        final ChunkSource.FillContext[] fillContexts =
                anyUngroupable ? new ChunkSource.FillContext[columnsToUngroupBy.length] : null;
        try (final SafeCloseableArray<ChunkSource.GetContext> ignored =
                allUngroupable ? null : new SafeCloseableArray<>(getContexts);
                final SafeCloseableArray<ChunkSource.FillContext> ignored2 =
                        anyUngroupable ? new SafeCloseableArray<>(fillContexts) : null;
                final SharedContext sharedContext = SharedContext.makeSharedContext();
                final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                final WritableLongChunk<Values> currentSizes =
                        anyUngroupable ? WritableLongChunk.makeWritableChunk(chunkSize) : null;
                final ResettableWritableLongChunk<Values> resettable =
                        anyUngroupable ? ResettableWritableLongChunk.makeResettableChunk() : null) {

            for (int ci = 0; ci < ungroupSources.length; ++ci) {
                if (computeSizeForColumn != null && !computeSizeForColumn[ci]) {
                    if (referenceColumn == null) {
                        referenceColumn = columnsToUngroupBy[ci];
                    }
                    continue;
                }
                if (ungroupableColumnSources[ci] == null) {
                    // noinspection resource,DataFlowIssue
                    getContexts[ci] = ungroupSources[ci].makeGetContext(chunkSize);
                } else {
                    // noinspection resource,DataFlowIssue
                    fillContexts[ci] = ungroupSources[ci].makeFillContext(chunkSize);
                }
            }
            if (referenceColumn == null) {
                referenceColumn = columnsToUngroupBy[0];
            }

            int offset = 0;
            while (rsit.hasMore()) {
                final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(chunkSize);
                final int actualChunkSize = rsChunk.intSize();
                sharedContext.reset();

                for (int columnIndex = 0; columnIndex < ungroupSources.length; columnIndex++) {
                    if (computeSizeForColumn != null && !computeSizeForColumn[columnIndex]) {
                        continue;
                    }

                    final ColumnSource<?> arrayColumn = ungroupSources[columnIndex];
                    final String name = columnsToUngroupBy[columnIndex];
                    final UngroupableColumnSource ungroupable = ungroupableColumnSources[columnIndex];

                    if (ungroupable == null) {
                        final Chunk<? extends Values> chunk =
                                usePrev ? arrayColumn.getPrevChunk(getContexts[columnIndex], rsChunk)
                                        : arrayColumn.getChunk(getContexts[columnIndex], rsChunk);
                        if (columnIndex == 0 && computeSizeForColumn == null) {
                            maxSize = Math.max(sizeKernels[0].maxSize(chunk.asObjectChunk(), sizes, offset), maxSize);
                        } else if (nullFill) {
                            final long maxSizeForChunk =
                                    sizeKernels[columnIndex].maybeIncreaseSize(chunk.asObjectChunk(), sizes, offset);
                            maxSize = Math.max(maxSizeForChunk, maxSize);
                        } else {
                            final MutableLong mismatchedSize = new MutableLong();
                            final int firstDifferenceChunkOffset =
                                    sizeKernels[columnIndex].checkSize(chunk.asObjectChunk(), sizes, offset,
                                            mismatchedSize);
                            if (firstDifferenceChunkOffset >= 0) {
                                try (final RowSet rowset = rsChunk.asRowSet()) {
                                    final long correspondingRowKey = rowset.get(firstDifferenceChunkOffset);
                                    final long positionInTable;
                                    if (usePrev) {
                                        positionInTable = parent.getRowSet().findPrev(correspondingRowKey);
                                    } else {
                                        positionInTable = parent.getRowSet().find(correspondingRowKey);
                                    }
                                    final String message = String.format(
                                            "Array sizes differ at row key %d (%sposition %d), %s has size %d, %s has size %d",
                                            correspondingRowKey, usePrev ? "previous " : "", positionInTable,
                                            referenceColumn,
                                            sizes[offset + firstDifferenceChunkOffset], name, mismatchedSize.get());
                                    throw new IllegalStateException(message);
                                }
                            }
                        }
                    } else if (columnIndex == 0 && computeSizeForColumn == null) {
                        resettable.resetFromArray(sizes, offset, actualChunkSize);
                        if (usePrev) {
                            ungroupableColumnSources[columnIndex].getUngroupedPrevSize(fillContexts[columnIndex],
                                    rsChunk, resettable);
                        } else {
                            ungroupableColumnSources[columnIndex].getUngroupedSize(fillContexts[columnIndex], rsChunk,
                                    resettable);
                        }

                        for (int ii = 0; ii < actualChunkSize; ii++) {
                            maxSize = Math.max(resettable.get(ii), maxSize);
                        }
                    } else {
                        if (usePrev) {
                            ungroupableColumnSources[columnIndex].getUngroupedPrevSize(fillContexts[columnIndex],
                                    rsChunk, currentSizes);
                        } else {
                            ungroupableColumnSources[columnIndex].getUngroupedSize(fillContexts[columnIndex], rsChunk,
                                    currentSizes);
                        }

                        for (int ii = 0; ii < actualChunkSize; ii++) {
                            final long currentSize = currentSizes.get(ii);
                            if (nullFill) {
                                if (currentSize > sizes[offset + ii]) {
                                    sizes[offset + ii] = currentSize;
                                    maxSize = Math.max(currentSize, maxSize);
                                }
                            } else if (currentSize != sizes[offset + ii]) {
                                try (final RowSet rowset = rsChunk.asRowSet()) {
                                    final long correspondingRowKey = rowset.get(ii);
                                    final long positionInTable;
                                    if (usePrev) {
                                        positionInTable = parent.getRowSet().findPrev(correspondingRowKey);
                                    } else {
                                        positionInTable = parent.getRowSet().find(correspondingRowKey);
                                    }
                                    final String message = String.format(
                                            "Array sizes differ at row key %d (%sposition %d), %s has size %d, %s has size %d",
                                            correspondingRowKey, usePrev ? "previous " : "", positionInTable,
                                            referenceColumn,
                                            sizes[offset + ii], name, currentSize);
                                    throw new IllegalStateException(message);
                                }
                            }
                        }
                    }
                }

                offset += rsChunk.intSize();
            }
        }

        return maxSize;
    }

    /**
     * Compute the size of the elements referenced by rowsetInParent using the RowSet of our result table; both of these
     * values must be in the same coordinate space (previous for our use cases).
     *
     * @param resultRowSet the result rowset
     * @param base the base of the result rowset to translate to source row keys
     * @param rowsetInParent the rowset in the parent table to compute sizes for
     * @param sizes an array to fill with sizes
     */
    private void computeSizeFromResultRowset(final RowSet resultRowSet,
            final int base,
            final RowSet rowsetInParent,
            final long[] sizes) {
        Assert.assertion(rowsetInParent.isNonempty(), "rowsetInParent.isNonempty()");

        final RowSet.RangeIterator prevIt = resultRowSet.rangeIterator();
        final RowSet.Iterator parentIt = rowsetInParent.iterator();
        long nextKey = 0;

        for (int pos = 0; pos < sizes.length; ++pos) {
            final long parentKey = parentIt.nextLong();
            if (parentKey < nextKey) {
                sizes[pos] = 0;
                continue;
            }

            final long firstKeyForRow = parentKey << base;
            final long lastKeyForRow = ((parentKey + 1) << base) - 1;
            if (!prevIt.advance(firstKeyForRow)) {
                // the rest of the sizes must be zero, because there is nothing else in our result rowset
                Arrays.fill(sizes, pos, sizes.length, 0);
                break;
            }

            if (prevIt.currentRangeStart() == firstKeyForRow) {
                final long actualEnd = Math.min(prevIt.currentRangeEnd(), lastKeyForRow);
                sizes[pos] = actualEnd - firstKeyForRow + 1;
            } else {
                sizes[pos] = 0;
                // we should be in some other row
                Assert.gt(prevIt.currentRangeStart(), "prevIt.currentRangeStart()", lastKeyForRow, "lastRowForKey");
                nextKey = prevIt.currentRangeStart() >> base;
            }
        }
    }

    private void getUngroupRowset(final long[] sizes, final RowSetBuilderSequential builder, final long base,
            final RowSet rowSet) {
        Assert.geqZero(base, "base");
        Assert.leq(base, "base", 62);
        final long lastKey = rowSet.lastRowKey();
        if (lastKey >= 0) {
            final long lastSlotStart = lastKey << base;
            final long lastSlotRestored = lastSlotStart >> base;
            final long slotSize = 1L << base;
            if (lastSlotRestored != lastKey || (lastSlotStart + slotSize) < 0) {
                throw new IllegalStateException(
                        "Key overflow detected, perhaps you should flatten your table before calling ungroup: lastRowKey="
                                + lastKey + ", base=" + base);
            }
        }

        int pos = 0;
        for (final RowSet.Iterator iterator = rowSet.iterator(); iterator.hasNext();) {
            final long next = iterator.nextLong();
            final long nextShifted = next << base;
            final long arraySize = sizes[pos++];
            if (arraySize != 0) {
                builder.appendRange(nextShifted, nextShifted + arraySize - 1);
            }
        }
    }

    private class UngroupListener extends BaseTable.ListenerImpl {

        private final QueryTable result;
        private final CrossJoinShiftState shiftState;
        private final TrackingRowSet parentRowset;
        private final Map<String, ColumnSource<?>> resultMap;
        private final ModifiedColumnSet.Transformer transformer;

        public UngroupListener(final QueryTable result, final CrossJoinShiftState shiftState,
                final TrackingRowSet parentRowset,
                final Map<String, ColumnSource<?>> resultMap) {
            super("ungroup(" + getDescription() + ')', UngroupOperation.this.parent, result);
            this.result = result;
            this.shiftState = shiftState;
            this.parentRowset = parentRowset;
            this.resultMap = resultMap;
            this.transformer = parent.newModifiedColumnSetIdentityTransformer(result);
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            parent.intSize("ungroup");

            if (upstream.removed().size() == parent.getRowSet().sizePrev()) {
                // our new base can safely be the minimum base; as everything has been removed from this table. This
                // is convenient to allow the base to shrink in some circumstances.
                clearAndRecompute(upstream);
                return;
            }

            final RowSetBuilderSequential added = RowSetFactory.builderSequential();
            final RowSetBuilderSequential removed = RowSetFactory.builderSequential();

            final RowSetBuilderSequential addedByModifies = RowSetFactory.builderSequential();
            final RowSetBuilderSequential removedByModifies = RowSetFactory.builderSequential();
            final RowSetBuilderSequential modifiedByModifies = RowSetFactory.builderSequential();

            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();

            final MutableObject<long[]> modifiedCurrentSizes = new MutableObject<>();

            int requiredBase = evaluateAdded(upstream.added(), added, shiftState.getNumShiftBits());
            final int addedBase = requiredBase;
            if (requiredBase == shiftState.getNumShiftBits()) {
                requiredBase = evaluateModified(upstream,
                        addedByModifies,
                        removedByModifies,
                        modifiedByModifies,
                        shiftState.getNumShiftBits(),
                        modifiedCurrentSizes);
            } else if (upstream.modified().isNonempty()) {
                modifiedCurrentSizes.setValue(new long[upstream.modified().intSize()]);

                final boolean allArraysModified = upstream.modifiedColumnSet().containsAll(allUngroupColumns);
                if (!allArraysModified && !nullFill) {
                    // cannot increase base when we have not modified all the arrays
                    computeSizeFromResultRowset(result.getRowSet(), shiftState.getPrevNumShiftBits(),
                            upstream.getModifiedPreShift(), modifiedCurrentSizes.get());
                } else {
                    // need to compute all values across the arrays
                    final long maxModSize =
                            computeMaxSize(upstream.modified(), modifiedCurrentSizes.get(), false, null);
                    requiredBase = Math.max(determineRequiredBase(maxModSize), requiredBase);
                }
            }
            if (requiredBase != shiftState.getNumShiftBits()) {
                try (final WritableRowSet addedRowsetInOldBase = added.build()) {
                    rebase(requiredBase, upstream, addedRowsetInOldBase, addedBase, modifiedCurrentSizes.get());
                }
                return;
            }

            evaluateRemoved(upstream.removed(), removed);

            final WritableRowSet addedRowSet = added.build();
            final WritableRowSet removedRowSet = removed.build();

            try (final WritableRowSet built = addedByModifies.build()) {
                addedRowSet.insert(built);
            }
            try (final WritableRowSet built = removedByModifies.build()) {
                removedRowSet.insert(built);
            }

            final TrackingWritableRowSet resultRowset = result.getRowSet().writableCast();
            resultRowset.remove(removedRowSet);

            final RowSetBuilderRandom removedByShiftBuilder = RowSetFactory.builderRandom();
            final RowSetBuilderRandom addedByShiftBuilder = RowSetFactory.builderRandom();
            processShifts(upstream, resultRowset, requiredBase, shiftBuilder, removedByShiftBuilder,
                    addedByShiftBuilder);

            try (final WritableRowSet built = removedByShiftBuilder.build()) {
                resultRowset.remove(built);
            }
            try (final WritableRowSet built = addedByShiftBuilder.build()) {
                resultRowset.insert(built);
            }
            resultRowset.insert(addedRowSet);

            final RowSet modifiedRowSet = modifiedByModifies.build();
            transformer.clearAndTransform(upstream.modifiedColumnSet(), result.getModifiedColumnSetForUpdates());
            final TableUpdateImpl downstream = new TableUpdateImpl(addedRowSet, removedRowSet, modifiedRowSet,
                    shiftBuilder.build(), result.getModifiedColumnSetForUpdates());
            result.notifyListeners(downstream);
        }

        private void processShifts(final TableUpdate upstream,
                final TrackingWritableRowSet resultRowset,
                final int base,
                final RowSetShiftData.Builder shiftBuilder,
                final RowSetBuilderRandom removedByShiftBuilder,
                final RowSetBuilderRandom addedByShiftBuilder) {
            if (upstream.shifted().empty()) {
                return;
            }

            try (final RowSet.RangeIterator rit = resultRowset.rangeIterator()) {
                // there must be at least one thing in the result rowset, otherwise we would have followed the clear
                // logic instead of this logic
                rit.next();
                long nextResultKey = rit.currentRangeStart();
                long nextSourceKey = nextResultKey >> base;

                final long lastRowKeyOffset = (1L << base) - 1;

                final RowSetShiftData upstreamShift = upstream.shifted();
                final long shiftSize = upstreamShift.size();
                for (int ii = 0; ii < shiftSize; ++ii) {
                    final long begin = upstreamShift.getBeginRange(ii);
                    final long end = upstreamShift.getEndRange(ii);
                    final long shiftDelta = upstreamShift.getShiftDelta(ii);

                    final long resultShiftAmount = shiftDelta << (long) base;

                    for (long rk = Math.max(begin, nextSourceKey); rk <= end; rk =
                            Math.max(rk + 1, nextSourceKey)) {
                        final long oldRangeStart = rk << (long) base;
                        final long oldRangeEnd = oldRangeStart + lastRowKeyOffset;

                        if (!rit.advance(oldRangeStart)) {
                            return;
                        }
                        // we successfully advanced
                        nextResultKey = rit.currentRangeStart();
                        nextSourceKey = nextResultKey >> base;

                        if (nextResultKey > oldRangeEnd) {
                            // no need to shift things that don't exist
                            continue;
                        }

                        // get the range from our result rowset
                        Assert.eq(oldRangeStart, "oldRangeStart", rit.currentRangeStart(),
                                "rsit.currentRangeStart()");
                        final long actualRangeEnd = Math.min(rit.currentRangeEnd(), oldRangeEnd);
                        shiftBuilder.shiftRange(oldRangeStart, actualRangeEnd, resultShiftAmount);
                        removedByShiftBuilder.addRange(oldRangeStart, actualRangeEnd);
                        addedByShiftBuilder.addRange(oldRangeStart + resultShiftAmount,
                                actualRangeEnd + resultShiftAmount);
                    }
                }
            }
        }

        private void rebase(final int newBase,
                final TableUpdate upstream,
                final WritableRowSet addedRowSet,
                final int addedRowsetBase,
                final long[] modifiedCurrentSizes) {
            final RowSetBuilderSequential removedBuilder = RowSetFactory.builderSequential();

            final RowSetBuilderSequential addedByModifiesBuilder = RowSetFactory.builderSequential();
            final RowSetBuilderSequential removedByModifiesBuilder = RowSetFactory.builderSequential();
            final RowSetBuilderSequential modifiedByModifiesBuilder = RowSetFactory.builderSequential();

            final WritableRowSet addedInNewBaseRowSet;
            if (addedRowsetBase == newBase) {
                addedInNewBaseRowSet = addedRowSet.copy();
            } else {
                final RowSetBuilderSequential addedBuilder = RowSetFactory.builderSequential();

                final long slotMaxSize = 1L << addedRowsetBase;
                final RowSet.RangeIterator rit = addedRowSet.rangeIterator();
                while (rit.hasNext()) {
                    rit.next();
                    final long rangeStart = rit.currentRangeStart();
                    final long rangeEnd = rit.currentRangeEnd();

                    for (long rk = rangeStart; rk <= rangeEnd; rk += slotMaxSize) {
                        final long slotSize = Math.min(rangeEnd, rk + slotMaxSize) - rk + 1;
                        final long sourceKey = rk >> addedRowsetBase;
                        final long newSourceKey = sourceKey << newBase;
                        addedBuilder.appendRange(newSourceKey, newSourceKey + slotSize - 1);
                    }
                }
                addedInNewBaseRowSet = addedBuilder.build();
            }

            final int computedModifiedBase = evaluateModified(upstream,
                    addedByModifiesBuilder,
                    removedByModifiesBuilder,
                    modifiedByModifiesBuilder,
                    newBase,
                    new MutableObject<>(modifiedCurrentSizes));
            Assert.leq(computedModifiedBase, "computedAddBase", newBase, "newBase");

            evaluateRemoved(upstream.removed(), removedBuilder);

            final WritableRowSet removedRowSet = removedBuilder.build();
            try (final WritableRowSet removedByModifies = removedByModifiesBuilder.build()) {
                removedRowSet.insert(removedByModifies);
            }
            // we want to remove everything that is no longer relevant for our shift
            final TrackingWritableRowSet resultRowset = result.getRowSet().writableCast();
            resultRowset.remove(removedRowSet);

            final int prevBase = shiftState.getPrevNumShiftBits();
            final int maxOldSlotSize = 1 << prevBase;

            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();

            final ShiftWrapper shiftWrapper = new ShiftWrapper(upstream.shifted());

            try (final RowSet.RangeIterator rangeIterator = result.getRowSet().rangeIterator()) {
                // everything that exists needs to be shifted
                while (rangeIterator.hasNext()) {
                    rangeIterator.next();

                    final long startRange = rangeIterator.currentRangeStart();
                    final long endRange = rangeIterator.currentRangeEnd();

                    for (long oldSlotStart = startRange; oldSlotStart <= endRange; oldSlotStart += maxOldSlotSize) {
                        final long oldSlotEnd = Math.min(oldSlotStart + maxOldSlotSize - 1, endRange);
                        final long oldSlotSize = oldSlotEnd - oldSlotStart + 1;

                        final long oldSourceKey = oldSlotStart >> prevBase;
                        final long newSourceKey;

                        if (shiftWrapper.advance(oldSourceKey)) {
                            newSourceKey = oldSourceKey + shiftWrapper.currentShiftDelta();
                        } else {
                            newSourceKey = oldSourceKey;
                        }

                        final long newSlotStart = newSourceKey << newBase;
                        final long newSlotEnd = newSlotStart + oldSlotSize - 1;

                        shiftBuilder.shiftRange(oldSlotStart, oldSlotEnd, newSlotStart - oldSlotStart);
                        builder.appendRange(newSlotStart, newSlotEnd);
                    }
                }
            }
            try (final WritableRowSet built = builder.build()) {
                resultRowset.resetTo(built);
            }
            try (final RowSet addedByModifies = addedByModifiesBuilder.build()) {
                addedInNewBaseRowSet.insert(addedByModifies);
            }

            resultRowset.insert(addedInNewBaseRowSet);

            setNewBase(newBase);

            final RowSet modifiedRowSet = modifiedByModifiesBuilder.build();
            transformer.clearAndTransform(upstream.modifiedColumnSet(), result.getModifiedColumnSetForUpdates());
            final TableUpdateImpl downstream = new TableUpdateImpl(addedInNewBaseRowSet, removedRowSet, modifiedRowSet,
                    shiftBuilder.build(), result.getModifiedColumnSetForUpdates());
            result.notifyListeners(downstream);
        }

        private class ShiftWrapper {
            RowSetShiftData shiftData;
            int pos = -1;
            long nextKey;

            ShiftWrapper(final RowSetShiftData shiftData) {
                this.shiftData = shiftData;
                if (shiftData.nonempty()) {
                    pos = 0;
                    nextKey = shiftData.getBeginRange(pos);
                } else {
                    nextKey = RowSet.NULL_ROW_KEY;
                }
            }

            long currentShiftDelta() {
                return shiftData.getShiftDelta(pos);
            }

            /**
             * Advance the shift data to the given key.
             * 
             * @param advanceTo the key to advance to
             * @return true if the shift data is positioned at the desired key, false otherwise
             */
            boolean advance(final long advanceTo) {
                if (nextKey == RowSet.NULL_ROW_KEY) {
                    // already at the end
                    return false;
                }
                if (advanceTo <= nextKey) {
                    // already at the right place
                    return advanceTo == nextKey;
                }

                if (advanceTo <= shiftData.getEndRange(pos)) {
                    Assert.geq(advanceTo, "advanceTo", shiftData.getBeginRange(pos), "shiftData.getBeginRange(pos)");
                    nextKey = advanceTo;
                    return true;
                }

                while (pos < shiftData.size()) {
                    if (advanceTo <= shiftData.getEndRange(pos)) {
                        nextKey = Math.max(shiftData.getBeginRange(pos), advanceTo);
                        return nextKey == advanceTo;
                    }
                    pos++;
                }
                nextKey = RowSet.NULL_ROW_KEY;
                return false;
            }
        }

        private void clearAndRecompute(final TableUpdate upstream) {
            final RowSetBuilderSequential added = RowSetFactory.builderSequential();
            final int newBase = evaluateAdded(upstream.added(), added, QueryTable.minimumUngroupBase);

            final WritableRowSet newRowset = added.build();
            final TrackingWritableRowSet resultRowset = result.getRowSet().writableCast();
            resultRowset.resetTo(newRowset);

            setNewBase(newBase);

            result.notifyListeners(new TableUpdateImpl(newRowset, resultRowset.copyPrev(), RowSetFactory.empty(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        }

        private void setNewBase(final int newBase) {
            for (final ColumnSource<?> source : resultMap.values()) {
                if (source instanceof UngroupedColumnSource) {
                    ((UngroupedColumnSource<?>) source).setBase(newBase);
                }
            }
            shiftState.setNumShiftBitsAndUpdatePrev(newBase);
        }

        private int evaluateAdded(final RowSet rowSet,
                @NotNull final RowSetBuilderSequential ungroupBuilder,
                final int existingBase) {
            if (rowSet.isEmpty()) {
                return existingBase;
            }
            final long[] sizes = new long[rowSet.intSize("ungroup")];
            final long maxSize = computeMaxSize(rowSet, sizes, false, null);
            final int minBase = determineRequiredBase(maxSize);

            final int resultBase = Math.max(existingBase, minBase);
            getUngroupRowset(sizes, ungroupBuilder, resultBase, rowSet);
            return resultBase;
        }

        private void evaluateRemoved(final RowSet rowSet, final RowSetBuilderSequential builder) {
            if (rowSet.isEmpty()) {
                return;
            }
            final long[] sizes = new long[rowSet.intSize("ungroup")];
            computeSizeFromResultRowset(result.getRowSet().prev(), shiftState.getPrevNumShiftBits(), rowSet, sizes);
            getUngroupRowset(sizes, builder, shiftState.getPrevNumShiftBits(), rowSet);
        }

        /**
         *
         * @param upstream the upstream table update
         * @param addedBuilder the builder for rows added by modifications
         * @param removedBuilder the builder for rows removed by modifications
         * @param modifyBuilder the builder for rows modified by modifications
         * @param currentSizes an input parameter if non-null, else an output parameter to allow re-use of retrieved
         *        sizes when we determine a change of base is necessary
         * @return the necessary base
         */
        private int evaluateModified(final TableUpdate upstream,
                final RowSetBuilderSequential addedBuilder,
                final RowSetBuilderSequential removedBuilder,
                final RowSetBuilderSequential modifyBuilder,
                final int base,
                final MutableObject<long[]> currentSizes) {
            final RowSet modified = upstream.modified();
            if (modified.isEmpty()) {
                return base;
            }

            final int prevBase = shiftState.getPrevNumShiftBits();
            final boolean noModifiedArrays = !upstream.modifiedColumnSet().containsAny(allUngroupColumns);
            final boolean allArraysModified = upstream.modifiedColumnSet().containsAll(allUngroupColumns);

            final RowSet modifiedPreShift = upstream.getModifiedPreShift();
            final int size = modified.intSize("ungroup");
            final long[] sizes;
            long[] prevSizes = null;
            if (currentSizes.get() == null) {
                sizes = new long[size];
                if (noModifiedArrays) {
                    computeSizeFromResultRowset(result.getRowSet(), base, modifiedPreShift, sizes);
                    prevSizes = sizes;
                } else {
                    final long maxSize;
                    if (allArraysModified || nullFill) {
                        maxSize = computeMaxSize(modified, sizes, false, null);
                    } else {
                        // make an array of booleans for columns we will recompute
                        final boolean[] computeSizeForColumn = getColumnsToRecompute(upstream);
                        // get the reference size from our previous index
                        computeSizeFromResultRowset(result.getRowSet(), prevBase, modifiedPreShift, sizes);
                        prevSizes = sizes;
                        // now actually compute the max
                        maxSize = computeMaxSize(modified, sizes, false, computeSizeForColumn);
                    }
                    final int minBase = determineRequiredBase(maxSize);
                    if (minBase > base) {
                        // If we are going to force a rebase, there is no need to compute the entire rowset, but the
                        // caller would like our copy of the sizes we computed
                        currentSizes.setValue(sizes);
                        return minBase;
                    }
                }
            } else {
                sizes = currentSizes.get();
                Assert.eq(sizes.length, "sizes.length", size, "modified.intSize()");
                // noinspection OptionalGetWithoutIsPresent
                final long maxSize = Arrays.stream(sizes).max().getAsLong();
                final int minBase = determineRequiredBase(maxSize);
                Assert.leq(minBase, "minBase", base, "base");
            }

            // This function is used from both the normal and the rebase path.
            if (prevSizes == null) {
                prevSizes = new long[size];
                computeSizeFromResultRowset(result.getRowSet().prev(), prevBase, modifiedPreShift, prevSizes);
            }

            final RowSet.Iterator iterator = modified.iterator();
            final RowSet.Iterator iteratorPreShift = modifiedPreShift.iterator();
            for (int idx = 0; idx < size; idx++) {
                final long currentRowKey = iterator.nextLong();
                final long previousRowKey = iteratorPreShift.nextLong();

                updateRowsetForRow(addedBuilder, removedBuilder, modifyBuilder, sizes[idx], prevSizes[idx],
                        currentRowKey, previousRowKey, base, prevBase);
            }

            return base;
        }

        private boolean @NotNull [] getColumnsToRecompute(final TableUpdate upstream) {
            final boolean[] computeSizeForColumn = new boolean[ungroupColumnModifiedColumnSets.length];
            for (int ii = 0; ii < ungroupColumnModifiedColumnSets.length; ii++) {
                computeSizeForColumn[ii] =
                        upstream.modifiedColumnSet().containsAny(ungroupColumnModifiedColumnSets[ii]);
            }
            return computeSizeForColumn;
        }
    }

    private void updateRowsetForRow(final RowSetBuilderSequential addedBuilder,
            final RowSetBuilderSequential removedBuilder,
            final RowSetBuilderSequential modifyBuilder,
            final long size,
            final long prevSize,
            long currentRowKey,
            long previousRowKey,
            final long base,
            final long prevBase) {
        currentRowKey <<= base;
        previousRowKey <<= prevBase;

        Assert.geqZero(currentRowKey, "currentRowKey");
        if (size > 0) {
            Assert.geqZero(currentRowKey + size - 1, "currentRowKey + size - 1");
        }

        if (size == prevSize) {
            if (size > 0) {
                modifyBuilder.appendRange(currentRowKey, currentRowKey + size - 1);
            }
        } else if (size < prevSize) {
            if (size > 0) {
                modifyBuilder.appendRange(currentRowKey, currentRowKey + size - 1);
            }
            removedBuilder.appendRange(previousRowKey + size, previousRowKey + prevSize - 1);
        } else {
            if (prevSize > 0) {
                modifyBuilder.appendRange(currentRowKey, currentRowKey + prevSize - 1);
            }
            addedBuilder.appendRange(currentRowKey + prevSize, currentRowKey + size - 1);
        }
    }
}

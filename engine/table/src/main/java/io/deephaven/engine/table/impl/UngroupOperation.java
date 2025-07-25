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
        }
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

        final long maxSize = computeMaxSize(initRowSet, sizes, usePrev);
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

    private long computeMaxSize(final RowSet rowSet, final long[] sizes, final boolean usePrev) {
        long maxSize = 0;

        final int chunkSize = (int) Math.min(CHUNK_SIZE, rowSet.size());

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
                if (ungroupableColumnSources[ci] == null) {
                    // noinspection resource,DataFlowIssue
                    getContexts[ci] = ungroupSources[ci].makeGetContext(chunkSize);
                } else {
                    // noinspection resource,DataFlowIssue
                    fillContexts[ci] = ungroupSources[ci].makeFillContext(chunkSize);
                }
            }

            int offset = 0;
            while (rsit.hasMore()) {
                final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(chunkSize);
                final int actualChunkSize = rsChunk.intSize();
                sharedContext.reset();

                for (int columnIndex = 0; columnIndex < ungroupSources.length; columnIndex++) {
                    final ColumnSource<?> arrayColumn = ungroupSources[columnIndex];
                    final String name = columnsToUngroupBy[columnIndex];
                    final UngroupableColumnSource ungroupable = ungroupableColumnSources[columnIndex];

                    if (ungroupable == null) {
                        final Chunk<? extends Values> chunk =
                                usePrev ? arrayColumn.getPrevChunk(getContexts[columnIndex], rsChunk)
                                        : arrayColumn.getChunk(getContexts[columnIndex], rsChunk);
                        if (columnIndex == 0) {
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
                                    final String message = String.format(
                                            "Array sizes differ at row key %d (position %d), %s has size %d, %s has size %d",
                                            correspondingRowKey, offset + firstDifferenceChunkOffset,
                                            columnsToUngroupBy[0],
                                            sizes[offset + firstDifferenceChunkOffset], name, mismatchedSize.get());
                                    throw new IllegalStateException(message);
                                }
                            }
                        }
                    } else if (columnIndex == 0) {
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
                                    final String message = String.format(
                                            "Array sizes differ at row key %d (position %d), %s has size %d, %s has size %d",
                                            correspondingRowKey, offset + ii, columnsToUngroupBy[0],
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

    private void computePrevSize(final RowSet prevResultRowset,
            final int base,
            final RowSet prevRowsetInParent,
            final long[] sizes) {
        Assert.assertion(prevRowsetInParent.isNonempty(), "prevRowsetInParent.isNonempty()");

        final RowSet.RangeIterator prevIt = prevResultRowset.rangeIterator();
        final RowSet.Iterator parentIt = prevRowsetInParent.iterator();
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

            int requiredBase = evaluateAdded(upstream.added(), added, shiftState.getNumShiftBits(), false);
            if (requiredBase == shiftState.getNumShiftBits()) {
                requiredBase = evaluateModified(upstream.modified(),
                        upstream.getModifiedPreShift(),
                        addedByModifies,
                        removedByModifies,
                        modifiedByModifies,
                        shiftState.getNumShiftBits(),
                        modifiedCurrentSizes);
            } else {
                modifiedCurrentSizes.setValue(new long[upstream.modified().intSize()]);
                final long maxModSize = computeMaxSize(upstream.modified(), modifiedCurrentSizes.get(), false);
                requiredBase = Math.max(determineRequiredBase(maxModSize), requiredBase);
            }
            if (requiredBase != shiftState.getNumShiftBits()) {
                rebase(requiredBase, upstream, modifiedCurrentSizes.get());
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

            // TODO: we should examine the MCS to avoid work on columns that have not changed
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

        private void rebase(final int newBase, final TableUpdate upstream, final long[] modifiedCurrentSizes) {
            final RowSetBuilderSequential addedBuilder = RowSetFactory.builderSequential();
            final RowSetBuilderSequential removedBuilder = RowSetFactory.builderSequential();

            final RowSetBuilderSequential addedByModifiesBuilder = RowSetFactory.builderSequential();
            final RowSetBuilderSequential removedByModifiesBuilder = RowSetFactory.builderSequential();
            final RowSetBuilderSequential modifiedByModifiesBuilder = RowSetFactory.builderSequential();

            final int computedAddBase = evaluateAdded(upstream.added(), addedBuilder, newBase, false);
            final int computedModifiedBase = evaluateModified(upstream.modified(), upstream.getModifiedPreShift(),
                    addedByModifiesBuilder, removedByModifiesBuilder, modifiedByModifiesBuilder, newBase,
                    new MutableObject<>(modifiedCurrentSizes));
            Assert.leq(computedAddBase, "computedAddBase", newBase, "newBase");
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
            final WritableRowSet added = addedBuilder.build();
            try (final RowSet addedByModifies = addedByModifiesBuilder.build()) {
                added.insert(addedByModifies);
            }

            resultRowset.insert(added);

            setNewBase(newBase);

            final RowSet modifiedRowSet = modifiedByModifiesBuilder.build();
            transformer.clearAndTransform(upstream.modifiedColumnSet(), result.getModifiedColumnSetForUpdates());
            final TableUpdateImpl downstream = new TableUpdateImpl(added, removedRowSet, modifiedRowSet,
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
                        // TODO: this nextKey should be advanceTo, we want to cover a case where we have more shift data
                        // in this range
                        nextKey = Math.max(shiftData.getBeginRange(pos), nextKey);
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
            final int newBase = evaluateAdded(upstream.added(), added, QueryTable.minimumUngroupBase, true);

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
                final int existingBase,
                final boolean alwaysBuild) {
            if (rowSet.isEmpty()) {
                return existingBase;
            }
            final long[] sizes = new long[rowSet.intSize("ungroup")];
            final long maxSize = computeMaxSize(rowSet, sizes, false);
            final int minBase = determineRequiredBase(maxSize);

            if (!alwaysBuild && minBase > existingBase) {
                // If we are going to change the base, there is no point in creating this rowset
                return minBase;
            }
            final int resultBase = Math.max(existingBase, minBase);
            getUngroupRowset(sizes, ungroupBuilder, resultBase, rowSet);
            return resultBase;
        }

        private void evaluateRemoved(final RowSet rowSet, final RowSetBuilderSequential builder) {
            if (rowSet.isEmpty()) {
                return;
            }
            final long[] sizes = new long[rowSet.intSize("ungroup")];
            computePrevSize(result.getRowSet().prev(), shiftState.getPrevNumShiftBits(), rowSet, sizes);
            getUngroupRowset(sizes, builder, shiftState.getPrevNumShiftBits(), rowSet);
        }

        /**
         *
         * @param modified the modified rowset, in post-shift space
         * @param modifiedPreShift the modified rowset, in pre-shift space
         * @param addedBuilder the builder for rows added by modifications
         * @param removedBuilder the builder for rows removed by modifications
         * @param modifyBuilder the builder for rows modified by modifications
         * @param currentSizes an input parameter if non-null, else an output parameter to allow re-use of retrieved
         *        sizes when we determine a change of base is necessary
         * @return the necessary base
         */
        private int evaluateModified(final RowSet modified,
                final RowSet modifiedPreShift,
                final RowSetBuilderSequential addedBuilder,
                final RowSetBuilderSequential removedBuilder,
                final RowSetBuilderSequential modifyBuilder,
                final int base,
                final MutableObject<long[]> currentSizes) {
            if (modified.isEmpty()) {
                return base;
            }

            final int size = modified.intSize("ungroup");
            final long[] sizes;
            final long maxSize;
            if (currentSizes.get() == null) {
                sizes = new long[size];
                maxSize = computeMaxSize(modified, sizes, false);
                final int minBase = determineRequiredBase(maxSize);
                if (minBase > base) {
                    // If we are going to force a rebase, there is no need to compute the entire rowset, but the caller
                    // would like our copy of the sizes we computed
                    currentSizes.setValue(sizes);
                    return minBase;
                }
            } else {
                sizes = currentSizes.get();
                Assert.eq(sizes.length, "sizes.length", size, "modified.intSize(\"ungroup\")");
                // noinspection OptionalGetWithoutIsPresent
                Assert.eq(sizes.length, "sizes.length", size, "modified.intSize()");
                // noinspection OptionalGetWithoutIsPresent
                maxSize = Arrays.stream(sizes).max().getAsLong();
                final int minBase = determineRequiredBase(maxSize);
                Assert.leq(minBase, "minBase", base, "base");
            }

            // This function is used from both the normal and the rebase path.
            final long[] prevSizes = new long[size];
            final int prevBase = shiftState.getPrevNumShiftBits();
            computePrevSize(result.getRowSet().prev(), prevBase, modifiedPreShift, prevSizes);

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

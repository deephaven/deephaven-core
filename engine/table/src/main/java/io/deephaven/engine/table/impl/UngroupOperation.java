//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
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
    final UngroupSizeKernel[] sizeKernels;

    public UngroupOperation(final QueryTable parent, final boolean nullFill, final String[] columnsToUngroupBy) {
        this.parent = parent;
        this.nullFill = nullFill;
        this.columnsToUngroupBy = columnsToUngroupBy;

        ungroupSources = new ColumnSource[columnsToUngroupBy.length];
        ungroupableColumnSources = new UngroupableColumnSource[columnsToUngroupBy.length];
        sizeKernels = new UngroupSizeKernel[columnsToUngroupBy.length];

        for (int columnIndex = 0; columnIndex < columnsToUngroupBy.length; columnIndex++) {
            final String name = columnsToUngroupBy[columnIndex];
            final ColumnSource<?> column = parent.getColumnSource(name);
            if (UngroupableColumnSource.isUngroupable(column)) {
                ungroupableColumnSources[columnIndex] = (UngroupableColumnSource) column;
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
        anyUngroupable = Arrays.stream(ungroupableColumnSources).anyMatch(Objects::nonNull);
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
        final int initialBase = Math.max(64 - Long.numberOfLeadingZeros(maxSize), QueryTable.minimumUngroupBase);

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

    private void updateRowsetForRow(final RowSetBuilderSequential addedBuilder,
            final RowSetBuilderSequential removedBuilder,
            final RowSetBuilderSequential modifyBuilder,
            final long size,
            final long prevSize,
            long currentRowKey,
            long previousRowKey,
            final long base) {
        currentRowKey <<= base;
        previousRowKey <<= base;

        Require.requirement(currentRowKey >= 0 && (size == 0 || (currentRowKey + size - 1 >= 0)),
                "rowKey >= 0 && (size == 0 || (rowKey + size - 1 >= 0))");

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

    private long computeMaxSize(final RowSet rowSet, final long[] sizes, final boolean usePrev) {
        long maxSize = 0;

        final int chunkSize = (int) Math.min(CHUNK_SIZE, rowSet.size());

        final ChunkSource.GetContext[] getContexts = new ChunkSource.GetContext[columnsToUngroupBy.length];
        final ChunkSource.FillContext[] fillContexts =
                anyUngroupable ? new ChunkSource.FillContext[columnsToUngroupBy.length] : null;
        try (final SafeCloseableArray<ChunkSource.GetContext> ignored = new SafeCloseableArray<>(getContexts);
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
                    // noinspection resource
                    getContexts[ci] = ungroupSources[ci].makeGetContext(chunkSize);
                } else {
                    // noinspection resource,DataFlowIssue
                    fillContexts[ci] = ungroupSources[ci].makeFillContext(chunkSize);
                }
            }

            int offset = 0;
            while (rsit.hasMore()) {
                final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(CHUNK_SIZE);
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
                        resettable.resetFromArray(sizes, offset, rsChunk.intSize());
                        if (usePrev) {
                            ungroupableColumnSources[columnIndex].getUngroupedPrevSize(fillContexts[columnIndex],
                                    rsChunk, resettable);
                        } else {
                            ungroupableColumnSources[columnIndex].getUngroupedSize(fillContexts[columnIndex], rsChunk,
                                    resettable);
                        }

                        for (int ii = 0; ii < resettable.size(); ii++) {
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

                        for (int ii = 0; ii < currentSizes.size(); ii++) {
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
            final RowSet parentRowset,
            final long[] sizes) {
        final RowSet.RangeIterator prevIt = prevResultRowset.rangeIterator();
        final RowSet.Iterator parentIt = parentRowset.iterator();
        long nextKey = 0;

        for (int pos = 0; pos < sizes.length; ++pos) {
            final long parentKey = parentIt.nextLong();
            if (parentKey < nextKey) {
                sizes[pos] = 0;
                continue;
            }

            final long firstKeyForRow = parentKey << base;
            if (!prevIt.advance(firstKeyForRow)) {
                // the rest of the sizes must be zero, because there is nothing else in our result rowset
                Arrays.fill(sizes, pos, sizes.length, 0);
                break;
            }

            if (prevIt.currentRangeStart() == firstKeyForRow) {
                sizes[pos] = prevIt.currentRangeEnd() - firstKeyForRow + 1;
            } else {
                sizes[pos] = 0;
                // we should be in some other row
                final long lastKeyForRow = ((parentKey + 1) << base) - 1;
                Assert.gt(prevIt.currentRangeStart(), "prevIt.currentRangeStart()", lastKeyForRow, "lastRowForKey");
                nextKey = prevIt.currentRangeStart() >> base;
            }
        }
    }

    private void getUngroupRowset(final long[] sizes, final RowSetBuilderSequential builder, final long base,
            final RowSet rowSet) {
        Assert.assertion(base >= 0 && base <= 63, "base >= 0 && base <= 63", base, "base");
        final long mask = ((1L << base) - 1) << (64 - base);
        final long lastKey = rowSet.lastRowKey();
        if ((lastKey > 0) && ((lastKey & mask) != 0)) {
            throw new IllegalStateException(
                    "Key overflow detected, perhaps you should flatten your table before calling ungroup.  "
                            + ",lastRowKey=" + lastKey + ", base=" + base);
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
            super("ungroup(" + UngroupOperation.this.getDescription() + ')', UngroupOperation.this.parent, result);
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
                final RowSetBuilderSequential ungroupAdded = RowSetFactory.builderSequential();
                // TODO: cover the true vs. false
                final int newBase = evaluateAdded(upstream.added(), ungroupAdded, QueryTable.minimumUngroupBase, true);
                rebase(newBase, ungroupAdded.build());
                return;
            }

            final RowSetBuilderSequential ungroupAdded = RowSetFactory.builderSequential();
            final RowSetBuilderSequential ungroupRemoved = RowSetFactory.builderSequential();

            final RowSetBuilderSequential ungroupModifiedAdded = RowSetFactory.builderSequential();
            final RowSetBuilderSequential ungroupModifiedRemoved = RowSetFactory.builderSequential();
            final RowSetBuilderSequential ungroupModifiedModified = RowSetFactory.builderSequential();

            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();

            int requiredBase = evaluateAdded(upstream.added(), ungroupAdded, shiftState.getNumShiftBits(), false);
            if (requiredBase == shiftState.getNumShiftBits()) {
                requiredBase = evaluateModified(upstream.modified(),
                        upstream.getModifiedPreShift(),
                        ungroupModifiedAdded,
                        ungroupModifiedRemoved,
                        ungroupModifiedModified);
            }
            if (requiredBase != shiftState.getNumShiftBits()) {
                rebase(requiredBase + 1);
                return;
            }

            evaluateRemoved(upstream.removed(), ungroupRemoved);

            final WritableRowSet addedRowSet = ungroupAdded.build();
            final WritableRowSet removedRowSet = ungroupRemoved.build();

            addedRowSet.insert(ungroupModifiedAdded.build());
            removedRowSet.insert(ungroupModifiedRemoved.build());

            // noinspection resource
            final TrackingWritableRowSet resultRowset = result.getRowSet().writableCast();
            resultRowset.remove(removedRowSet);

            final RowSetBuilderRandom removedByShiftBuilder = RowSetFactory.builderRandom();
            final RowSetBuilderRandom addedByShiftBuilder = RowSetFactory.builderRandom();
            if (upstream.shifted().nonempty()) {
                try (final RowSet.RangeIterator rsit = resultRowset.rangeIterator()) {
                    // there must be at least one thing in the result rowset, otherwise we would have followed the clear
                    // logic instead of this logic
                    rsit.next();
                    long nextResultKey = rsit.currentRangeStart();
                    long nextSourceKey = nextResultKey >> requiredBase;

                    final long lastRowKeyOffset = (1L << requiredBase) - 1;

                    final RowSetShiftData upstreamShift = upstream.shifted();
                    final long shiftSize = upstreamShift.size();
                    SHIFT_LOOP: for (int ii = 0; ii < shiftSize; ++ii) {
                        final long begin = upstreamShift.getBeginRange(ii);
                        final long end = upstreamShift.getEndRange(ii);
                        final long shiftDelta = upstreamShift.getShiftDelta(ii);

                        final long resultShiftAmount = shiftDelta << (long) requiredBase;

                        for (long rk = Math.max(begin, nextSourceKey); rk <= end; rk =
                                Math.max(rk + 1, nextSourceKey)) {
                            final long oldRangeStart = rk << (long) requiredBase;
                            final long oldRangeEnd = (rk << (long) requiredBase) + lastRowKeyOffset;

                            if (!rsit.advance(oldRangeStart)) {
                                break SHIFT_LOOP;
                            }
                            // we successfully advanced
                            nextResultKey = rsit.currentRangeStart();
                            nextSourceKey = nextResultKey >> requiredBase;

                            if (nextResultKey > oldRangeEnd) {
                                // no need to shift things that don't exist anymore
                                continue;
                            }

                            // get the range from our result rowset
                            Assert.eq(oldRangeStart, "oldRangeStart", rsit.currentRangeStart(),
                                    "rsit.currentRangeStart()");
                            final long actualRangeEnd = rsit.currentRangeEnd();
                            shiftBuilder.shiftRange(oldRangeStart, actualRangeEnd, resultShiftAmount);
                            removedByShiftBuilder.addRange(oldRangeStart, actualRangeEnd);
                            addedByShiftBuilder.addRange(oldRangeStart + resultShiftAmount,
                                    actualRangeEnd + resultShiftAmount);

                        }
                    }
                }
            }

            resultRowset.remove(removedByShiftBuilder.build());
            resultRowset.insert(addedByShiftBuilder.build());
            resultRowset.insert(addedRowSet);

            // TODO: we should examine the MCS to avoid work on rebased columns that have not changed (and just
            // transform things for the key columns)
            final RowSet modifiedRowSet = ungroupModifiedModified.build();
            transformer.clearAndTransform(upstream.modifiedColumnSet(), result.getModifiedColumnSetForUpdates());
            final TableUpdateImpl downstream = new TableUpdateImpl(addedRowSet, removedRowSet, modifiedRowSet,
                    shiftBuilder.build(), result.getModifiedColumnSetForUpdates());
            result.notifyListeners(downstream);
        }

        private void rebase(final int newBase) {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            final long[] sizes = new long[parentRowset.intSize()];
            computeMaxSize(parentRowset, sizes, false);
            getUngroupRowset(sizes, builder, newBase, parentRowset);
            final WritableRowSet newRowSet = builder.build();
            rebase(newBase, newRowSet);
        }

        private void rebase(final int newBase, final WritableRowSet newRowSet) {
            final TrackingWritableRowSet resultRowset = result.getRowSet().writableCast();
            resultRowset.resetTo(newRowSet);

            for (final ColumnSource<?> source : resultMap.values()) {
                if (source instanceof UngroupedColumnSource) {
                    ((UngroupedColumnSource<?>) source).setBase(newBase);
                }
            }
            shiftState.setNumShiftBitsAndUpdatePrev(newBase);

            result.notifyListeners(new TableUpdateImpl(newRowSet, resultRowset.copyPrev(), RowSetFactory.empty(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        }

        private int evaluateAdded(final RowSet rowSet, @NotNull final RowSetBuilderSequential ungroupBuilder,
                final int existingBase,
                final boolean alwaysBuild) {
            if (rowSet.isEmpty()) {
                return existingBase;
            }
            final long[] sizes = new long[rowSet.intSize("ungroup")];
            final long maxSize = computeMaxSize(rowSet, sizes, false);
            final int minBase = 64 - Long.numberOfLeadingZeros(maxSize);

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
            computePrevSize(result.getRowSet().prev(), shiftState.getNumShiftBits(), rowSet, sizes);
            getUngroupRowset(sizes, builder, shiftState.getNumShiftBits(), rowSet);
        }

        private int evaluateModified(final RowSet modified,
                final RowSet modifiedPreShift,
                final RowSetBuilderSequential addedBuilder,
                final RowSetBuilderSequential removedBuilder,
                final RowSetBuilderSequential modifyBuilder) {
            final int base = shiftState.getNumShiftBits();
            if (modified.isEmpty()) {
                return base;
            }

            final int size = modified.intSize("ungroup");
            final long[] sizes = new long[size];
            final long[] prevSizes = new long[size];

            final long maxSize = computeMaxSize(modified, sizes, false);
            computePrevSize(result.getRowSet().prev(), shiftState.getNumShiftBits(), modifiedPreShift, prevSizes);
            final int minBase = 64 - Long.numberOfLeadingZeros(maxSize);
            if (minBase > base) {
                // If we are going to force a rebase, there is no need to compute the entire rowset
                return minBase;
            }

            final RowSet.Iterator iterator = modified.iterator();
            final RowSet.Iterator iteratorPreShift = modifiedPreShift.iterator();
            for (int idx = 0; idx < size; idx++) {
                final long currentRowKey = iterator.nextLong();
                final long previousRowKey = iteratorPreShift.nextLong();

                updateRowsetForRow(addedBuilder, removedBuilder, modifyBuilder, sizes[idx], prevSizes[idx],
                        currentRowKey, previousRowKey, base);
            }

            return base;
        }
    }
}

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
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.sources.BitShiftingColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupableColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.util.mutable.MutableLong;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class UngroupOperation implements QueryTable.MemoizableOperation<QueryTable> {
    static final int CHUNK_SIZE =
            Configuration.getInstance().getIntegerWithDefault("UngroupOperation.chunkSize", 1 << 10);

    private final QueryTable parent;
    private final boolean nullFill;
    private final String[] columnsToUngroupBy;

    final ColumnSource<?>[] ungroupSources;
    final UngroupableColumnSource[] ungroupableColumnSources;
    final UngroupSizeKernels.SizeFunction[] sizeFunctions;
    final UngroupSizeKernels.MaxSizeFunction[] maxSizeFunctions;
    final UngroupSizeKernels.CheckSizeFunction[] checkSizeFunctions;
    final UngroupSizeKernels.MaybeIncreaseSizeFunction[] maybeIncreaseSizeFunctions;

    public UngroupOperation(final QueryTable parent, final boolean nullFill, final String[] columnsToUngroupBy) {
        this.parent = parent;
        this.nullFill = nullFill;
        this.columnsToUngroupBy = columnsToUngroupBy;

        ungroupSources = new ColumnSource[columnsToUngroupBy.length];
        ungroupableColumnSources = new UngroupableColumnSource[columnsToUngroupBy.length];
        sizeFunctions = new UngroupSizeKernels.SizeFunction[columnsToUngroupBy.length];
        maxSizeFunctions = new UngroupSizeKernels.MaxSizeFunction[columnsToUngroupBy.length];
        checkSizeFunctions = new UngroupSizeKernels.CheckSizeFunction[columnsToUngroupBy.length];
        maybeIncreaseSizeFunctions = new UngroupSizeKernels.MaybeIncreaseSizeFunction[columnsToUngroupBy.length];

        for (int columnIndex = 0; columnIndex < columnsToUngroupBy.length; columnIndex++) {
            final String name = columnsToUngroupBy[columnIndex];
            final ColumnSource<?> column = parent.getColumnSource(name);
            if (UngroupableColumnSource.isUngroupable(column)) {
                ungroupableColumnSources[columnIndex] = (UngroupableColumnSource) column;
            }
            ungroupSources[columnIndex] = column;
            if (column.getType().isArray()) {
                sizeFunctions[columnIndex] = UngroupSizeKernels::sizeArray;
                maxSizeFunctions[columnIndex] = UngroupSizeKernels::maxSizeArray;
                checkSizeFunctions[columnIndex] = UngroupSizeKernels::checkSizeArray;
                maybeIncreaseSizeFunctions[columnIndex] = UngroupSizeKernels::maybeIncreaseSizeArray;
            } else if (Vector.class.isAssignableFrom(column.getType())) {
                sizeFunctions[columnIndex] = UngroupSizeKernels::sizeVector;
                maxSizeFunctions[columnIndex] = UngroupSizeKernels::maxSizeVector;
                checkSizeFunctions[columnIndex] = UngroupSizeKernels::checkSizeVector;
                maybeIncreaseSizeFunctions[columnIndex] = UngroupSizeKernels::maybeIncreaseSizeVector;
            } else {
                throw new RuntimeException("Column " + name + " is not an array");
            }
        }
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
        getUngroupIndex(sizes, builder, initialBase, initRowSet);
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

    private long computeModifiedIndicesAndMaxSize(final RowSet modified,
            final RowSet modifiedPreShift,
            final RowSetBuilderSequential modifyBuilder,
            final RowSetBuilderSequential addedBuilded,
            final RowSetBuilderSequential removedBuilder,
            final long base) {
        final int size = modified.intSize("ungroup");
        final long[] sizes = new long[size];
        final long[] prevSizes = new long[size];

        final long maxSize = computeMaxSize(modified, sizes, false);
        computePrevSize(modifiedPreShift, prevSizes);

        // TODO: if the base is going to increase there is no point in doing the below index updates, we'll throw
        // everything out anyway
        final RowSet.Iterator iterator = modified.iterator();
        final RowSet.Iterator iteratorPreShift = modifiedPreShift.iterator();
        for (int idx = 0; idx < size; idx++) {
            final long currentRowKey = iterator.nextLong();
            final long previousRowKey = iteratorPreShift.nextLong();

            updateIndexForRow(modifyBuilder, addedBuilded, removedBuilder, sizes[idx], prevSizes[idx], currentRowKey,
                    previousRowKey, base);
        }

        return maxSize;
    }

    private void updateIndexForRow(final RowSetBuilderSequential modifyBuilder,
            final RowSetBuilderSequential addedBuilded,
            final RowSetBuilderSequential removedBuilder,
            final long size,
            final long prevSize, long currentRowKey,
            long previousRowKey,
            final long base) {
        currentRowKey <<= base;
        previousRowKey <<= base;

        Require.requirement(currentRowKey >= 0 && (size == 0 || (currentRowKey + size - 1 >= 0)),
                "rowKey >= 0 && (size == 0 || (rowKey + size - 1 >= 0))");
        Require.requirement(previousRowKey >= 0 && (prevSize == 0 || (previousRowKey + prevSize - 1 >= 0)),
                "previousRowKey >= 0 && (prevSize == 0 || (previousRowKey + prevSize - 1 >= 0))");

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
            addedBuilded.appendRange(currentRowKey + prevSize, currentRowKey + size - 1);
        }
    }

    private long computeMaxSize(final RowSet rowSet, final long[] sizes, final boolean usePrev) {
        String referenceColumn = null;
        long maxSize = 0;
        boolean sizeIsInitialized = false;
        for (int columnIndex = 0; columnIndex < ungroupSources.length; columnIndex++) {
            final ColumnSource<?> arrayColumn = ungroupSources[columnIndex];
            final String name = columnsToUngroupBy[columnIndex];
            final UngroupableColumnSource ungroupable = ungroupableColumnSources[columnIndex];

            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;

                if (ungroupable == null) {
                    int offset = 0;
                    try (final ChunkSource.GetContext getContext = arrayColumn.makeGetContext(CHUNK_SIZE)) {
                        final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                        while (rsit.hasMore()) {
                            final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(CHUNK_SIZE);
                            final Chunk<? extends Values> chunk =
                                    usePrev ? arrayColumn.getPrevChunk(getContext, rsChunk)
                                            : arrayColumn.getChunk(getContext, rsChunk);
                            final ObjectChunk<Object, ? extends Values> objectChunk = chunk.asObjectChunk();
                            maxSize = Math.max(maxSizeFunctions[columnIndex].maxSize(objectChunk, sizes, offset),
                                    maxSize);
                            offset += objectChunk.size();
                        }
                    }
                } else {
                    int offset = 0;
                    try (final ChunkSource.FillContext fillContext = arrayColumn.makeFillContext(CHUNK_SIZE);
                            final ResettableWritableLongChunk<Values> resettable =
                                    ResettableWritableLongChunk.makeResettableChunk()) {
                        final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                        while (rsit.hasMore()) {
                            final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(CHUNK_SIZE);
                            resettable.resetFromArray(sizes, offset, rsChunk.intSize());
                            if (usePrev) {
                                ungroupable.getUngroupedPrevSize(fillContext, rsChunk, resettable);
                            } else {
                                ungroupable.getUngroupedSize(fillContext, rsChunk, resettable);
                            }

                            for (int ii = 0; ii < resettable.size(); ii++) {
                                maxSize = Math.max(resettable.get(ii), maxSize);
                            }
                            offset += resettable.size();
                        }
                    }
                }
            } else if (nullFill) {
                if (ungroupable == null) {
                    try (final ChunkSource.GetContext getContext = arrayColumn.makeGetContext(CHUNK_SIZE)) {
                        final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                        int offset = 0;
                        while (rsit.hasMore()) {
                            final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(CHUNK_SIZE);
                            final Chunk<? extends Values> chunk =
                                    usePrev ? arrayColumn.getPrevChunk(getContext, rsChunk)
                                            : arrayColumn.getChunk(getContext, rsChunk);
                            final ObjectChunk<Object, ? extends Values> objectChunk = chunk.asObjectChunk();

                            final long maxSizeForChunk = maybeIncreaseSizeFunctions[columnIndex]
                                    .maybeIncreaseSize(objectChunk, sizes, offset);
                            maxSize = Math.max(maxSizeForChunk, maxSize);
                            offset += objectChunk.size();
                        }
                    }
                } else {
                    int offset = 0;
                    try (final ChunkSource.FillContext fillContext = arrayColumn.makeFillContext(CHUNK_SIZE);
                            final WritableLongChunk<Values> currentSizes =
                                    WritableLongChunk.makeWritableChunk(CHUNK_SIZE)) {
                        final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                        while (rsit.hasMore()) {
                            final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(CHUNK_SIZE);
                            if (usePrev) {
                                ungroupable.getUngroupedPrevSize(fillContext, rsChunk, currentSizes);
                            } else {
                                ungroupable.getUngroupedSize(fillContext, rsChunk, currentSizes);
                            }

                            for (int ii = 0; ii < currentSizes.size(); ii++) {
                                final long currentSize = currentSizes.get(ii);
                                if (currentSize > sizes[offset + ii]) {
                                    sizes[offset + ii] = currentSize;
                                    maxSize = Math.max(currentSize, maxSize);
                                }
                            }
                            offset += currentSizes.size();
                        }
                    }
                }
            } else {
                if (ungroupable == null) {
                    final MutableLong encountered = new MutableLong();

                    int offset = 0;
                    try (final ChunkSource.GetContext getContext = arrayColumn.makeGetContext(CHUNK_SIZE)) {
                        final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                        while (rsit.hasMore()) {
                            final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(CHUNK_SIZE);
                            final Chunk<? extends Values> chunk =
                                    usePrev ? arrayColumn.getPrevChunk(getContext, rsChunk)
                                            : arrayColumn.getChunk(getContext, rsChunk);
                            final ObjectChunk<Object, ? extends Values> objectChunk = chunk.asObjectChunk();
                            final int firstDifference =
                                    checkSizeFunctions[columnIndex].checkSize(objectChunk, sizes, offset, encountered);
                            if (firstDifference >= 0) {
                                final long correspondingRowKey = rsChunk.asRowSet().get(firstDifference);
                                final String message = String.format(
                                        "Array sizes differ at row key %d (position %d), %s has size %d, %s has size %d",
                                        correspondingRowKey, offset + firstDifference, referenceColumn,
                                        sizes[offset + firstDifference], name, encountered.get());
                                throw new IllegalStateException(message);
                            }
                            offset += objectChunk.size();
                        }
                    }
                } else {
                    int offset = 0;
                    try (final ChunkSource.FillContext fillContext = arrayColumn.makeFillContext(CHUNK_SIZE);
                            final WritableLongChunk<Values> currentSizes =
                                    WritableLongChunk.makeWritableChunk(CHUNK_SIZE)) {
                        final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                        while (rsit.hasMore()) {
                            final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(CHUNK_SIZE);
                            if (usePrev) {
                                ungroupable.getUngroupedPrevSize(fillContext, rsChunk, currentSizes);
                            } else {
                                ungroupable.getUngroupedSize(fillContext, rsChunk, currentSizes);
                            }
                            Assert.eq(currentSizes.size(), "currentSizes.size()", rsChunk.intSize(),
                                    "rsChunk.intSize()");

                            for (int ii = 0; ii < currentSizes.size(); ii++) {
                                final long currentSize = currentSizes.get(ii);
                                if (currentSize != sizes[offset + ii]) {
                                    final long correspondingRowKey = rsChunk.asRowSet().get(ii);
                                    final String message = String.format(
                                            "Array sizes differ at row key %d (position %d), %s has size %d, %s has size %d",
                                            correspondingRowKey, offset + ii, referenceColumn,
                                            sizes[offset + ii], name, currentSize);
                                    throw new IllegalStateException(message);
                                }
                            }
                            offset += currentSizes.size();
                        }
                    }
                }
            }
        }
        return maxSize;
    }

    private void computePrevSize(final RowSet rowSet, final long[] sizes) {
        boolean sizeIsInitialized = false;
        for (int columnIndex = 0; columnIndex < ungroupSources.length; columnIndex++) {
            final ColumnSource<?> arrayColumn = ungroupSources[columnIndex];
            final UngroupableColumnSource ungroupable = ungroupableColumnSources[columnIndex];

            if (!sizeIsInitialized) {
                sizeIsInitialized = true;

                if (ungroupable == null) {
                    int offset = 0;
                    try (final ChunkSource.GetContext getContext = arrayColumn.makeGetContext(CHUNK_SIZE)) {
                        final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                        while (rsit.hasMore()) {
                            final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(CHUNK_SIZE);
                            final Chunk<? extends Values> chunk = arrayColumn.getPrevChunk(getContext, rsChunk);
                            final ObjectChunk<Object, ? extends Values> objectChunk = chunk.asObjectChunk();
                            sizeFunctions[columnIndex].size(objectChunk, sizes, offset);
                            offset += objectChunk.size();
                        }
                    }
                } else {
                    int offset = 0;
                    try (final ChunkSource.FillContext fillContext = arrayColumn.makeFillContext(CHUNK_SIZE);
                            final ResettableWritableLongChunk<Values> resettable =
                                    ResettableWritableLongChunk.makeResettableChunk()) {
                        final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                        while (rsit.hasMore()) {
                            final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(CHUNK_SIZE);
                            resettable.resetFromArray(sizes, offset, rsChunk.intSize());
                            ungroupable.getUngroupedPrevSize(fillContext, rsChunk, resettable);
                            offset += resettable.size();
                        }
                    }
                }
                if (!nullFill) {
                    // In the "normal" case we've defined the sizes to all match; so we can check only the first column
                    return;
                }
            }

            Assert.assertion(nullFill, "nullFill");

            if (ungroupable == null) {
                try (final ChunkSource.GetContext getContext = arrayColumn.makeGetContext(CHUNK_SIZE)) {
                    final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                    int offset = 0;
                    while (rsit.hasMore()) {
                        final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(CHUNK_SIZE);
                        final Chunk<? extends Values> chunk = arrayColumn.getPrevChunk(getContext, rsChunk);
                        final ObjectChunk<Object, ? extends Values> objectChunk = chunk.asObjectChunk();

                        maybeIncreaseSizeFunctions[columnIndex].maybeIncreaseSize(objectChunk, sizes, offset);
                        offset += objectChunk.size();
                    }
                }
            } else {
                int offset = 0;
                try (final ChunkSource.FillContext fillContext = arrayColumn.makeFillContext(CHUNK_SIZE);
                        final WritableLongChunk<Values> currentSizes =
                                WritableLongChunk.makeWritableChunk(CHUNK_SIZE)) {
                    final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
                    while (rsit.hasMore()) {
                        final RowSequence rsChunk = rsit.getNextRowSequenceWithLength(CHUNK_SIZE);
                        ungroupable.getUngroupedPrevSize(fillContext, rsChunk, currentSizes);

                        for (int ii = 0; ii < currentSizes.size(); ii++) {
                            final long currentSize = currentSizes.get(ii);
                            if (currentSize > sizes[offset + ii]) {
                                sizes[offset + ii] = currentSize;
                            }
                        }
                        offset += currentSizes.size();
                    }
                }
            }
        }
    }

    private void getUngroupIndex(final long[] sizes, final RowSetBuilderSequential builder, final long base,
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
            final long nextShift = next << base;
            Assert.assertion(nextShift >= 0, "nextShift >= 0", nextShift, "nextShift", base, "base", next, "next");
            final long arraySize = sizes[pos++];
            if (arraySize != 0) {
                builder.appendRange(nextShift, nextShift + arraySize - 1);
            }
        }
    }

    private class UngroupListener extends BaseTable.ListenerImpl {

        private final QueryTable result;
        private final CrossJoinShiftState shiftState;
        private final TrackingRowSet parentRowset;
        private final Map<String, ColumnSource<?>> resultMap;

        public UngroupListener(final QueryTable result, final CrossJoinShiftState shiftState,
                final TrackingRowSet parentRowset,
                final Map<String, ColumnSource<?>> resultMap) {
            super("ungroup(" + Arrays.deepToString(UngroupOperation.this.columnsToUngroupBy) + ')',
                    UngroupOperation.this.parent, result);
            this.result = result;
            this.shiftState = shiftState;
            this.parentRowset = parentRowset;
            this.resultMap = resultMap;
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            parent.intSize("ungroup");

            final boolean clearResult;
            int newBase;
            if (upstream.removed().size() == parent.getRowSet().sizePrev()) {
                // our new base can safely be the minimum base; as everything has been removed from this table. This
                // is convenient to allow the base to shrink in some circumstances.
                newBase = QueryTable.minimumUngroupBase;
                clearResult = true;
            } else {
                newBase = shiftState.getNumShiftBits();
                clearResult = false;
            }

            final RowSetBuilderSequential ungroupAdded = RowSetFactory.builderSequential();
            final RowSetBuilderSequential ungroupRemoved = RowSetFactory.builderSequential();

            final RowSetBuilderSequential ungroupModifiedAdded = RowSetFactory.builderSequential();
            final RowSetBuilderSequential ungroupModifiedRemoved = RowSetFactory.builderSequential();
            final RowSetBuilderSequential ungroupModifiedModified = RowSetFactory.builderSequential();

            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();

            if (clearResult) {
                newBase = evaluateIndex(upstream.added(), ungroupAdded, newBase);
                rebase(newBase, ungroupAdded.build());
                return;
            }

            newBase = evaluateIndex(upstream.added(), ungroupAdded, newBase);
            if (newBase == shiftState.getNumShiftBits()) {
                newBase = evaluateModified(upstream.modified(), upstream.getModifiedPreShift(), ungroupModifiedModified,
                        ungroupModifiedAdded, ungroupModifiedRemoved, newBase);
            }
            if (newBase != shiftState.getNumShiftBits()) {
                rebase(newBase + 1);
                return;
            }

            evaluateRemovedIndex(upstream.removed(), ungroupRemoved);

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
                final long currentBaseSize = (1L << newBase) - 1;

                final RowSetShiftData upstreamShift = upstream.shifted();
                final long shiftSize = upstreamShift.size();
                for (int ii = 0; ii < shiftSize; ++ii) {
                    final long begin = upstreamShift.getBeginRange(ii);
                    final long end = upstreamShift.getEndRange(ii);
                    final long shiftDelta = upstreamShift.getShiftDelta(ii);

                    final long resultShiftAmount = shiftDelta << (long) newBase;

                    for (long rk = begin; rk <= end; rk++) {
                        final long oldRangeStart = rk << (long) newBase;
                        final long oldRangeEnd = (rk << (long) newBase) + currentBaseSize;
                        // get the range from our result rowset, I'm not sure that I love creating a rowset for each
                        // row; we could instead do iteration because we are not actually modifying this thing as we
                        // go
                        try (final WritableRowSet expandedRowsetForRow =
                                resultRowset.subSetByKeyRange(oldRangeStart, oldRangeEnd)) {
                            if (expandedRowsetForRow.isNonempty()) {
                                // no need to shift things that don't exist anymore
                                shiftBuilder.shiftRange(oldRangeStart, oldRangeEnd, resultShiftAmount);
                                removedByShiftBuilder.addRowSet(expandedRowsetForRow);
                                // move it over
                                expandedRowsetForRow.shiftInPlace(resultShiftAmount);
                                addedByShiftBuilder.addRowSet(expandedRowsetForRow);
                            }
                        }
                    }
                }
            }

            resultRowset.remove(removedByShiftBuilder.build());
            resultRowset.insert(addedByShiftBuilder.build());
            resultRowset.insert(addedRowSet);
            for (final ColumnSource<?> source : resultMap.values()) {
                if (source instanceof UngroupedColumnSource) {
                    ((UngroupedColumnSource<?>) source).setBase(newBase);
                }
            }

            // TODO: we should examine the MCS to avoid work on rebased columns that have not changed (and just
            // transform things for the key columns)
            final RowSet modifiedRowSet = ungroupModifiedModified.build();
            final TableUpdateImpl downstream = new TableUpdateImpl(addedRowSet, removedRowSet, modifiedRowSet,
                    shiftBuilder.build(), ModifiedColumnSet.ALL);
            result.notifyListeners(downstream);
        }

        private void rebase(final int newBase) {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            final long[] sizes = new long[parentRowset.intSize()];
            computeMaxSize(parentRowset, sizes, false);
            getUngroupIndex(sizes, builder, newBase, parentRowset);
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

        private int evaluateIndex(final RowSet rowSet, @NotNull final RowSetBuilderSequential ungroupBuilder,
                final int newBase) {
            if (rowSet.isEmpty()) {
                return newBase;
            }
            final long[] sizes = new long[rowSet.intSize("ungroup")];
            final long maxSize = computeMaxSize(rowSet, sizes, false);
            final int minBase = 64 - Long.numberOfLeadingZeros(maxSize);
            final int base = Math.max(newBase, minBase);
            // TODO: we could be throwing this out, maybe we shouldn't build it
            getUngroupIndex(sizes, ungroupBuilder, base, rowSet);
            return base;
        }

        private void evaluateRemovedIndex(final RowSet rowSet, final RowSetBuilderSequential builder) {
            if (rowSet.isEmpty()) {
                return;
            }
            final long[] sizes = new long[rowSet.intSize("ungroup")];
            computePrevSize(rowSet, sizes);
            getUngroupIndex(sizes, builder, shiftState.getNumShiftBits(), rowSet);
        }

        private int evaluateModified(final RowSet modified,
                final RowSet modifiedPreShift,
                final RowSetBuilderSequential modifyBuilder,
                final RowSetBuilderSequential addedBuilded,
                final RowSetBuilderSequential removedBuilder,
                final int newBase) {
            if (modified.isEmpty()) {
                return newBase;
            }

            final long maxSize = computeModifiedIndicesAndMaxSize(modified,
                    modifiedPreShift,
                    modifyBuilder,
                    addedBuilded,
                    removedBuilder,
                    shiftState.getNumShiftBits());
            final int minBase = 64 - Long.numberOfLeadingZeros(maxSize);
            return Math.max(newBase, minBase);
        }
    }
}

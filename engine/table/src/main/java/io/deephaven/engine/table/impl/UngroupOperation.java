//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.sources.BitShiftingColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupableColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.vector.Vector;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class UngroupOperation implements QueryTable.MemoizableOperation<QueryTable> {
    static final Logger log = LoggerFactory.getLogger(UngroupOperation.class);

    private final QueryTable parent;
    private final boolean nullFill;
    private final String[] columnsToUngroupBy;

    public UngroupOperation(final QueryTable parent, final boolean nullFill, final String[] columnsToUngroupBy) {
        this.parent = parent;
        this.nullFill = nullFill;
        this.columnsToUngroupBy = columnsToUngroupBy;
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
        final Map<String, ColumnSource<?>> arrayColumns = new HashMap<>();
        final Map<String, ColumnSource<?>> vectorColumns = new HashMap<>();
        for (final String name : columnsToUngroupBy) {
            final ColumnSource<?> column = parent.getColumnSource(name);
            if (column.getType().isArray()) {
                arrayColumns.put(name, column);
            } else if (Vector.class.isAssignableFrom(column.getType())) {
                vectorColumns.put(name, column);
            } else {
                throw new RuntimeException("Column " + name + " is not an array");
            }
        }
        final long[] sizes = new long[parent.intSize("ungroup")];
        final TrackingRowSet parentRowset = parent.getRowSet();

        final RowSet initRowSet = usePrev ? parentRowset.prev() : parentRowset;

        final long maxSize;
        if (usePrev) {
            maxSize = computeMaxSizePrev(initRowSet, arrayColumns, vectorColumns, sizes, nullFill);
        } else {
            maxSize = computeMaxSize(initRowSet, arrayColumns, vectorColumns, sizes, nullFill);
        }
        final int initialBase = Math.max(64 - Long.numberOfLeadingZeros(maxSize), QueryTable.minimumUngroupBase);

        final CrossJoinShiftState shiftState = new CrossJoinShiftState(initialBase, true);

        final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();
        for (final Map.Entry<String, ColumnSource<?>> es : parent.getColumnSourceMap().entrySet()) {
            final ColumnSource<?> column = es.getValue();
            final String name = es.getKey();
            final ColumnSource<?> result;
            if (vectorColumns.containsKey(name) || arrayColumns.containsKey(name)) {
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
            listener = new UngroupListener(result, shiftState, arrayColumns, parentRowset, vectorColumns, resultMap);
        } else {
            listener = null;
        }
        return new Result<>(result, listener);
    }

    private long computeModifiedIndicesAndMaxSize(final RowSet modified,
            final RowSet modifiedPreShift,
            final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns,
            final RowSetBuilderSequential modifyBuilder,
            final RowSetBuilderSequential addedBuilded,
            final RowSetBuilderSequential removedBuilder,
            final long base,
            final boolean nullFill) {
        if (nullFill) {
            return computeModifiedIndicesAndMaxSizeNullFill(modified, modifiedPreShift, arrayColumns, vectorColumns,
                    modifyBuilder, addedBuilded, removedBuilder, base);
        }
        return computeModifiedIndicesAndMaxSizeNormal(modified, modifiedPreShift, arrayColumns, vectorColumns,
                modifyBuilder, addedBuilded, removedBuilder, base);
    }

    private long computeModifiedIndicesAndMaxSizeNullFill(
            final RowSet modified,
            final RowSet modifiedPreShift,
            final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns,
            final RowSetBuilderSequential modifyBuilder,
            final RowSetBuilderSequential addedBuilded,
            final RowSetBuilderSequential removedBuilder, final long base) {
        long maxSize = 0;
        final RowSet.Iterator iterator = modified.iterator();
        final RowSet.Iterator iteratorPreShift = modifiedPreShift.iterator();

        for (int idx = 0; idx < modified.size(); idx++) {
            long maxCur = 0;
            long maxPrev = 0;
            final long currentRowKey = iterator.nextLong();
            final long previousRowKey = iteratorPreShift.nextLong();
            for (final Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final Object array = arrayColumn.get(currentRowKey);
                final int size = (array == null ? 0 : Array.getLength(array));
                maxCur = Math.max(maxCur, size);
                final Object prevArray = arrayColumn.getPrev(previousRowKey);
                final int prevSize = (prevArray == null ? 0 : Array.getLength(prevArray));
                maxPrev = Math.max(maxPrev, prevSize);
            }
            for (final Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
                final ColumnSource<?> vectorColumn = es.getValue();
                final Vector<?> array = (Vector<?>) vectorColumn.get(currentRowKey);
                final long size = (array == null ? 0 : array.size());
                maxCur = Math.max(maxCur, size);
                final Vector<?> prevArray = (Vector<?>) vectorColumn.getPrev(previousRowKey);
                final long prevSize = (prevArray == null ? 0 : prevArray.size());
                maxPrev = Math.max(maxPrev, prevSize);
            }
            maxSize =
                    maxAndIndexUpdateForRow(modifyBuilder, addedBuilded, removedBuilder, maxSize, maxCur, currentRowKey,
                            previousRowKey, maxPrev, base);
        }
        return maxSize;
    }

    private long computeModifiedIndicesAndMaxSizeNormal(final RowSet modified,
            final RowSet modifiedPreShift,
            final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns,
            final RowSetBuilderSequential modifyBuilder,
            final RowSetBuilderSequential addedBuilded,
            final RowSetBuilderSequential removedBuilder,
            final long base) {
        String referenceColumn = null;
        long maxSize = 0;
        boolean sizeIsInitialized = false;
        final int size = modified.intSize("ungroup");
        final long[] sizes = new long[size];


        for (final Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
            final ColumnSource<?> arrayColumn = es.getValue();
            final String name = es.getKey();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;

                final RowSet.Iterator iterator = modified.iterator();
                final RowSet.Iterator iteratorPreShift = modifiedPreShift.iterator();


                for (int idx = 0; idx < size; idx++) {
                    final long currentRowKey = iterator.nextLong();
                    final long previousRowKey = iteratorPreShift.nextLong();

                    final Object array = arrayColumn.get(currentRowKey);
                    sizes[idx] = (array == null ? 0 : Array.getLength(array));
                    final Object prevArray = arrayColumn.getPrev(previousRowKey);
                    final int prevSize = (prevArray == null ? 0 : Array.getLength(prevArray));

                    maxSize = maxAndIndexUpdateForRow(modifyBuilder, addedBuilded, removedBuilder, maxSize, sizes[idx],
                            currentRowKey, previousRowKey, prevSize, base);
                }
            } else {
                checkSizeArray(modified, referenceColumn, sizes, arrayColumn, name);
            }
        }
        for (final Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
            final ColumnSource<?> arrayColumn = es.getValue();
            final String name = es.getKey();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;

                final RowSet.Iterator iterator = modified.iterator();
                final RowSet.Iterator iteratorPreShift = modifiedPreShift.iterator();

                for (int i = 0; i < size; i++) {
                    final long currentRowKey = iterator.nextLong();
                    final long previousRowKey = iteratorPreShift.nextLong();


                    final Vector<?> array = (Vector<?>) arrayColumn.get(currentRowKey);
                    sizes[i] = (array == null ? 0 : array.size());
                    final Vector<?> prevArray = (Vector<?>) arrayColumn.getPrev(previousRowKey);
                    final long prevSize = (prevArray == null ? 0 : prevArray.size());

                    maxSize = maxAndIndexUpdateForRow(modifyBuilder, addedBuilded, removedBuilder, maxSize, sizes[i],
                            currentRowKey, previousRowKey, prevSize, base);
                }
            } else {
                checkSizeVector(modified, referenceColumn, sizes, arrayColumn, name);
            }
        }
        return maxSize;
    }

    private long maxAndIndexUpdateForRow(final RowSetBuilderSequential modifyBuilder,
            final RowSetBuilderSequential addedBuilded,
            final RowSetBuilderSequential removedBuilder,
            long maxSize,
            final long size,
            long currentRowKey,
            long previousRowKey,
            final long prevSize,
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
        maxSize = Math.max(maxSize, size);
        return maxSize;
    }

    private static long computeMaxSize(final RowSet rowSet,
                                       final Map<String, ColumnSource<?>> arrayColumns,
                                       final Map<String, ColumnSource<?>> vectorColumns,
                                       final long[] sizes,
                                       final boolean nullFill) {
        if (nullFill) {
            return computeMaxSizeNullFill(rowSet, arrayColumns, vectorColumns, sizes);
        }
        return computeMaxSizeNormal(rowSet, arrayColumns, vectorColumns, sizes);
    }

    private static long computeMaxSizePrev(final RowSet rowSet, final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns, final long[] sizes,
            final boolean nullFill) {
        if (nullFill) {
            return computePrevSizeNullFill(rowSet, arrayColumns, vectorColumns, sizes);
        }
        return computePrevSizeNormal(rowSet, arrayColumns, vectorColumns, sizes);
    }

    private static long computeMaxSizeNullFill(final RowSet rowset,
            final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns,
            final long[] sizes) {
        boolean sizeIsInitialized = false;

        long maxSize = 0;

        for (final Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
            final ColumnSource<?> arrayColumn = es.getValue();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                maxSize = computeMaxSizeArray(rowset, sizes, arrayColumn, maxSize);
            } else {
                maxSize = maybeIncreaseSizeArray(rowset, sizes, arrayColumn, maxSize);
            }
        }

        for (final Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
            final ColumnSource<?> vectorColumn = es.getValue();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                maxSize = computeMaxSizeVector(rowset, sizes, vectorColumn, maxSize);
            } else {
                maybeIncreaseSizeVector(rowset, sizes, vectorColumn, maxSize);
            }
        }

        return maxSize;
    }


    private static long computeMaxSizeNormal(final RowSet rowSet, final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns, final long[] sizes) {
        String referenceColumn = null;
        long maxSize = 0;
        boolean sizeIsInitialized = false;
        for (final Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
            final ColumnSource<?> arrayColumn = es.getValue();
            final String name = es.getKey();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;
                maxSize = computeMaxSizeArray(rowSet, sizes, arrayColumn, maxSize);
            } else {
                checkSizeArray(rowSet, referenceColumn, sizes, arrayColumn, name);
            }
        }
        for (final Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
            final ColumnSource<?> arrayColumn = es.getValue();
            final String name = es.getKey();
            if (!sizeIsInitialized) {
                sizeIsInitialized = true;
                referenceColumn = name;
                maxSize = computeMaxSizeVector(rowSet, sizes, arrayColumn, maxSize);
            } else {
                checkSizeVector(rowSet, referenceColumn, sizes, arrayColumn, name);
            }
        }
        return maxSize;
    }

    private static void checkSizeVector(RowSet rowSet, String referenceColumn, long[] sizes,
            ColumnSource<?> vectorColumn, String name) {
        final boolean isUngroupable = vectorColumn instanceof UngroupableColumnSource
                && ((UngroupableColumnSource) vectorColumn).isUngroupable();

        final RowSet.Iterator iterator = rowSet.iterator();
        final long size = rowSet.size();

        if (isUngroupable) {
            final UngroupableColumnSource ungroupable = (UngroupableColumnSource) vectorColumn;
            for (int idx = 0; idx < size; idx++) {
                final long expectedSize;
                expectedSize = ungroupable.getUngroupedSize(iterator.nextLong());
                Assert.assertion(sizes[idx] == expectedSize, "sizes[idx] == ((Vector)vectorColumn.get(idx)).size()",
                        referenceColumn, "referenceColumn", name, "vectorColumn.getName()", idx, "row");
            }
        } else {
            for (int idx = 0; idx < size; idx++) {
                final long expectedSize;
                final Vector<?> vector = (Vector<?>) vectorColumn.get(iterator.nextLong());
                expectedSize = vector != null ? vector.size() : 0;
                Assert.assertion(sizes[idx] == expectedSize, "sizes[idx] == ((Vector)vectorColumn.get(idx)).size()",
                        referenceColumn, "referenceColumn", name, "vectorColumn.getName()", idx, "row");
            }
        }
    }

    private static long computeMaxSizeVector(final RowSet rowset, final long[] sizes, final ColumnSource<?> arrayColumn,
            long maxSize) {
        final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                && ((UngroupableColumnSource) arrayColumn).isUngroupable();

        final RowSet.Iterator iterator = rowset.iterator();

        final int size = rowset.intSize();
        if (isUngroupable) {
            final UngroupableColumnSource ungroupable = (UngroupableColumnSource) arrayColumn;
            for (int ii = 0; ii < size; ii++) {
                sizes[ii] = ungroupable.getUngroupedSize(iterator.nextLong());
                maxSize = Math.max(maxSize, sizes[ii]);
            }
        } else {
            for (int ii = 0; ii < size; ii++) {
                final Vector<?> vector = (Vector<?>) arrayColumn.get(iterator.nextLong());
                sizes[ii] = vector != null ? vector.size() : 0;
                maxSize = Math.max(maxSize, sizes[ii]);
            }
        }
        return maxSize;
    }

    private static long maybeIncreaseSizeVector(final RowSet rowset, final long[] sizes,
            final ColumnSource<?> arrayColumn,
            long maxSize) {
        final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                && ((UngroupableColumnSource) arrayColumn).isUngroupable();

        final RowSet.Iterator iterator = rowset.iterator();

        final int size = rowset.intSize();
        if (isUngroupable) {
            final UngroupableColumnSource ungroupable = (UngroupableColumnSource) arrayColumn;
            for (int ii = 0; ii < size; ii++) {
                final long currentSize = ungroupable.getUngroupedSize(iterator.nextLong());
                sizes[ii] = Math.max(currentSize, sizes[ii]);
                maxSize = Math.max(maxSize, sizes[ii]);
            }
        } else {
            for (int ii = 0; ii < size; ii++) {
                final Vector<?> vector = (Vector<?>) arrayColumn.get(iterator.nextLong());
                final long currentSize = vector != null ? vector.size() : 0;
                sizes[ii] = Math.max(sizes[ii], currentSize);
                maxSize = Math.max(maxSize, sizes[ii]);
            }
        }
        return maxSize;
    }

    private static void checkSizeArray(final RowSet rowset, final String referenceColumn, final long[] sizes,
            final ColumnSource<?> arrayColumn, final String name) {
        final RowSet.Iterator iterator = rowset.iterator();
        for (int idx = 0; idx < rowset.size(); idx++) {
            final Object array = arrayColumn.get(iterator.nextLong());
            final int size = array == null ? 0 : Array.getLength(array);
            Assert.assertion(sizes[idx] == size,
                    "sizes[idx] == Array.getLength(arrayColumn.get(idx))",
                    referenceColumn, "referenceColumn", name, "name", idx, "row");
        }
    }

    private static long computeMaxSizeArray(final RowSet rowset, final long[] sizes, final ColumnSource<?> arrayColumn,
            long maxSize) {
        // TODO: Chunk This
        final RowSet.Iterator iterator = rowset.iterator();
        final long size = rowset.size();
        for (int idx = 0; idx < size; idx++) {
            final Object array = arrayColumn.get(iterator.nextLong());
            sizes[idx] = (array == null ? 0 : Array.getLength(array));
            maxSize = Math.max(maxSize, sizes[idx]);
        }
        return maxSize;
    }

    private static long maybeIncreaseSizeArray(final RowSet rowset, final long[] sizes,
            final ColumnSource<?> arrayColumn,
            long maxSize) {
        // TODO: Chunk This
        final RowSet.Iterator iterator = rowset.iterator();
        final long size = rowset.size();
        for (int idx = 0; idx < size; idx++) {
            final Object array = arrayColumn.get(iterator.nextLong());
            final int currentSize = array == null ? 0 : Array.getLength(array);
            sizes[idx] = Math.max(sizes[idx], currentSize);
            maxSize = Math.max(maxSize, sizes[idx]);
        }
        return maxSize;
    }

    private static long computePrevSize(final RowSet rowSet, final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns, final long[] sizes, final boolean nullFill) {
        if (nullFill) {
            return computePrevSizeNullFill(rowSet, arrayColumns, vectorColumns, sizes);
        } else {
            return computePrevSizeNormal(rowSet, arrayColumns, vectorColumns, sizes);
        }
    }

    private static long computePrevSizeNullFill(final RowSet rowset, final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns, final long[] sizes) {
        // TODO: Chunk This
        long maxSize = 0;
        final RowSet.Iterator iterator = rowset.iterator();
        for (int idx = 0; idx < rowset.size(); idx++) {
            long localMax = 0;
            final long nextIndex = iterator.nextLong();
            for (final Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final Object array = arrayColumn.getPrev(nextIndex);
                final long size = (array == null ? 0 : Array.getLength(array));
                localMax = Math.max(localMax, size);

            }
            for (final Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                        && ((UngroupableColumnSource) arrayColumn).isUngroupable();
                final long size;
                if (isUngroupable) {
                    size = ((UngroupableColumnSource) arrayColumn).getUngroupedPrevSize(nextIndex);
                } else {
                    final Vector<?> vector = (Vector<?>) arrayColumn.getPrev(nextIndex);
                    size = vector != null ? vector.size() : 0;
                }
                localMax = Math.max(localMax, size);
            }
            sizes[idx] = localMax;
            maxSize = Math.max(maxSize, localMax);
        }
        return maxSize;
    }

    private static long computePrevSizeNormal(final RowSet rowset, final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns, final long[] sizes) {
        for (final ColumnSource<?> arrayColumn : arrayColumns.values()) {
            // In the "normal" case we've defined the sizes to all match; so we can check only the first column
            return computePreviousArraySize(rowset, sizes, arrayColumn);
        }
        for (final ColumnSource<?> arrayColumn : vectorColumns.values()) {
            // In the "normal" case we've defined the sizes to all match; so we can check only the first column
            return computePreviousVectorSize(rowset, sizes, arrayColumn);
        }
        throw new IllegalStateException("No columns to ungroup!");
    }

    private static long computePreviousVectorSize(final RowSet rowset, final long[] sizes,
            final ColumnSource<?> arrayColumn) {
        final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                && ((UngroupableColumnSource) arrayColumn).isUngroupable();

        final RowSet.Iterator iterator = rowset.iterator();
        final long size = rowset.intSize();
        long maxSize = 0;

        if (isUngroupable) {
            final UngroupableColumnSource ungroupable = (UngroupableColumnSource) arrayColumn;
            for (int idx = 0; idx < size; idx++) {
                sizes[idx] = ungroupable.getUngroupedPrevSize(iterator.nextLong());
                maxSize = Math.max(maxSize, sizes[idx]);
            }
        } else {
            for (int idx = 0; idx < size; idx++) {
                final Vector<?> array = (Vector<?>) arrayColumn.getPrev(iterator.nextLong());
                sizes[idx] = array == null ? 0 : array.size();
                maxSize = Math.max(maxSize, sizes[idx]);
            }
        }

        return maxSize;
    }

    private static long computePreviousArraySize(final RowSet rowset, final long[] sizes,
            final ColumnSource<?> arrayColumn) {
        final long size = rowset.size();
        long maxSize = 0;
        final RowSet.Iterator iterator = rowset.iterator();
        for (int idx = 0; idx < size; idx++) {
            final Object array = arrayColumn.getPrev(iterator.nextLong());
            sizes[idx] = (array == null ? 0 : Array.getLength(array));
            maxSize = Math.max(maxSize, sizes[idx]);
        }
        return maxSize;
    }

    private static long[] computeSize(final RowSet rowSet, final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns, final boolean nullFill) {
        if (nullFill) {
            return computeSizeNullFill(rowSet, arrayColumns, vectorColumns);
        }

        return computeSizeNormal(rowSet, arrayColumns, vectorColumns);
    }

    private static long[] computeSizeNullFill(final RowSet rowSet, final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns) {
        final long[] sizes = new long[rowSet.intSize("ungroup")];
        final RowSet.Iterator iterator = rowSet.iterator();
        for (int i = 0; i < rowSet.size(); i++) {
            long localMax = 0;
            final long nextIndex = iterator.nextLong();
            for (final Map.Entry<String, ColumnSource<?>> es : arrayColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final Object array = arrayColumn.get(nextIndex);
                final long size = (array == null ? 0 : Array.getLength(array));
                localMax = Math.max(localMax, size);

            }
            for (final Map.Entry<String, ColumnSource<?>> es : vectorColumns.entrySet()) {
                final ColumnSource<?> arrayColumn = es.getValue();
                final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                        && ((UngroupableColumnSource) arrayColumn).isUngroupable();
                final long size;
                if (isUngroupable) {
                    size = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(nextIndex);
                } else {
                    final Vector<?> vector = (Vector<?>) arrayColumn.get(nextIndex);
                    size = vector != null ? vector.size() : 0;
                }
                localMax = Math.max(localMax, size);
            }
            sizes[i] = localMax;
        }
        return sizes;
    }

    private static long[] computeSizeNormal(final RowSet rowSet, final Map<String, ColumnSource<?>> arrayColumns,
            final Map<String, ColumnSource<?>> vectorColumns) {
        final long[] sizes = new long[rowSet.intSize("ungroup")];
        for (final ColumnSource<?> arrayColumn : arrayColumns.values()) {
            final RowSet.Iterator iterator = rowSet.iterator();
            for (int i = 0; i < rowSet.size(); i++) {
                final Object array = arrayColumn.get(iterator.nextLong());
                sizes[i] = (array == null ? 0 : Array.getLength(array));
            }
            return sizes; // TODO: WTF??
        }
        for (final ColumnSource<?> arrayColumn : vectorColumns.values()) {
            final boolean isUngroupable = arrayColumn instanceof UngroupableColumnSource
                    && ((UngroupableColumnSource) arrayColumn).isUngroupable();

            final RowSet.Iterator iterator = rowSet.iterator();
            for (int i = 0; i < rowSet.size(); i++) {
                if (isUngroupable) {
                    sizes[i] = ((UngroupableColumnSource) arrayColumn).getUngroupedSize(iterator.nextLong());
                } else {
                    final Vector<?> array = (Vector<?>) arrayColumn.get(iterator.nextLong());
                    sizes[i] = array == null ? 0 : array.size();
                }
            }
            return sizes; // TODO: WTF??
        }
        return null;
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
        private final Map<String, ColumnSource<?>> arrayColumns;
        private final TrackingRowSet parentRowset;
        private final Map<String, ColumnSource<?>> vectorColumns;
        private final Map<String, ColumnSource<?>> resultMap;

        public UngroupListener(final QueryTable result, final CrossJoinShiftState shiftState,
                final Map<String, ColumnSource<?>> arrayColumns, final TrackingRowSet parentRowset,
                final Map<String, ColumnSource<?>> vectorColumns, final Map<String, ColumnSource<?>> resultMap) {
            super("ungroup(" + Arrays.deepToString(UngroupOperation.this.columnsToUngroupBy) + ')',
                    UngroupOperation.this.parent, result);
            this.result = result;
            this.shiftState = shiftState;
            this.arrayColumns = arrayColumns;
            this.parentRowset = parentRowset;
            this.vectorColumns = vectorColumns;
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

            if (!clearResult) {
                newBase = evaluateIndex(upstream.added(), ungroupAdded, newBase);
                newBase = evaluateModified(upstream.modified(), upstream.getModifiedPreShift(), ungroupModifiedModified,
                        ungroupModifiedAdded, ungroupModifiedRemoved, newBase);
            }

            if (newBase != shiftState.getNumShiftBits()) {
                rebase(newBase + 1);
            } else {
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
                    final long base = newBase;
                    final long currentBaseSize = (1L << newBase) - 1;

                    final RowSetShiftData upstreamShift = upstream.shifted();
                    final long shiftSize = upstreamShift.size();
                    for (int ii = 0; ii < shiftSize; ++ii) {
                        final long begin = upstreamShift.getBeginRange(ii);
                        final long end = upstreamShift.getEndRange(ii);
                        final long shiftDelta = upstreamShift.getShiftDelta(ii);

                        final long resultShiftAmount = shiftDelta << base;

                        for (long rk = begin; rk <= end; rk++) {
                            final long oldRangeStart = rk << base;
                            final long oldRangeEnd = (rk << base) + currentBaseSize;
                            // get the range from our result rowset, I'm not sure that I love creating a rowset for each
                            // row; we could instead do iteration because we are not actually modifying this thing as we
                            // go
                            try (final WritableRowSet expandedRowsetForRow = resultRowset.subSetByKeyRange(oldRangeStart, oldRangeEnd)) {
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
        }

        private void rebase(final int newBase) {
            // TODO: would be nice to shift this and have a real answer instead of just declaring "everything" different

            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            getUngroupIndex(computeSize(parentRowset, arrayColumns, vectorColumns, nullFill), builder, newBase,
                    parentRowset);
            final WritableRowSet newRowSet = builder.build();
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

        private int evaluateIndex(final RowSet rowSet, final RowSetBuilderSequential ungroupBuilder,
                final int newBase) {
            if (rowSet.isEmpty()) {
                return newBase;
            }
            final long[] sizes = new long[rowSet.intSize("ungroup")];
            final long maxSize = computeMaxSize(rowSet, arrayColumns, vectorColumns, sizes, nullFill);
            final int minBase = 64 - Long.numberOfLeadingZeros(maxSize);
            // TODO: we could be throwing this out, maybe we shouldn't build it
            getUngroupIndex(sizes, ungroupBuilder, shiftState.getNumShiftBits(), rowSet);
            return Math.max(newBase, minBase);
        }

        private void evaluateRemovedIndex(final RowSet rowSet, final RowSetBuilderSequential builder) {
            if (rowSet.isEmpty()) {
                return;
            }
            final long[] sizes = new long[rowSet.intSize("ungroup")];
            computePrevSize(rowSet, arrayColumns, vectorColumns, sizes, nullFill);
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
                    arrayColumns,
                    vectorColumns,
                    modifyBuilder,
                    addedBuilded,
                    removedBuilder,
                    shiftState.getNumShiftBits(),
                    nullFill);
            final int minBase = 64 - Long.numberOfLeadingZeros(maxSize);
            return Math.max(newBase, minBase);
        }
    }
}

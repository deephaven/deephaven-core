/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRegionBinarySearchKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.SortColumn;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sort.timsort.ByteTimsortDescendingKernel;
import io.deephaven.engine.table.impl.sort.timsort.ByteTimsortKernel;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionByte;
import io.deephaven.util.compare.ByteComparisons;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;

public class ByteRegionBinarySearchKernel {
    public static RowSet binSearchMatch(
            ColumnRegionByte<?> region,
            long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] sortedKeys) {
        final SortColumn.Order order = sortColumn.order();
        final byte[] unboxed = ArrayTypeUtils.getUnboxedByteArray(sortedKeys);
        if (order == SortColumn.Order.DESCENDING) {
            try (final ByteTimsortDescendingKernel.ByteSortKernelContext<Any> context =
                         ByteTimsortDescendingKernel.createContext(unboxed.length)) {
                context.sort(WritableByteChunk.writableChunkWrap(unboxed));
            }
        } else {
            try (final ByteTimsortKernel.ByteSortKernelContext<Any> context =
                         ByteTimsortKernel.createContext(unboxed.length)) {
                context.sort(WritableByteChunk.writableChunkWrap(unboxed));
            }
        }

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (final byte toFind : unboxed) {
            final long lastFound = binSearchSingle(region, builder, firstKey, lastKey, order, toFind);

            if (lastFound >= 0) {
                firstKey = lastFound + 1;
            }
        }

        return builder.build();
    }

    /**
     * Find the extents of the range containing the key to find, returning the last index found.
     *
     * @param builder       the builder to accumulate into
     * @param firstKey      the key to start searching
     * @param lastKey       the key to end searching
     * @param sortDirection the sort direction of the column
     * @param toFind        the element to find
     * @return the last key in the found range.
     */
    private static long binSearchSingle(
            @NotNull final ColumnRegionByte<?> region,
            @NotNull final RowSetBuilderSequential builder,
            final long firstKey,
            final long lastKey,
            SortColumn.Order sortDirection,
            final byte toFind) {
        // Find the beginning of the range
        long matchStart = findRangePart(region, toFind, firstKey, lastKey, sortDirection, -1);
        if (matchStart < 0) {
            return -1;
        }

        // Now we have to locate the actual start and end of the range.
        long matchEnd = matchStart;
        if (matchStart < lastKey && ByteComparisons.eq(region.getByte(matchStart + 1),toFind)) {
            matchEnd = findRangePart(region, toFind, matchStart + 1, lastKey, sortDirection, 1);
        }

        builder.appendRange(matchStart, matchEnd);
        return matchEnd;
    }

    private static long findRangePart(
            @NotNull final ColumnRegionByte<?> region,
            final byte toFind,
            long start,
            long end,
            final SortColumn.Order sortDirection,
            final int rangeDirection) {
        final int sortDirectionInt = sortDirection == SortColumn.Order.ASCENDING ? 1 : -1;
        long matchStart = -1;
        while (start <= end) {
            long pivot = (start + end) >>> 1;
            final byte curVal = region.getByte(pivot);
            final int comparison = ByteComparisons.compare(curVal, toFind) * sortDirectionInt;
            if (comparison < 0) {
                start = pivot + 1;
            } else if (comparison == 0) {
                matchStart = pivot;
                if (rangeDirection > 0) {
                    start = pivot + 1;
                } else {
                    end = pivot - 1;
                }
            } else {
                end = pivot - 1;
            }
        }

        return matchStart;
    }
}

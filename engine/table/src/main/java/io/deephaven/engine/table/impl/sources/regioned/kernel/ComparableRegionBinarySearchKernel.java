//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.SortColumn;
import io.deephaven.api.SortSpec;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sort.timsort.ObjectTimsortDescendingKernel;
import io.deephaven.engine.table.impl.sort.timsort.ObjectTimsortKernel;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionObject;
import io.deephaven.util.compare.ObjectComparisons;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.insertionPoint;

/**
 * Match search over a sorted column region whose data type is not known to order consistently with equals -- see
 * {@link BinarySearchKernelHelper#compareConsistentWithEquality(Class)}. Where {@link ObjectRegionBinarySearchKernel}
 * can answer a match with the run its bounds locate, this kernel must treat that run as a superset and pick the matches
 * out of it by equality, which is the relation the chunk filter it stands in for uses.
 *
 * <p>
 * Rows are read one at a time, as {@link ObjectRegionBinarySearchKernel} reads them: a region is flat and addressed
 * directly by row key, so chunked reads measure no faster here. Its column counterpart,
 * {@link ComparableColumnBinarySearchKernel}, does chunk, because a column source's selection may be sparse.
 *
 * <p>
 * Only the match search differs; the bounds it navigates by, and every range search, are
 * {@link ObjectRegionBinarySearchKernel}'s.
 */
public class ComparableRegionBinarySearchKernel {

    private ComparableRegionBinarySearchKernel() {}

    /**
     * Performs a binary search on a given column region to find the row keys that are equal to one of
     * {@code searchValues}. The method returns the {@link RowSet} containing the matched row keys.
     * <p>
     * NB: equality is determined by {@link ObjectComparisons#eq(Object, Object)}, which may differ from
     * {@code compareTo()} for certain types. For example,
     * {@code new BigDecimal("1.0").compareTo(new BigDecimal("1.00")) == 0} but
     * {@code new BigDecimal("1.0").equals(new BigDecimal("1.00")) == false}.
     *
     * @param region The column region in which the search will be performed.
     * @param firstKey The first key in the column region to consider for the search.
     * @param lastKey The last key in the column region to consider for the search.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param searchValues An array of keys to find within the column region.
     *
     * @return A {@link RowSet} containing the row keys that are equal to one of the search values.
     */
    public static RowSet binarySearchMatch(
            @NotNull final ColumnRegionObject<?, ?> region,
            long firstKey,
            final long lastKey,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues) {
        final SortSpec.Order order = sortColumn.order();
        final Object[] copiedValues = Arrays.copyOf(searchValues, searchValues.length);
        if (sortColumn.isAscending()) {
            try (final ObjectTimsortKernel.ObjectSortKernelContext<Any> context =
                    ObjectTimsortKernel.createContext(copiedValues.length)) {
                context.sort(WritableObjectChunk.writableChunkWrap(copiedValues));
            }
        } else {
            try (final ObjectTimsortDescendingKernel.ObjectSortKernelContext<Any> context =
                    ObjectTimsortDescendingKernel.createContext(copiedValues.length)) {
                context.sort(WritableObjectChunk.writableChunkWrap(copiedValues));
            }
        }

        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final boolean ascending = order.isAscending();

        for (int idx = 0; idx < copiedValues.length && firstKey <= lastKey;) {
            // First, we are identifying a set of comparison-equal values.
            int groupEnd = idx + 1;
            while (groupEnd < copiedValues.length
                    && ObjectComparisons.compare(copiedValues[groupEnd], copiedValues[idx]) == 0) {
                ++groupEnd;
            }
            // Second, find the bounds of a run in the region that compares equal to the search value.
            final Object toFind = copiedValues[idx];
            final long lowerResult = ascending
                    ? ObjectRegionBinarySearchKernel.lowerBoundAscending(region, firstKey, lastKey, toFind, true)
                    : ObjectRegionBinarySearchKernel.lowerBoundDescending(region, firstKey, lastKey, toFind, true);
            final long runStart = lowerResult >= 0 ? lowerResult : insertionPoint(lowerResult);
            final long upperResult = ascending
                    ? ObjectRegionBinarySearchKernel.upperBoundAscending(region, runStart, lastKey, toFind, true)
                    : ObjectRegionBinarySearchKernel.upperBoundDescending(region, runStart, lastKey, toFind, true);
            final long runEnd = upperResult >= 0 ? upperResult + 1 : insertionPoint(upperResult);
            // Third, check each value of the run for Object equality with the set of comparison-equal search values.
            for (long key = runStart; key < runEnd; ++key) {
                final Object value = region.getObject(key);
                for (int valueIdx = idx; valueIdx < groupEnd; ++valueIdx) {
                    if (ObjectComparisons.eq(value, copiedValues[valueIdx])) {
                        // This row matches at least one of the search values, so add its row key to the result.
                        builder.appendKey(key);
                        break;
                    }
                }
            }
            firstKey = runEnd;
            idx = groupEnd;
        }

        return builder.build();
    }
}

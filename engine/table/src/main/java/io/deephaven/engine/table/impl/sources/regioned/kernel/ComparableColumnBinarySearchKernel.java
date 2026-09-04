//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.SortColumn;
import io.deephaven.api.SortSpec;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sort.timsort.ObjectTimsortDescendingKernel;
import io.deephaven.engine.table.impl.sort.timsort.ObjectTimsortKernel;
import io.deephaven.util.compare.ObjectComparisons;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.insertionPoint;

/**
 * Match search over a sorted column source whose data type is not known to order consistently with equals -- see
 * {@link BinarySearchKernelHelper#compareConsistentWithEquality(Class)}. Where {@link ObjectColumnBinarySearchKernel}
 * can answer a match with the run its bounds locate, this kernel must treat that run as a superset and pick the matches
 * out of it by equality, which is the relation the chunk filter it stands in for uses.
 *
 * <p>
 * Each run is read through chunked row-sequence iteration rather than a position lookup and a single-row get per row,
 * because a column source's selection may be sparse, where mapping a position to a row key is far more expensive than
 * the arithmetic a flat selection needs. Its region counterpart, {@link ComparableRegionBinarySearchKernel}, reads row
 * by row, since a region is flat and addressed directly by row key.
 *
 * <p>
 * Only the match search differs; the bounds it navigates by, and every range search, are
 * {@link ObjectColumnBinarySearchKernel}'s.
 */
public class ComparableColumnBinarySearchKernel {

    private static final int CHUNK_SIZE = 2048;

    private ComparableColumnBinarySearchKernel() {}

    /**
     * Performs a binary search on a given sorted {@link ColumnSource} to find the row keys from a provided
     * {@link RowSet} that are equal to one of {@code searchValues}. The method returns the {@link RowSet} containing
     * the matched row keys.
     * <p>
     * NB: equality is determined by {@link ObjectComparisons#eq(Object, Object)}, which may differ from
     * {@code compareTo()} for certain types. For example,
     * {@code new BigDecimal("1.0").compareTo(new BigDecimal("1.00")) == 0} but
     * {@code new BigDecimal("1.0").equals(new BigDecimal("1.00")) == false}.
     *
     * @param source The column source in which the search will be performed.
     * @param selection The {@link RowSet} defining which rows are populated and the order in which they are searched.
     * @param sortColumn A {@link SortColumn} object representing the sorting order of the column.
     * @param searchValues An array of keys to find within the source.
     * @param usePrev If true, the search will use the previous values instead of current values.
     *
     * @return A {@link RowSet} containing the row keys that are equal to one of the search values.
     */
    public static RowSet binarySearchMatch(
            @NotNull final ColumnSource<?> source,
            @NotNull final RowSet selection,
            @NotNull final SortColumn sortColumn,
            @NotNull final Object[] searchValues,
            final boolean usePrev) {
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
        final long lastPos = selection.size() - 1;
        final boolean ascending = order.isAscending();
        long firstPos = 0;

        // Everything a run scan needs is allocated once here and reused across every run: the context and chunks are
        // sized together, and runs are located in increasing position order, so one iterator can advance across all
        // of them.
        final int contextSize = (int) Math.min(CHUNK_SIZE, selection.size());
        try (final ColumnSource.GetContext getContext = source.makeGetContext(contextSize);
                final WritableLongChunk<OrderedRowKeys> keys = WritableLongChunk.makeWritableChunk(contextSize);
                final WritableLongChunk<OrderedRowKeys> matches = WritableLongChunk.makeWritableChunk(contextSize);
                final RowSequence.Iterator rsIt = selection.getRowSequenceIterator()) {
            for (int idx = 0; idx < copiedValues.length && firstPos <= lastPos;) {
                // First, we are identifying a set of comparison-equal values.
                int groupEnd = idx + 1;
                while (groupEnd < copiedValues.length
                        && ObjectComparisons.compare(copiedValues[groupEnd], copiedValues[idx]) == 0) {
                    ++groupEnd;
                }
                // Second, find the bounds of a run in the column that compares equal to the search value. These
                // bounds are positions within selection, not row keys, so their difference is a row count
                // even when selection is sparse.
                final Object toFind = copiedValues[idx];
                final long lowerResult = ascending
                        ? ObjectColumnBinarySearchKernel.lowerBoundAscending(
                                source, selection, firstPos, lastPos, toFind, true, usePrev)
                        : ObjectColumnBinarySearchKernel.lowerBoundDescending(
                                source, selection, firstPos, lastPos, toFind, true, usePrev);
                final long runStartPos = lowerResult >= 0 ? lowerResult : insertionPoint(lowerResult);
                final long upperResult = ascending
                        ? ObjectColumnBinarySearchKernel.upperBoundAscending(
                                source, selection, runStartPos, lastPos, toFind, true, usePrev)
                        : ObjectColumnBinarySearchKernel.upperBoundDescending(
                                source, selection, runStartPos, lastPos, toFind, true, usePrev);
                final long runEndPos = upperResult >= 0 ? upperResult + 1 : insertionPoint(upperResult);
                if (runEndPos > runStartPos) {
                    // Third, check each value of the run for Object equality with the set of comparison-equal
                    // search values. Resolving runStartPos is the only place a position becomes a row key; runs
                    // advance forward, so the one iterator serves them all.
                    rsIt.advance(selection.get(runStartPos));
                    long remaining = runEndPos - runStartPos;
                    while (remaining > 0 && rsIt.hasMore()) {
                        final RowSequence rows = rsIt.getNextRowSequenceWithLength(Math.min(contextSize, remaining));
                        final ObjectChunk<?, ? extends Values> valueChunk = (usePrev
                                ? source.getPrevChunk(getContext, rows)
                                : source.getChunk(getContext, rows)).asObjectChunk();
                        rows.fillRowKeyChunk(keys);
                        matches.setSize(0);
                        for (int ii = 0; ii < valueChunk.size(); ++ii) {
                            final Object value = valueChunk.get(ii);
                            for (int valueIdx = idx; valueIdx < groupEnd; ++valueIdx) {
                                if (ObjectComparisons.eq(value, copiedValues[valueIdx])) {
                                    // This row matches at least one of the search values, so add it to the
                                    // matches chunk.
                                    matches.add(keys.get(ii));
                                    break;
                                }
                            }
                        }
                        builder.appendOrderedRowKeysChunk(matches);
                        remaining -= valueChunk.size();
                    }
                }
                firstPos = runEndPos;
                idx = groupEnd;
            }
        }

        return builder.build();
    }
}

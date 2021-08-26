package io.deephaven.db.tables.verify;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.sortcheck.SortCheck;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.utils.ChunkUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;

public class SortedAssertionInstrumentedListenerAdapter extends BaseTable.ShiftAwareListenerImpl {
    private static final int CHUNK_SIZE = 1 << 16;
    private final String description;
    private final String column;
    private final SortingOrder order;
    private final ModifiedColumnSet parentColumnSet;
    private final Index parentIndex;
    private final ColumnSource<?> parentColumnSource;
    private final SortCheck sortCheck;

    public SortedAssertionInstrumentedListenerAdapter(String description,
            DynamicTable parent,
            DynamicTable dependent,
            String columnName,
            SortingOrder order) {
        super(
                "assertSorted(" + (description == null ? "" : description) + ", " + columnName + ", " + order + ')',
                parent, dependent);
        this.description = description;
        this.column = columnName;
        this.order = order;
        parentIndex = parent.getIndex();
        parentColumnSource = parent.getColumnSource(columnName);
        parentColumnSet = parent.newModifiedColumnSet(columnName);
        sortCheck = SortCheck.make(parentColumnSource.getChunkType(), order == SortingOrder.Descending);
    }

    @Override
    public void onUpdate(final Update upstream) {
        final boolean modifiedRows =
                upstream.modified.nonempty() && upstream.modifiedColumnSet.containsAny(parentColumnSet);
        if (upstream.added.nonempty() || modifiedRows) {
            final Index rowsOfInterest = modifiedRows ? upstream.added.union(upstream.modified) : upstream.added;
            try (final Index ignored = modifiedRows ? rowsOfInterest : null;
                    final Index toProcess = makeAdjacentIndex(rowsOfInterest)) {
                Assert.assertion(toProcess.subsetOf(parentIndex), "toProcess.subsetOf(parentIndex)",
                        makeAdjacentIndex(rowsOfInterest), "toProcess", parentIndex, "parentIndex");
                doCheck(toProcess);
            }
        }
        super.onUpdate(upstream);
    }

    private void doCheck(Index toProcess) {
        doCheckStatic(toProcess, parentColumnSource, sortCheck, description, column, order);
    }

    public static void doCheckStatic(Index toProcess, ColumnSource<?> parentColumnSource, SortCheck sortCheck,
            String description, String column, SortingOrder order) {
        final int contextSize = (int) Math.min(CHUNK_SIZE, toProcess.size());

        try (final ChunkSource.GetContext getContext = parentColumnSource.makeGetContext(contextSize);
                final OrderedKeys.Iterator okIt = toProcess.getOrderedKeysIterator()) {
            while (okIt.hasMore()) {
                final OrderedKeys chunkOk = okIt.getNextOrderedKeysWithLength(contextSize);
                final Chunk<? extends Attributes.Values> valuesChunk = parentColumnSource.getChunk(getContext, chunkOk);
                final int firstUnsorted = sortCheck.sortCheck(valuesChunk);
                if (firstUnsorted >= 0) {
                    final String value1 = ChunkUtils.extractKeyStringFromChunk(valuesChunk.getChunkType(), valuesChunk,
                            firstUnsorted);
                    final String value2 = ChunkUtils.extractKeyStringFromChunk(valuesChunk.getChunkType(), valuesChunk,
                            firstUnsorted + 1);
                    throw new SortedAssertionFailure(description, column, order, value1, value2);
                }
            }
        }
    }

    private Index makeAdjacentIndex(Index rowsOfInterest) {
        try (final Index inverted = parentIndex.invert(rowsOfInterest)) {
            final Index.SequentialBuilder processBuilder = Index.CURRENT_FACTORY.getSequentialBuilder();
            long lastPosition = parentIndex.size() - 1;
            long lastUsedPosition = 0;
            for (ReadOnlyIndex.RangeIterator rangeIterator = inverted.rangeIterator(); rangeIterator.hasNext();) {
                rangeIterator.next();
                long start = rangeIterator.currentRangeStart();
                long end = rangeIterator.currentRangeEnd();

                if (start - 1 > lastUsedPosition) {
                    start--;
                }
                if (end < lastPosition) {
                    end++;
                }

                processBuilder.appendRange(start, end);
                lastUsedPosition = end;
            }
            try (final Index positions = processBuilder.getIndex()) {
                return parentIndex.subindexByPos(positions);
            }
        }
    }
}

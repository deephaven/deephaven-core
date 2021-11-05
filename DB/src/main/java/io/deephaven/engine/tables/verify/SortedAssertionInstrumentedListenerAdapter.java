package io.deephaven.engine.tables.verify;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.tables.SortingOrder;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.v2.BaseTable;
import io.deephaven.engine.v2.ModifiedColumnSet;
import io.deephaven.engine.v2.sortcheck.SortCheck;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.Chunk;
import io.deephaven.engine.v2.sources.chunk.ChunkSource;
import io.deephaven.engine.v2.utils.*;
import io.deephaven.engine.structures.RowSequence;

public class SortedAssertionInstrumentedListenerAdapter extends BaseTable.ListenerImpl {
    private static final int CHUNK_SIZE = 1 << 16;
    private final String description;
    private final String column;
    private final SortingOrder order;
    private final ModifiedColumnSet parentColumnSet;
    private final RowSet parentRowSet;
    private final ColumnSource<?> parentColumnSource;
    private final SortCheck sortCheck;

    public SortedAssertionInstrumentedListenerAdapter(String description,
            Table parent,
            Table dependent,
            String columnName,
            SortingOrder order) {
        super(
                "assertSorted(" + (description == null ? "" : description) + ", " + columnName + ", " + order + ')',
                parent, dependent);
        this.description = description;
        this.column = columnName;
        this.order = order;
        parentRowSet = parent.getRowSet();
        parentColumnSource = parent.getColumnSource(columnName);
        parentColumnSet = parent.newModifiedColumnSet(columnName);
        sortCheck = SortCheck.make(parentColumnSource.getChunkType(), order == SortingOrder.Descending);
    }

    @Override
    public void onUpdate(final Update upstream) {
        final boolean modifiedRows =
                upstream.modified.isNonempty() && upstream.modifiedColumnSet.containsAny(parentColumnSet);
        if (upstream.added.isNonempty() || modifiedRows) {
            final RowSet rowsOfInterest = modifiedRows ? upstream.added.union(upstream.modified) : upstream.added;
            try (final RowSet ignored = modifiedRows ? rowsOfInterest : null;
                 final RowSet toProcess = makeAdjacentIndex(rowsOfInterest)) {
                Assert.assertion(toProcess.subsetOf(parentRowSet), "toProcess.subsetOf(parentRowSet)",
                        makeAdjacentIndex(rowsOfInterest), "toProcess", parentRowSet, "parentRowSet");
                doCheck(toProcess);
            }
        }
        super.onUpdate(upstream);
    }

    private void doCheck(RowSet toProcess) {
        doCheckStatic(toProcess, parentColumnSource, sortCheck, description, column, order);
    }

    public static void doCheckStatic(RowSet toProcess, ColumnSource<?> parentColumnSource, SortCheck sortCheck,
                                     String description, String column, SortingOrder order) {
        final int contextSize = (int) Math.min(CHUNK_SIZE, toProcess.size());

        try (final ChunkSource.GetContext getContext = parentColumnSource.makeGetContext(contextSize);
                final RowSequence.Iterator rsIt = toProcess.getRowSequenceIterator()) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(contextSize);
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

    private RowSet makeAdjacentIndex(RowSet rowsOfInterest) {
        try (final RowSet inverted = parentRowSet.invert(rowsOfInterest)) {
            final RowSetBuilderSequential processBuilder = RowSetFactoryImpl.INSTANCE.builderSequential();
            long lastPosition = parentRowSet.size() - 1;
            long lastUsedPosition = 0;
            for (RowSet.RangeIterator rangeIterator = inverted.rangeIterator(); rangeIterator.hasNext();) {
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
            try (final RowSet positions = processBuilder.build()) {
                return parentRowSet.subSetForPositions(positions);
            }
        }
    }
}

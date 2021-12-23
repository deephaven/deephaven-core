package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.impl.ShiftedRowSequence;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.SafeCloseableList;
import org.jetbrains.annotations.NotNull;

/**
 * A firstBy aggregation operator for stream tables.
 *
 * @see Table#STREAM_TABLE_ATTRIBUTE
 */
public class StreamFirstChunkedOperator extends BaseStreamFirstOrLastChunkedOperator {

    /**
     * <p>
     * The next destination slot that we expect to be used.
     * <p>
     * Any destination less than this one can safely be ignored while processing adds since the first row can never
     * change once a destination has been created given that we ignore removes.
     */
    private long nextDestination;

    /**
     * <p>
     * The first destination that we used on the current step (if we used any). At the very beginning of a step, this is
     * equivalent to {@link #nextDestination} and also the result table's size.
     * <p>
     * We use this as an offset shift for {@code redirections}, so that {@code redirections} only needs to hold first
     * source keys for newly-added destinations, rather than the entire space.
     * <p>
     * At the end of a step, this is updated to prepare for the next step.
     */
    private long firstDestinationThisStep;

    StreamFirstChunkedOperator(@NotNull final MatchPair[] resultPairs, @NotNull final Table streamTable) {
        super(resultPairs, streamTable);
    }

    @Override
    public final boolean unchunkedRowSet() {
        return true;
    }

    @Override
    public final void startTrackingPrevValues() {
        // We never change the value at any key in outputColumns since there are no removes; consequently there's no
        // need to enable previous value tracking.
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        redirections.ensureCapacity(tableSize - firstDestinationThisStep, false);
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext,
            final Chunk<? extends Values> values, // Unused
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations,
            @NotNull final IntChunk<ChunkPositions> startPositions,
            final IntChunk<ChunkLengths> length, // Unused
            @NotNull final WritableBooleanChunk<Values> stateModified) {

        final StreamFirstBucketedContext context = (StreamFirstBucketedContext) bucketedContext;

        long maxDestination = nextDestination - 1;

        // we can essentially do a radix sort; anything less than nextDestination is not of interest; everything else
        // must fall between nextDestination and our chunk size
        context.rowKeyToInsert.fillWithValue(0, startPositions.size(), Long.MAX_VALUE);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int destination = destinations.get(startPosition);
            if (destination >= nextDestination) {
                Assert.lt(destination, "destination", nextDestination + startPositions.size(),
                        "nextDestination + startPositions.size()");
                maxDestination = Math.max(destination, maxDestination);

                final long inputRowKey = inputRowKeys.get(startPosition);
                final int index = (int) (destination - nextDestination);

                context.destinationsToInsert.set(index, destination);
                context.rowKeyToInsert.set(index, Math.min(context.rowKeyToInsert.get(index), inputRowKey));
            }
        }
        context.destinationsToInsert.setSize((int) (maxDestination - nextDestination + 1));
        context.rowKeyToInsert.setSize((int) (maxDestination - nextDestination + 1));

        for (int ii = 0; ii < context.destinationsToInsert.size(); ++ii) {
            final int destination = context.destinationsToInsert.get(ii);
            final long rowKey = context.rowKeyToInsert.get(ii);
            redirections.set(destination - firstDestinationThisStep, rowKey);
        }

        nextDestination = maxDestination + 1;
    }

    @Override
    public boolean addChunk(final SingletonContext context, // Unused
            final int chunkSize,
            final Chunk<? extends Values> values, // Unused
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            final long destination) {
        if (chunkSize == 0) {
            return false;
        }
        return maybeAssignFirst(destination, inputRowKeys.get(0));
    }

    @Override
    public boolean addRowSet(final SingletonContext context,
            @NotNull final RowSet rowSet,
            final long destination) {
        if (rowSet.isEmpty()) {
            return false;
        }
        return maybeAssignFirst(destination, rowSet.firstRowKey());
    }

    private boolean maybeAssignFirst(final long destination, final long sourceIndexKey) {
        if (destination < nextDestination) {
            // Skip anything that's not new, it cannot change the first key
            return false;
        }
        if (destination == nextDestination) {
            redirections.set(nextDestination++ - firstDestinationThisStep, sourceIndexKey);
        } else {
            // noinspection ThrowableNotThrown
            Assert.statementNeverExecuted(
                    "Destination " + destination + " greater than next destination " + nextDestination);
        }
        return true;
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable) {
        copyStreamToResult(resultTable.getRowSet());
        redirections = null;
        Assert.eq(resultTable.size(), "resultTable.size()", nextDestination, "nextDestination");
        firstDestinationThisStep = nextDestination;
    }

    @Override
    public void propagateUpdates(@NotNull final TableUpdate downstream,
            @NotNull final RowSet newDestinations) {
        // NB: We cannot assert no modifies; other operators in the same aggregation might modify columns not in our
        // result set.
        Assert.assertion(downstream.removed().isEmpty() && downstream.shifted().empty(),
                "downstream.removed.empty() && downstream.shifted.empty()");
        copyStreamToResult(downstream.added());
        redirections = null;
        if (downstream.added().isNonempty()) {
            Assert.eq(downstream.added().lastRowKey() + 1, "downstream.added.lastRowKey() + 1", nextDestination,
                    "nextDestination");
            firstDestinationThisStep = nextDestination;
        }
    }

    /**
     * <p>
     * For each destination slot, map to the (first) source row key and copy source values to destination slots for all
     * result columns.
     *
     * <p>
     * This implementation proceeds chunk-wise in the following manner:
     * <ol>
     * <li>Get a chunk of destination slots</l1>
     * <li>Get a chunk of source indices</li>
     * <lI>For each input column: get a chunk of input values and then fill the output column</li>
     * </ol>
     *
     * @param destinations The added destination slots as an {@link RowSequence}
     */
    private void copyStreamToResult(@NotNull final RowSequence destinations) {
        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final RowSequence.Iterator destinationsIterator = toClose.add(destinations.getRowSequenceIterator());
            final ShiftedRowSequence shiftedSliceDestinations = toClose.add(new ShiftedRowSequence());
            final ChunkSource.GetContext redirectionsContext =
                    toClose.add(redirections.makeGetContext(COPY_CHUNK_SIZE));
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts =
                    toClose.addArray(new ChunkSource.GetContext[numResultColumns]);
            final ChunkSink.FillFromContext[] outputContexts =
                    toClose.addArray(new ChunkSink.FillFromContext[numResultColumns]);

            for (int ci = 0; ci < numResultColumns; ++ci) {
                inputContexts[ci] = inputColumns[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
                final WritableColumnSource<?> outputColumn = outputColumns[ci];
                outputContexts[ci] = outputColumn.makeFillFromContext(COPY_CHUNK_SIZE);
                outputColumn.ensureCapacity(destinations.lastRowKey() + 1, false);
            }

            while (destinationsIterator.hasMore()) {
                final RowSequence sliceDestinations =
                        destinationsIterator.getNextRowSequenceWithLength(COPY_CHUNK_SIZE);
                shiftedSliceDestinations.reset(sliceDestinations, -firstDestinationThisStep);
                final LongChunk<OrderedRowKeys> sourceIndices = Chunk.<Values, OrderedRowKeys>downcast(
                        redirections.getChunk(redirectionsContext, shiftedSliceDestinations)).asLongChunk();

                try (final RowSequence sliceSources = RowSequenceFactory.wrapRowKeysChunkAsRowSequence(sourceIndices)) {
                    for (int ci = 0; ci < numResultColumns; ++ci) {
                        final Chunk<? extends Values> inputChunk =
                                inputColumns[ci].getChunk(inputContexts[ci], sliceSources);
                        outputColumns[ci].fillFromChunk(outputContexts[ci], inputChunk, sliceDestinations);
                    }
                    inputSharedContext.reset();
                }
            }
        }
    }

    private static class StreamFirstBucketedContext implements BucketedContext {
        final WritableIntChunk<RowKeys> destinationsToInsert;
        final WritableLongChunk<RowKeys> rowKeyToInsert;

        public StreamFirstBucketedContext(int size) {
            destinationsToInsert = WritableIntChunk.makeWritableChunk(size);
            rowKeyToInsert = WritableLongChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            destinationsToInsert.close();
            rowKeyToInsert.close();
        }
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new StreamFirstBucketedContext(size);
    }
}

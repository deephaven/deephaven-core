package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sources.WritableChunkSink;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.db.v2.utils.ShiftedOrderedKeys;
import io.deephaven.util.SafeCloseableList;
import org.jetbrains.annotations.NotNull;

/**
 * A firstBy aggregation operator for stream tables.
 *
 * @see Table#STREAM_TABLE_ATTRIBUTE
 */
public class StreamFirstChunkedOperator extends BaseStreamFirstOrLastChunkedOperator {

    /**
     * <p>The next destination slot that we expect to be used.
     * <p>Any destination less than this one can safely be ignored while processing adds since the first row can never
     * change once a destination has been created given that we ignore removes.
     */
    private long nextDestination;

    /**
     * <p>The first destination that we used on the current step (if we used any). At the very beginning of a step, this
     * is equivalent to {@link #nextDestination} and also the result table's size.
     * <p>We use this as an offset shift for {@code redirections}, so that {@code redirections} only needs to hold first
     * source keys for newly-added destinations, rather than the entire space.
     * <p>At the end of a step, this is updated to prepare for the next step.
     */
    private long firstDestinationThisStep;

    StreamFirstChunkedOperator(@NotNull final MatchPair[] resultPairs, @NotNull final Table streamTable) {
        super(resultPairs, streamTable);
    }

    @Override
    public final boolean unchunkedIndex() {
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
    public void addChunk(final BucketedContext context, // Unused
                         final Chunk<? extends Values> values, // Unused
                         @NotNull final LongChunk<? extends KeyIndices> inputIndices,
                         @NotNull final IntChunk<KeyIndices> destinations,
                         @NotNull final IntChunk<ChunkPositions> startPositions,
                         final IntChunk<ChunkLengths> length, // Unused
                         @NotNull final WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            if (maybeAssignFirst(destination, inputIndices.get(startPosition))) {
                stateModified.set(ii, true);
            }
        }
    }

    @Override
    public boolean addChunk(final SingletonContext context, // Unused
                            final int chunkSize,
                            final Chunk<? extends Values> values, // Unused
                            @NotNull final LongChunk<? extends KeyIndices> inputIndices,
                            final long destination) {
        if (chunkSize == 0) {
            return false;
        }
        return maybeAssignFirst(destination, inputIndices.get(0));
    }

    @Override
    public boolean addIndex(final SingletonContext context,
                            @NotNull final Index index,
                            final long destination) {
        if (index.isEmpty()) {
            return false;
        }
        return maybeAssignFirst(destination, index.firstKey());
    }

    private boolean maybeAssignFirst(final long destination, final long sourceIndexKey) {
        if (destination < nextDestination) {
            // Skip anything that's not new, it cannot change the first key
            return false;
        }
        if (destination == nextDestination) {
            redirections.set(nextDestination++ - firstDestinationThisStep, sourceIndexKey);
        } else {
            //noinspection ThrowableNotThrown
            Assert.statementNeverExecuted("Destination " + destination + " greater than next destination " + nextDestination);
        }
        return true;
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable) {
        copyStreamToResult(resultTable.getIndex());
        redirections = null;
        Assert.eq(resultTable.size(), "resultTable.size()", nextDestination, "nextDestination");
        firstDestinationThisStep = nextDestination;
    }

    @Override
    public void propagateUpdates(@NotNull final ShiftAwareListener.Update downstream,
                                 @NotNull final ReadOnlyIndex newDestinations) {
        // NB: We cannot assert no modifies; other operators in the same aggregation might modify columns not in our
        //     result set.
        Assert.assertion(downstream.removed.empty() && downstream.shifted.empty(),
                "downstream.removed.empty() && downstream.shifted.empty()");
        copyStreamToResult(downstream.added);
        redirections = null;
        if (downstream.added.nonempty()) {
            Assert.eq(downstream.added.lastKey() + 1, "downstream.added.lastKey() + 1", nextDestination, "nextDestination");
            firstDestinationThisStep = nextDestination;
        }
    }

    /**
     * <p>For each destination slot, map to the (first) source index key and copy source values to destination slots for
     * all result columns.
     *
     * <p>This implementation proceeds chunk-wise in the following manner:
     * <ol>
     *     <li>Get a chunk of destination slots</l1>
     *     <li>Get a chunk of source indices</li>
     *     <lI>For each input column: get a chunk of input values and then fill the output column</li>
     * </ol>
     *
     * @param destinations The added destination slots as an {@link OrderedKeys}
     */
    private void copyStreamToResult(@NotNull final OrderedKeys destinations) {
        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final OrderedKeys.Iterator destinationsIterator = toClose.add(destinations.getOrderedKeysIterator());
            final ShiftedOrderedKeys shiftedSliceDestinations = toClose.add(new ShiftedOrderedKeys());
            final ChunkSource.GetContext redirectionsContext = toClose.add(redirections.makeGetContext(COPY_CHUNK_SIZE));
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts = toClose.addArray(new ChunkSource.GetContext[numResultColumns]);
            final WritableChunkSink.FillFromContext[] outputContexts = toClose.addArray(new WritableChunkSink.FillFromContext[numResultColumns]);

            for (int ci = 0; ci < numResultColumns; ++ci) {
                inputContexts[ci] = inputColumns[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
                final WritableSource<?> outputColumn = outputColumns[ci];
                outputContexts[ci] = outputColumn.makeFillFromContext(COPY_CHUNK_SIZE);
                outputColumn.ensureCapacity(destinations.lastKey() + 1, false);
            }

            while (destinationsIterator.hasMore()) {
                final OrderedKeys sliceDestinations = destinationsIterator.getNextOrderedKeysWithLength(COPY_CHUNK_SIZE);
                shiftedSliceDestinations.reset(sliceDestinations, -firstDestinationThisStep);
                final LongChunk<OrderedKeyIndices> sourceIndices = Chunk.<Values, OrderedKeyIndices>downcast(redirections.getChunk(redirectionsContext, shiftedSliceDestinations)).asLongChunk();

                try (final OrderedKeys sliceSources = OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(sourceIndices)) {
                    for (int ci = 0; ci < numResultColumns; ++ci) {
                        final Chunk<? extends Values> inputChunk = inputColumns[ci].getChunk(inputContexts[ci], sliceSources);
                        outputColumns[ci].fillFromChunk(outputContexts[ci], inputChunk, sliceDestinations);
                    }
                    inputSharedContext.reset();
                }
            }
        }
    }
}

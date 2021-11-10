package io.deephaven.engine.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rftable.ChunkSource;
import io.deephaven.engine.rftable.SharedContext;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.structures.rowsequence.RowSequenceUtil;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.select.MatchPair;
import io.deephaven.engine.v2.QueryTable;
import io.deephaven.engine.v2.Listener;
import io.deephaven.engine.v2.sources.WritableChunkSink;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.chunk.Attributes.*;
import io.deephaven.engine.chunk.*;
import io.deephaven.engine.v2.utils.*;
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
            @NotNull final LongChunk<? extends RowKeys> inputIndices,
            @NotNull final IntChunk<RowKeys> destinations,
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
            @NotNull final LongChunk<? extends RowKeys> inputIndices,
            final long destination) {
        if (chunkSize == 0) {
            return false;
        }
        return maybeAssignFirst(destination, inputIndices.get(0));
    }

    @Override
    public boolean addIndex(final SingletonContext context,
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
    public void propagateUpdates(@NotNull final Listener.Update downstream,
            @NotNull final RowSet newDestinations) {
        // NB: We cannot assert no modifies; other operators in the same aggregation might modify columns not in our
        // result set.
        Assert.assertion(downstream.removed.isEmpty() && downstream.shifted.empty(),
                "downstream.removed.empty() && downstream.shifted.empty()");
        copyStreamToResult(downstream.added);
        redirections = null;
        if (downstream.added.isNonempty()) {
            Assert.eq(downstream.added.lastRowKey() + 1, "downstream.added.lastRowKey() + 1", nextDestination,
                    "nextDestination");
            firstDestinationThisStep = nextDestination;
        }
    }

    /**
     * <p>
     * For each destination slot, map to the (first) source rowSet key and copy source values to destination slots for
     * all result columns.
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
            final WritableChunkSink.FillFromContext[] outputContexts =
                    toClose.addArray(new WritableChunkSink.FillFromContext[numResultColumns]);

            for (int ci = 0; ci < numResultColumns; ++ci) {
                inputContexts[ci] = inputColumns[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
                final WritableSource<?> outputColumn = outputColumns[ci];
                outputContexts[ci] = outputColumn.makeFillFromContext(COPY_CHUNK_SIZE);
                outputColumn.ensureCapacity(destinations.lastRowKey() + 1, false);
            }

            while (destinationsIterator.hasMore()) {
                final RowSequence sliceDestinations =
                        destinationsIterator.getNextRowSequenceWithLength(COPY_CHUNK_SIZE);
                shiftedSliceDestinations.reset(sliceDestinations, -firstDestinationThisStep);
                final LongChunk<OrderedRowKeys> sourceIndices = Chunk.<Values, OrderedRowKeys>downcast(
                        redirections.getChunk(redirectionsContext, shiftedSliceDestinations)).asLongChunk();

                try (final RowSequence sliceSources = RowSequenceUtil.wrapRowKeysChunkAsRowSequence(sourceIndices)) {
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
}

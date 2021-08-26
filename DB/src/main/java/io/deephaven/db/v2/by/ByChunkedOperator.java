package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.util.liveness.LivenessReferent;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.sources.aggregate.AggregateColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static io.deephaven.db.v2.sources.ArrayBackedColumnSource.BLOCK_SIZE;

/**
 * An {@link IterativeChunkedAggregationOperator} used in the implementation of {@link io.deephaven.db.tables.Table#by}.
 */
public final class ByChunkedOperator implements IterativeChunkedAggregationOperator {

    private final QueryTable inputTable;
    private final boolean registeredWithHelper;
    private final boolean live;
    private final ObjectArraySource<Index> indices;
    private final String[] inputColumnNames;
    private final Map<String, AggregateColumnSource<?, ?>> resultColumns;
    private final ModifiedColumnSet resultInputsModifiedColumnSet;

    private boolean stepValuesModified;
    private boolean someKeyHasAddsOrRemoves;
    private boolean someKeyHasModifies;

    ByChunkedOperator(@NotNull final QueryTable inputTable, final boolean registeredWithHelper,
            @NotNull final MatchPair... resultColumnPairs) {
        this.inputTable = inputTable;
        this.registeredWithHelper = registeredWithHelper;
        live = inputTable.isRefreshing();
        indices = new ObjectArraySource<>(Index.class);
        // noinspection unchecked
        resultColumns = Arrays.stream(resultColumnPairs).collect(Collectors.toMap(MatchPair::left,
                matchPair -> (AggregateColumnSource<?, ?>) AggregateColumnSource
                        .make(inputTable.getColumnSource(matchPair.right()), indices),
                Assert::neverInvoked, LinkedHashMap::new));
        inputColumnNames = MatchPair.getRightColumns(resultColumnPairs);
        if (live) {
            resultInputsModifiedColumnSet = inputTable.newModifiedColumnSet(inputColumnNames);
        } else {
            resultInputsModifiedColumnSet = null;
        }
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends KeyIndices> inputIndices,
            @NotNull final IntChunk<KeyIndices> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(values, "values");
        someKeyHasAddsOrRemoves |= startPositions.size() > 0;
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> inputIndicesAsOrdered = (LongChunk<OrderedKeyIndices>) inputIndices;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            addChunk(inputIndicesAsOrdered, startPosition, runLength, destination);
        }
        stateModified.fillWithValue(0, startPositions.size(), true);
    }

    @Override
    public void removeChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends KeyIndices> inputIndices,
            @NotNull final IntChunk<KeyIndices> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(values, "values");
        someKeyHasAddsOrRemoves |= startPositions.size() > 0;
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> inputIndicesAsOrdered = (LongChunk<OrderedKeyIndices>) inputIndices;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            removeChunk(inputIndicesAsOrdered, startPosition, runLength, destination);
        }
        stateModified.fillWithValue(0, startPositions.size(), true);
    }

    @Override
    public void modifyChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends KeyIndices> postShiftIndices,
            @NotNull final IntChunk<KeyIndices> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        // We have no inputs, so we should never get here.
        throw new IllegalStateException();
    }

    @Override
    public void shiftChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends KeyIndices> preShiftIndices,
            @NotNull final LongChunk<? extends KeyIndices> postShiftIndices,
            @NotNull final IntChunk<KeyIndices> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(previousValues, "previousValues");
        Assert.eqNull(newValues, "newValues");
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> preShiftIndicesAsOrdered = (LongChunk<OrderedKeyIndices>) preShiftIndices;
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> postShiftIndicesAsOrdered = (LongChunk<OrderedKeyIndices>) postShiftIndices;

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            doShift(preShiftIndicesAsOrdered, postShiftIndicesAsOrdered, startPosition, runLength, destination);
        }
    }

    @Override
    public void modifyIndices(final BucketedContext context,
            @NotNull final LongChunk<? extends KeyIndices> inputIndices,
            @NotNull final IntChunk<KeyIndices> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (!stepValuesModified) {
            return;
        }
        someKeyHasModifies |= startPositions.size() > 0;
        stateModified.fillWithValue(0, startPositions.size(), true);
    }

    @Override
    public boolean addChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends KeyIndices> inputIndices, final long destination) {
        Assert.eqNull(values, "values");
        someKeyHasAddsOrRemoves |= chunkSize > 0;
        // noinspection unchecked
        addChunk((LongChunk<OrderedKeyIndices>) inputIndices, 0, chunkSize, destination);
        return true;
    }

    @Override
    public boolean addIndex(SingletonContext context, Index index, long destination) {
        someKeyHasAddsOrRemoves |= index.nonempty();
        addIndex(index, destination);
        return true;
    }

    @Override
    public boolean removeChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends KeyIndices> inputIndices, final long destination) {
        Assert.eqNull(values, "values");
        someKeyHasAddsOrRemoves |= chunkSize > 0;
        // noinspection unchecked
        removeChunk((LongChunk<OrderedKeyIndices>) inputIndices, 0, chunkSize, destination);
        return true;
    }

    @Override
    public boolean modifyChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> previousValues, final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends KeyIndices> postShiftIndices,
            final long destination) {
        // We have no inputs, so we should never get here.
        throw new IllegalStateException();
    }

    @Override
    public boolean shiftChunk(final SingletonContext singletonContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends KeyIndices> preInputIndices,
            @NotNull final LongChunk<? extends KeyIndices> postInputIndices,
            final long destination) {
        Assert.eqNull(previousValues, "previousValues");
        Assert.eqNull(newValues, "newValues");
        // noinspection unchecked
        doShift((LongChunk<OrderedKeyIndices>) preInputIndices, (LongChunk<OrderedKeyIndices>) postInputIndices, 0,
                preInputIndices.size(), destination);
        return false;
    }

    @Override
    public boolean modifyIndices(final SingletonContext context, @NotNull final LongChunk<? extends KeyIndices> indices,
            final long destination) {
        if (!stepValuesModified) {
            return false;
        }
        someKeyHasModifies |= indices.size() > 0;
        return indices.size() != 0;
    }

    private void addChunk(@NotNull final LongChunk<OrderedKeyIndices> indices, final int start, final int length,
            final long destination) {
        final Index index = indexForSlot(destination);
        index.insert(indices, start, length);
    }

    private void addIndex(@NotNull final Index addIndex, final long destination) {
        indexForSlot(destination).insert(addIndex);
    }

    private void removeChunk(@NotNull final LongChunk<OrderedKeyIndices> indices, final int start, final int length,
            final long destination) {
        final Index index = indexForSlot(destination);
        index.remove(indices, start, length);
    }

    private void doShift(@NotNull final LongChunk<OrderedKeyIndices> preShiftIndices,
            @NotNull final LongChunk<OrderedKeyIndices> postShiftIndices,
            final int startPosition, final int runLength, final long destination) {
        final Index index = indexForSlot(destination);
        index.remove(preShiftIndices, startPosition, runLength);
        index.insert(postShiftIndices, startPosition, runLength);
    }

    private Index indexForSlot(final long destination) {
        Index index = indices.getUnsafe(destination);
        if (index == null) {
            indices.set(destination, index = (live ? Index.FACTORY : Index.CURRENT_FACTORY).getEmptyIndex());
        }
        return index;
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        indices.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return resultColumns;
    }

    @Override
    public void startTrackingPrevValues() {
        // NB: We don't need previous tracking on the indices ColumnSource. It's in destination space, and we never move
        // anything. Nothing should be asking for previous values if they didn't exist previously.
        // indices.startTrackingPrevValues();
        // NB: These are usually (always, as of now) instances of AggregateColumnSource, meaning
        // startTrackingPrevValues() is a no-op.
        resultColumns.values().forEach(ColumnSource::startTrackingPrevValues);
    }

    @Override
    public UnaryOperator<ModifiedColumnSet> initializeRefreshing(@NotNull final QueryTable resultTable,
            @NotNull final LivenessReferent aggregationUpdateListener) {
        initializeNewIndexPreviousValues(resultTable.getIndex());
        return registeredWithHelper
                ? new InputToResultModifiedColumnSetFactory(resultTable,
                        resultColumns.keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY))
                : null;
    }

    /**
     * Make a factory that reads an upstream {@link ModifiedColumnSet} and produces a result {@link ModifiedColumnSet}.
     *
     * @param resultTable The result {@link QueryTable}
     * @param resultColumnNames The result column names, which must be parallel to this operator's input column names
     * @return The factory
     */
    UnaryOperator<ModifiedColumnSet> makeInputToResultModifiedColumnSetFactory(@NotNull final QueryTable resultTable,
            @NotNull final String[] resultColumnNames) {
        return new InputToResultModifiedColumnSetFactory(resultTable, resultColumnNames);
    }

    private class InputToResultModifiedColumnSetFactory implements UnaryOperator<ModifiedColumnSet> {

        private final ModifiedColumnSet updateModifiedColumnSet;
        private final ModifiedColumnSet allAggregatedColumns;
        private final ModifiedColumnSet.Transformer aggregatedColumnsTransformer;

        private InputToResultModifiedColumnSetFactory(@NotNull final QueryTable resultTable,
                @NotNull final String[] resultColumnNames) {
            updateModifiedColumnSet = new ModifiedColumnSet(resultTable.getModifiedColumnSetForUpdates());
            allAggregatedColumns = resultTable.newModifiedColumnSet(resultColumnNames);
            aggregatedColumnsTransformer = inputTable.newModifiedColumnSetTransformer(
                    inputColumnNames,
                    Arrays.stream(resultColumnNames).map(resultTable::newModifiedColumnSet)
                            .toArray(ModifiedColumnSet[]::new));
        }

        @Override
        public ModifiedColumnSet apply(@NotNull final ModifiedColumnSet upstreamModifiedColumnSet) {
            if (someKeyHasAddsOrRemoves) {
                return allAggregatedColumns;
            }
            if (someKeyHasModifies) {
                aggregatedColumnsTransformer.clearAndTransform(upstreamModifiedColumnSet, updateModifiedColumnSet);
                return updateModifiedColumnSet;
            }
            return ModifiedColumnSet.EMPTY;
        }
    }

    @Override
    public void resetForStep(@NotNull final ShiftAwareListener.Update upstream) {
        stepValuesModified = upstream.modified.nonempty() && upstream.modifiedColumnSet.nonempty()
                && upstream.modifiedColumnSet.containsAny(resultInputsModifiedColumnSet);
        someKeyHasAddsOrRemoves = false;
        someKeyHasModifies = false;
    }

    @Override
    public void propagateUpdates(@NotNull final ShiftAwareListener.Update downstream,
            @NotNull final ReadOnlyIndex newDestinations) {
        initializeNewIndexPreviousValues(newDestinations);
    }

    private void initializeNewIndexPreviousValues(@NotNull final OrderedKeys newDestinations) {
        if (newDestinations.isEmpty()) {
            return;
        }
        try (final ChunkSource.GetContext indicesGetContext = indices.makeGetContext(BLOCK_SIZE);
                final OrderedKeys.Iterator newDestinationsIterator = newDestinations.getOrderedKeysIterator()) {
            while (newDestinationsIterator.hasMore()) {
                final long nextDestination = newDestinationsIterator.peekNextKey();
                final long nextBlockEnd = (nextDestination / BLOCK_SIZE) * BLOCK_SIZE + BLOCK_SIZE - 1;
                // This OrderedKeys slice should be exactly aligned to a slice of a single data block in indices (since
                // it is an ArrayBackedColumnSource), allowing getChunk to skip a copy.
                final OrderedKeys newDestinationsSlice =
                        newDestinationsIterator.getNextOrderedKeysThrough(nextBlockEnd);
                final ObjectChunk<Index, Values> indicesChunk =
                        indices.getChunk(indicesGetContext, newDestinationsSlice).asObjectChunk();
                final int indicesChunkSize = indicesChunk.size();
                for (int ii = 0; ii < indicesChunkSize; ++ii) {
                    indicesChunk.get(ii).initializePreviousValue();
                }
            }
        }
    }

    @Override
    public boolean requiresIndices() {
        return true;
    }

    @Override
    public boolean unchunkedIndex() {
        return true;
    }
}

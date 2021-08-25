package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LongArraySource;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.sources.ReadOnlyRedirectedColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.LongColumnSourceRedirectionIndex;

import java.util.LinkedHashMap;
import java.util.Map;

public class FirstOrLastChunkedOperator implements IterativeChunkedAggregationOperator {
    private final boolean isFirst;
    private final LongArraySource redirections;
    private final ObjectArraySource<Index> indices;
    private final LongColumnSourceRedirectionIndex redirectionIndex;
    private final Map<String, ColumnSource<?>> resultColumns;
    private final boolean exposeRedirections;

    FirstOrLastChunkedOperator(boolean isFirst, MatchPair[] resultPairs, Table originalTable,
            String exposeRedirectionAs) {
        this.isFirst = isFirst;
        this.redirections = new LongArraySource();
        this.redirectionIndex = new LongColumnSourceRedirectionIndex(redirections);
        this.indices = new ObjectArraySource<>(Index.class);

        this.resultColumns = new LinkedHashMap<>(resultPairs.length);
        for (final MatchPair mp : resultPairs) {
            // noinspection unchecked
            resultColumns.put(mp.left(),
                    new ReadOnlyRedirectedColumnSource(redirectionIndex, originalTable.getColumnSource(mp.right())));
        }
        exposeRedirections = exposeRedirectionAs != null;
        if (exposeRedirectionAs != null) {
            resultColumns.put(exposeRedirectionAs, redirections);
        }
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> inputIndicesAsOrdered = (LongChunk<OrderedKeyIndices>) inputIndices;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii, addChunk(inputIndicesAsOrdered, startPosition, runLength, destination));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> inputIndicesAsOrdered = (LongChunk<OrderedKeyIndices>) inputIndices;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii, removeChunk(inputIndicesAsOrdered, startPosition, runLength, destination));
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // we have no inputs, so should never get here
        throw new IllegalStateException();
    }

    @Override
    public void shiftChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> preShiftIndices,
            LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> preShiftIndicesAsOrdered = (LongChunk<OrderedKeyIndices>) preShiftIndices;
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> postShiftIndicesAsOrdered = (LongChunk<OrderedKeyIndices>) postShiftIndices;

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long newValue =
                    doShift(preShiftIndicesAsOrdered, postShiftIndicesAsOrdered, startPosition, runLength, destination);
            if (exposeRedirections) {
                final long oldValue = redirections.getAndSetUnsafe(destination, newValue);
                if (oldValue != newValue) {
                    stateModified.set(ii, true);
                }
            } else {
                redirections.set(destination, newValue);
            }
        }
    }

    private long doShift(LongChunk<OrderedKeyIndices> preShiftIndices, LongChunk<OrderedKeyIndices> postShiftIndices,
            int startPosition, int runLength, long destination) {
        final Index index = indexForSlot(destination);
        index.remove(preShiftIndices, startPosition, runLength);
        index.insert(postShiftIndices, startPosition, runLength);
        return isFirst ? index.firstKey() : index.lastKey();
    }

    @Override
    public void modifyIndices(BucketedContext context, LongChunk<? extends KeyIndices> inputIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long redirectedRow = redirections.getUnsafe(destination);
            stateModified.set(ii,
                    hasRedirection(inputIndices, redirectedRow, startPosition, startPosition + runLength));
        }
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        // noinspection unchecked
        return addChunk((LongChunk<OrderedKeyIndices>) inputIndices, 0, inputIndices.size(), destination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        // noinspection unchecked
        return removeChunk((LongChunk<OrderedKeyIndices>) inputIndices, 0, inputIndices.size(), destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        // we have no inputs, so should never get here
        throw new IllegalStateException();
    }

    @Override
    public boolean shiftChunk(SingletonContext singletonContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> preInputIndices,
            LongChunk<? extends KeyIndices> postInputIndices, long destination) {
        // noinspection unchecked
        final long newValue = doShift((LongChunk<OrderedKeyIndices>) preInputIndices,
                (LongChunk<OrderedKeyIndices>) postInputIndices, 0, preInputIndices.size(), destination);
        if (exposeRedirections) {
            final long oldValue = redirections.getAndSetUnsafe(destination, newValue);
            return oldValue != newValue;
        } else {
            redirections.set(destination, newValue);
            return false;
        }
    }

    @Override
    public boolean modifyIndices(SingletonContext context, LongChunk<? extends KeyIndices> indices, long destination) {
        if (indices.size() == 0) {
            return false;
        }
        final long redirectedRow = redirections.getUnsafe(destination);
        // if indices contains redirectedRow, the we are modified, otherwise not
        return hasRedirection(indices, redirectedRow, 0, indices.size());
    }

    private boolean hasRedirection(LongChunk<? extends KeyIndices> indices, long redirectedRow, int startInclusive,
            int endExclusive) {
        if (isFirst) {
            return indices.get(startInclusive) == redirectedRow;
        } else {
            return indices.get(endExclusive - 1) == redirectedRow;
        }
    }

    private boolean addChunk(LongChunk<OrderedKeyIndices> indices, int start, int length, long destination) {
        final Index index = indexForSlot(destination);
        index.insert(indices, start, length);

        return updateRedirections(destination, index);
    }

    @Override
    public boolean addIndex(SingletonContext context, Index addIndex, long destination) {
        if (addIndex.empty()) {
            return false;
        }

        final Index index = indexForSlot(destination);
        index.insert(addIndex);

        return updateRedirections(destination, index);
    }

    private Index indexForSlot(long destination) {
        Index index = indices.getUnsafe(destination);
        if (index == null) {
            indices.set(destination, index = Index.CURRENT_FACTORY.getEmptyIndex());
        }
        return index;
    }

    private boolean removeChunk(LongChunk<OrderedKeyIndices> indices, int start, int length, long destination) {
        final Index index = indexForSlot(destination);
        index.remove(indices, start, length);

        return updateRedirections(destination, index);
    }

    private boolean updateRedirections(long destination, Index index) {
        final long newValue = isFirst ? index.firstKey() : index.lastKey();
        final long oldValue = redirections.getAndSetUnsafe(destination, newValue);
        return oldValue != newValue;
    }

    @Override
    public boolean unchunkedIndex() {
        return true;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        indices.ensureCapacity(tableSize);
        redirections.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return resultColumns;
    }

    @Override
    public void startTrackingPrevValues() {
        redirectionIndex.startTrackingPrevValues();
    }

    @Override
    public boolean requiresIndices() {
        return true;
    }

    IterativeChunkedAggregationOperator makeSecondaryOperator(boolean isFirst, MatchPair[] comboMatchPairs, Table table,
            String exposeRedirectionAs) {
        if (this.isFirst == isFirst) {
            // we only need more output columns, the redirectionIndex and redirections column are totally fine
            return new DuplicateOperator(comboMatchPairs, table, exposeRedirectionAs);
        } else {
            return new ComplementaryOperator(isFirst, comboMatchPairs, table, exposeRedirectionAs);
        }
    }

    private class DuplicateOperator implements IterativeChunkedAggregationOperator {
        private final Map<String, ColumnSource<?>> resultColumns = new LinkedHashMap<>();

        private DuplicateOperator(MatchPair[] resultPairs, Table table, String exposeRedirectionAs) {
            for (final MatchPair mp : resultPairs) {
                // noinspection unchecked
                resultColumns.put(mp.left(),
                        new ReadOnlyRedirectedColumnSource(redirectionIndex, table.getColumnSource(mp.right())));
            }
            if (exposeRedirectionAs != null) {
                resultColumns.put(exposeRedirectionAs, redirections);
            }
        }

        @Override
        public void addChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            checkForChangedRedirections(startPositions, destinations, stateModified);
        }

        private void checkForChangedRedirections(IntChunk<ChunkPositions> startPositions,
                IntChunk<KeyIndices> destinations, WritableBooleanChunk<Values> stateModified) {
            for (int ii = 0; ii < startPositions.size(); ++ii) {
                final int startPosition = startPositions.get(ii);
                final long destination = destinations.get(startPosition);
                final long redirectionPrev = redirections.getPrevLong(destination);
                final long redirection = redirections.getUnsafe(destination);
                if (redirectionPrev != redirection) {
                    stateModified.set(ii, true);
                }
            }
        }

        private void checkForMatchingRedirections(IntChunk<ChunkPositions> startPositions,
                IntChunk<ChunkLengths> lengths, LongChunk<? extends KeyIndices> postKeyIndices,
                IntChunk<KeyIndices> destinations, WritableBooleanChunk<Values> stateModified) {
            for (int ii = 0; ii < startPositions.size(); ++ii) {
                final int startPosition = startPositions.get(ii);
                final long destination = destinations.get(startPosition);
                final long redirection = redirections.getUnsafe(destination);
                final long chunkKey = isFirst ? postKeyIndices.get(startPosition)
                        : postKeyIndices.get(startPosition + lengths.get(ii) - 1);
                if (chunkKey == redirection) {
                    stateModified.set(ii, true);
                }
            }
        }

        @Override
        public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            checkForChangedRedirections(startPositions, destinations, stateModified);
        }

        @Override
        public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices,
                IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions,
                IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
            checkForMatchingRedirections(startPositions, length, postShiftIndices, destinations, stateModified);
        }

        @Override
        public void shiftChunk(BucketedContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> preShiftIndices,
                LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            checkForChangedRedirections(startPositions, destinations, stateModified);
        }

        @Override
        public void modifyIndices(BucketedContext context, LongChunk<? extends KeyIndices> inputIndices,
                IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions,
                IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
            checkForMatchingRedirections(startPositions, length, inputIndices, destinations, stateModified);
        }

        @Override
        public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, long destination) {
            return redirectionModified(destination);
        }

        @Override
        public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, long destination) {
            return redirectionModified(destination);
        }

        @Override
        public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
            return checkSingletonModification(postShiftIndices, destination);
        }

        @Override
        public boolean shiftChunk(SingletonContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> preShiftIndices,
                LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
            if (exposeRedirections) {
                return checkSingletonModification(postShiftIndices, destination);
            } else {
                return false;
            }
        }

        private boolean redirectionModified(long destination) {
            return redirections.getUnsafe(destination) != redirections.getPrevLong(destination);
        }

        private boolean checkSingletonModification(LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
            return redirections.getUnsafe(destination) == (isFirst ? postShiftIndices.get(0)
                    : postShiftIndices.get(postShiftIndices.size() - 1));
        }

        @Override
        public boolean modifyIndices(SingletonContext context, LongChunk<? extends KeyIndices> indices,
                long destination) {
            return redirections.getUnsafe(destination) == (isFirst ? indices.get(0) : indices.get(indices.size() - 1));
        }

        @Override
        public boolean addIndex(SingletonContext context, Index index, long destination) {
            return redirections.getUnsafe(destination) == (isFirst ? index.firstKey() : index.lastKey());
        }

        @Override
        public Map<String, ? extends ColumnSource<?>> getResultColumns() {
            return resultColumns;
        }

        @Override
        public boolean requiresIndices() {
            return true;
        }

        @Override
        public boolean unchunkedIndex() {
            return true;
        }

        @Override
        public void startTrackingPrevValues() {
            // nothing to do, we've already started tracking
        }

        @Override
        public void ensureCapacity(long tableSize) {
            // nothing to do, our enclosing class has ensured our capacity
        }
    }

    private class ComplementaryOperator implements IterativeChunkedAggregationOperator {
        private final boolean isFirst;
        private final LongArraySource redirections;
        private final LongColumnSourceRedirectionIndex redirectionIndex;
        private final Map<String, ColumnSource<?>> resultColumns;
        private final boolean exposeRedirections;

        private ComplementaryOperator(boolean isFirst, MatchPair[] resultPairs, Table table,
                String exposeRedirectionAs) {
            this.isFirst = isFirst;
            redirections = new LongArraySource();

            this.redirectionIndex = new LongColumnSourceRedirectionIndex(redirections);

            this.resultColumns = new LinkedHashMap<>(resultPairs.length);
            for (final MatchPair mp : resultPairs) {
                // noinspection unchecked
                resultColumns.put(mp.left(),
                        new ReadOnlyRedirectedColumnSource(redirectionIndex, table.getColumnSource(mp.right())));
            }
            exposeRedirections = exposeRedirectionAs != null;
            if (exposeRedirections) {
                resultColumns.put(exposeRedirectionAs, redirections);
            }
        }

        @Override
        public void addChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            updateBucketed(startPositions, destinations, stateModified);
        }

        @Override
        public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            updateBucketed(startPositions, destinations, stateModified);
        }

        @Override
        public void shiftChunk(BucketedContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> preShiftIndices,
                LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            updateBucketed(startPositions, destinations, stateModified);
        }

        private void updateBucketed(IntChunk<ChunkPositions> startPositions, IntChunk<KeyIndices> destinations,
                WritableBooleanChunk<Values> stateModified) {
            for (int ii = 0; ii < startPositions.size(); ++ii) {
                final int startPosition = startPositions.get(ii);
                final int destination = destinations.get(startPosition);
                final Index trackingIndex = indices.getUnsafe(destination);
                final long trackingKey = isFirst ? trackingIndex.firstKey() : trackingIndex.lastKey();
                if (redirections.getUnsafe(destination) != trackingKey) {
                    redirections.set(destination, trackingKey);
                    stateModified.set(ii, true);
                }
            }
        }

        @Override
        public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices,
                IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions,
                IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
            checkForModifications(postShiftIndices, destinations, startPositions, length, stateModified);
        }

        @Override
        public void modifyIndices(BucketedContext context, LongChunk<? extends KeyIndices> inputIndices,
                IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions,
                IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
            checkForModifications(inputIndices, destinations, startPositions, length, stateModified);
        }

        private void checkForModifications(LongChunk<? extends KeyIndices> inputIndices,
                IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions,
                IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
            for (int ii = 0; ii < startPositions.size(); ++ii) {
                final int startPosition = startPositions.get(ii);
                final long redirection = redirections.getUnsafe(destinations.get(startPosition));
                final int modifiedChunkPosition = startPosition + (isFirst ? 0 : (length.get(ii) - 1));
                if (inputIndices.get(modifiedChunkPosition) == redirection) {
                    stateModified.set(ii, true);
                }
            }
        }

        @Override
        public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, long destination) {
            return updateSingleton(destination);
        }

        @Override
        public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends KeyIndices> inputIndices, long destination) {
            return updateSingleton(destination);
        }

        @Override
        public boolean shiftChunk(SingletonContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> preShiftIndices,
                LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
            final boolean changed = updateSingleton(destination);
            return exposeRedirections && changed;
        }

        @Override
        public boolean addIndex(SingletonContext context, Index index, long destination) {
            return updateSingleton(destination);
        }

        private boolean updateSingleton(long destination) {
            final Index trackedIndex = Require.neqNull(indices.getUnsafe(destination), "indices.get(destination)");
            final long trackedKey = isFirst ? trackedIndex.firstKey() : trackedIndex.lastKey();
            return trackedKey != redirections.getAndSetUnsafe(destination, trackedKey);
        }

        @Override
        public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
            return checkSingletonModification(postShiftIndices, redirections.getUnsafe(destination));
        }

        @Override
        public boolean modifyIndices(SingletonContext context, LongChunk<? extends KeyIndices> indices,
                long destination) {
            return checkSingletonModification(indices, redirections.getUnsafe(destination));
        }

        private boolean checkSingletonModification(LongChunk<? extends KeyIndices> postShiftIndices, long redirection) {
            if (isFirst) {
                return redirection == postShiftIndices.get(0);
            } else {
                return redirection == postShiftIndices.get(postShiftIndices.size() - 1);
            }
        }

        @Override
        public Map<String, ? extends ColumnSource<?>> getResultColumns() {
            return resultColumns;
        }

        @Override
        public boolean requiresIndices() {
            return true;
        }

        @Override
        public boolean unchunkedIndex() {
            return true;
        }

        @Override
        public void ensureCapacity(long tableSize) {
            redirections.ensureCapacity(tableSize);
        }

        @Override
        public void startTrackingPrevValues() {
            redirectionIndex.startTrackingPrevValues();
        }

        @Override
        public BucketedContext makeBucketedContext(int size) {
            return null;
        }
    }
}

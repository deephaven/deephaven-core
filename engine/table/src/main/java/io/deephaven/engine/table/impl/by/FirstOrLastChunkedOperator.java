/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import java.util.LinkedHashMap;
import java.util.Map;

public class FirstOrLastChunkedOperator
        extends BasicStateChangeRecorder
        implements IterativeChunkedAggregationOperator {

    private final boolean isFirst;
    private final LongArraySource redirections;
    private final ObjectArraySource<WritableRowSet> rowSets;
    private final LongColumnSourceWritableRowRedirection rowRedirection;
    private final Map<String, ColumnSource<?>> resultColumns;
    private final boolean exposeRedirections;

    FirstOrLastChunkedOperator(boolean isFirst, MatchPair[] resultPairs, Table originalTable,
            String exposeRedirectionAs) {
        this.isFirst = isFirst;
        this.redirections = new LongArraySource();
        this.rowRedirection = new LongColumnSourceWritableRowRedirection(redirections);
        this.rowSets = new ObjectArraySource<>(WritableRowSet.class);

        this.resultColumns = new LinkedHashMap<>(resultPairs.length);
        for (final MatchPair mp : resultPairs) {
            // noinspection unchecked
            resultColumns.put(mp.leftColumn(),
                    new RedirectedColumnSource(rowRedirection, originalTable.getColumnSource(mp.rightColumn())));
        }
        exposeRedirections = exposeRedirectionAs != null;
        if (exposeRedirectionAs != null) {
            resultColumns.put(exposeRedirectionAs, redirections);
        }
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputIndicesAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii, addChunk(inputIndicesAsOrdered, startPosition, runLength, destination));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputIndicesAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii, removeChunk(inputIndicesAsOrdered, startPosition, runLength, destination));
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // we have no inputs, so should never get here
        throw new IllegalStateException();
    }

    @Override
    public void shiftChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> preShiftRowKeys,
            LongChunk<? extends RowKeys> postShiftRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> preShiftIndicesAsOrdered = (LongChunk<OrderedRowKeys>) preShiftRowKeys;
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> postShiftIndicesAsOrdered = (LongChunk<OrderedRowKeys>) postShiftRowKeys;

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

    private long doShift(LongChunk<OrderedRowKeys> preShiftIndices, LongChunk<OrderedRowKeys> postShiftIndices,
            int startPosition, int runLength, long destination) {
        final WritableRowSet rowSet = rowSetForSlot(destination);
        rowSet.remove(preShiftIndices, startPosition, runLength);
        rowSet.insert(postShiftIndices, startPosition, runLength);
        return isFirst ? rowSet.firstRowKey() : rowSet.lastRowKey();
    }

    @Override
    public void modifyRowKeys(BucketedContext context, LongChunk<? extends RowKeys> inputRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long redirectedRow = redirections.getUnsafe(destination);
            stateModified.set(ii,
                    hasRedirection(inputRowKeys, redirectedRow, startPosition, startPosition + runLength));
        }
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        // noinspection unchecked
        return addChunk((LongChunk<OrderedRowKeys>) inputRowKeys, 0, inputRowKeys.size(), destination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        // noinspection unchecked
        return removeChunk((LongChunk<OrderedRowKeys>) inputRowKeys, 0, inputRowKeys.size(), destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        // we have no inputs, so should never get here
        throw new IllegalStateException();
    }

    @Override
    public boolean shiftChunk(SingletonContext singletonContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> preShiftRowKeys,
            LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        // noinspection unchecked
        final long newValue = doShift((LongChunk<OrderedRowKeys>) preShiftRowKeys,
                (LongChunk<OrderedRowKeys>) postShiftRowKeys, 0, preShiftRowKeys.size(), destination);
        if (exposeRedirections) {
            final long oldValue = redirections.getAndSetUnsafe(destination, newValue);
            return oldValue != newValue;
        } else {
            redirections.set(destination, newValue);
            return false;
        }
    }

    @Override
    public boolean modifyRowKeys(SingletonContext context, LongChunk<? extends RowKeys> rowKeys, long destination) {
        if (rowKeys.size() == 0) {
            return false;
        }
        final long redirectedRow = redirections.getUnsafe(destination);
        // if indices contains redirectedRow, the we are modified, otherwise not
        return hasRedirection(rowKeys, redirectedRow, 0, rowKeys.size());
    }

    private boolean hasRedirection(LongChunk<? extends RowKeys> indices, long redirectedRow, int startInclusive,
            int endExclusive) {
        if (isFirst) {
            return indices.get(startInclusive) == redirectedRow;
        } else {
            return indices.get(endExclusive - 1) == redirectedRow;
        }
    }

    private boolean addChunk(LongChunk<OrderedRowKeys> indices, int start, int length, long destination) {
        final WritableRowSet rowSet = rowSetForSlot(destination);

        final boolean wasEmpty = rowSet.isEmpty();
        rowSet.insert(indices, start, length);
        if (wasEmpty && rowSet.isNonempty()) {
            onReincarnated(destination);
        }

        return updateRedirections(destination, rowSet);
    }

    @Override
    public boolean addRowSet(SingletonContext context, RowSet addRowSet, long destination) {
        if (addRowSet.isEmpty()) {
            return false;
        }

        final WritableRowSet rowSet = rowSetForSlot(destination);
        final boolean wasEmpty = rowSet.isEmpty();
        rowSet.insert(addRowSet);
        if (wasEmpty && rowSet.isNonempty()) {
            onReincarnated(destination);
        }

        return updateRedirections(destination, rowSet);
    }

    private WritableRowSet rowSetForSlot(long destination) {
        WritableRowSet rowSet = rowSets.getUnsafe(destination);
        if (rowSet == null) {
            rowSets.set(destination, rowSet = RowSetFactory.empty());
        }
        return rowSet;
    }

    private boolean removeChunk(LongChunk<OrderedRowKeys> indices, int start, int length, long destination) {
        final WritableRowSet rowSet = rowSetForSlot(destination);

        final boolean wasNonEmpty = rowSet.isNonempty();
        rowSet.remove(indices, start, length);
        if (wasNonEmpty && rowSet.isEmpty()) {
            onEmptied(destination);
        }

        return updateRedirections(destination, rowSet);
    }

    private boolean updateRedirections(long destination, RowSet rowSet) {
        final long newValue = isFirst ? rowSet.firstRowKey() : rowSet.lastRowKey();
        final long oldValue = redirections.getAndSetUnsafe(destination, newValue);
        return oldValue != newValue;
    }

    @Override
    public boolean unchunkedRowSet() {
        return true;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        rowSets.ensureCapacity(tableSize);
        redirections.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return resultColumns;
    }

    @Override
    public void startTrackingPrevValues() {
        rowRedirection.startTrackingPrevValues();
    }

    @Override
    public boolean requiresRowKeys() {
        return true;
    }

    IterativeChunkedAggregationOperator makeSecondaryOperator(boolean isFirst, MatchPair[] comboMatchPairs, Table table,
            String exposeRedirectionAs) {
        if (this.isFirst == isFirst) {
            // we only need more output columns, the rowRedirection and redirections column are totally fine
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
                resultColumns.put(mp.leftColumn(),
                        new RedirectedColumnSource(rowRedirection, table.getColumnSource(mp.rightColumn())));
            }
            if (exposeRedirectionAs != null) {
                resultColumns.put(exposeRedirectionAs, redirections);
            }
        }

        @Override
        public void addChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            checkForChangedRedirections(startPositions, destinations, stateModified);
        }

        private void checkForChangedRedirections(IntChunk<ChunkPositions> startPositions,
                IntChunk<RowKeys> destinations, WritableBooleanChunk<Values> stateModified) {
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
                IntChunk<ChunkLengths> lengths, LongChunk<? extends RowKeys> postKeyIndices,
                IntChunk<RowKeys> destinations, WritableBooleanChunk<Values> stateModified) {
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
                LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            checkForChangedRedirections(startPositions, destinations, stateModified);
        }

        @Override
        public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
                IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions,
                IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
            checkForMatchingRedirections(startPositions, length, postShiftRowKeys, destinations, stateModified);
        }

        @Override
        public void shiftChunk(BucketedContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> preShiftRowKeys,
                LongChunk<? extends RowKeys> postShiftRowKeys, IntChunk<RowKeys> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            checkForChangedRedirections(startPositions, destinations, stateModified);
        }

        @Override
        public void modifyRowKeys(BucketedContext context, LongChunk<? extends RowKeys> inputRowKeys,
                IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions,
                IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
            checkForMatchingRedirections(startPositions, length, inputRowKeys, destinations, stateModified);
        }

        @Override
        public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, long destination) {
            return redirectionModified(destination);
        }

        @Override
        public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, long destination) {
            return redirectionModified(destination);
        }

        @Override
        public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
            return checkSingletonModification(postShiftRowKeys, destination);
        }

        @Override
        public boolean shiftChunk(SingletonContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> preShiftRowKeys,
                LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
            if (exposeRedirections) {
                return checkSingletonModification(postShiftRowKeys, destination);
            } else {
                return false;
            }
        }

        private boolean redirectionModified(long destination) {
            return redirections.getUnsafe(destination) != redirections.getPrevLong(destination);
        }

        private boolean checkSingletonModification(LongChunk<? extends RowKeys> postShiftIndices, long destination) {
            return redirections.getUnsafe(destination) == (isFirst ? postShiftIndices.get(0)
                    : postShiftIndices.get(postShiftIndices.size() - 1));
        }

        @Override
        public boolean modifyRowKeys(SingletonContext context, LongChunk<? extends RowKeys> rowKeys,
                long destination) {
            return redirections.getUnsafe(destination) == (isFirst ? rowKeys.get(0) : rowKeys.get(rowKeys.size() - 1));
        }

        @Override
        public boolean addRowSet(SingletonContext context, RowSet rowSet, long destination) {
            return redirections.getUnsafe(destination) == (isFirst ? rowSet.firstRowKey() : rowSet.lastRowKey());
        }

        @Override
        public Map<String, ? extends ColumnSource<?>> getResultColumns() {
            return resultColumns;
        }

        @Override
        public boolean requiresRowKeys() {
            return true;
        }

        @Override
        public boolean unchunkedRowSet() {
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
        private final LongColumnSourceWritableRowRedirection rowRedirection;
        private final Map<String, ColumnSource<?>> resultColumns;
        private final boolean exposeRedirections;

        private ComplementaryOperator(boolean isFirst, MatchPair[] resultPairs, Table table,
                String exposeRedirectionAs) {
            this.isFirst = isFirst;
            redirections = new LongArraySource();

            this.rowRedirection = new LongColumnSourceWritableRowRedirection(redirections);

            this.resultColumns = new LinkedHashMap<>(resultPairs.length);
            for (final MatchPair mp : resultPairs) {
                // noinspection unchecked
                resultColumns.put(mp.leftColumn(),
                        new RedirectedColumnSource(rowRedirection, table.getColumnSource(mp.rightColumn())));
            }
            exposeRedirections = exposeRedirectionAs != null;
            if (exposeRedirections) {
                resultColumns.put(exposeRedirectionAs, redirections);
            }
        }

        @Override
        public void addChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            updateBucketed(startPositions, destinations, stateModified);
        }

        @Override
        public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            updateBucketed(startPositions, destinations, stateModified);
        }

        @Override
        public void shiftChunk(BucketedContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> preShiftRowKeys,
                LongChunk<? extends RowKeys> postShiftRowKeys, IntChunk<RowKeys> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {
            updateBucketed(startPositions, destinations, stateModified);
        }

        private void updateBucketed(IntChunk<ChunkPositions> startPositions, IntChunk<RowKeys> destinations,
                WritableBooleanChunk<Values> stateModified) {
            for (int ii = 0; ii < startPositions.size(); ++ii) {
                final int startPosition = startPositions.get(ii);
                final int destination = destinations.get(startPosition);
                final RowSet rowSet = rowSets.getUnsafe(destination);
                final long trackingKey = isFirst ? rowSet.firstRowKey() : rowSet.lastRowKey();
                if (redirections.getUnsafe(destination) != trackingKey) {
                    redirections.set(destination, trackingKey);
                    stateModified.set(ii, true);
                }
            }
        }

        @Override
        public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
                IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions,
                IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
            checkForModifications(postShiftRowKeys, destinations, startPositions, length, stateModified);
        }

        @Override
        public void modifyRowKeys(BucketedContext context, LongChunk<? extends RowKeys> inputRowKeys,
                IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions,
                IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
            checkForModifications(inputRowKeys, destinations, startPositions, length, stateModified);
        }

        private void checkForModifications(LongChunk<? extends RowKeys> inputIndices,
                IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions,
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
                LongChunk<? extends RowKeys> inputRowKeys, long destination) {
            return updateSingleton(destination);
        }

        @Override
        public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, long destination) {
            return updateSingleton(destination);
        }

        @Override
        public boolean shiftChunk(SingletonContext context, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> preShiftRowKeys,
                LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
            final boolean changed = updateSingleton(destination);
            return exposeRedirections && changed;
        }

        @Override
        public boolean addRowSet(SingletonContext context, RowSet rowSet, long destination) {
            return updateSingleton(destination);
        }

        private boolean updateSingleton(long destination) {
            final RowSet trackedRowSet = Require.neqNull(rowSets.getUnsafe(destination), "indices.get(destination)");
            final long trackedKey = isFirst ? trackedRowSet.firstRowKey() : trackedRowSet.lastRowKey();
            return trackedKey != redirections.getAndSetUnsafe(destination, trackedKey);
        }

        @Override
        public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
                Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
            return checkSingletonModification(postShiftRowKeys, redirections.getUnsafe(destination));
        }

        @Override
        public boolean modifyRowKeys(SingletonContext context, LongChunk<? extends RowKeys> rowKeys,
                long destination) {
            return checkSingletonModification(rowKeys, redirections.getUnsafe(destination));
        }

        private boolean checkSingletonModification(LongChunk<? extends RowKeys> postShiftIndices, long redirection) {
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
        public boolean requiresRowKeys() {
            return true;
        }

        @Override
        public boolean unchunkedRowSet() {
            return true;
        }

        @Override
        public void ensureCapacity(long tableSize) {
            redirections.ensureCapacity(tableSize);
        }

        @Override
        public void startTrackingPrevValues() {
            rowRedirection.startTrackingPrevValues();
        }

        @Override
        public BucketedContext makeBucketedContext(int size) {
            return null;
        }
    }
}

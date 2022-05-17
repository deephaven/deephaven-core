/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.HashCodes;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.chunk.util.hashing.*;
// this is ugly to have twice, but we do need it twice for replication
// @StateChunkIdentityName@ from \QLongChunk\E
import io.deephaven.chunk.util.hashing.LongChunkEquals;
import io.deephaven.engine.table.impl.sort.permute.PermuteKernel;
import io.deephaven.engine.table.impl.sort.timsort.LongIntTimsortKernel;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.util.*;


import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;

// region extra imports
import org.jetbrains.annotations.Nullable;
// endregion extra imports

import static io.deephaven.util.SafeCloseable.closeArray;

// region class visibility
// endregion class visibility
class RightIncrementalChunkedNaturalJoinStateManager
    // region extensions
    extends RightIncrementalNaturalJoinStateManager
    implements IncrementalNaturalJoinStateManager
    // endregion extensions
{
    // region constants
    private static final int CHUNK_SIZE = 4096;
    private static final int MINIMUM_INITIAL_HASH_SIZE = CHUNK_SIZE;
    private static final long MAX_TABLE_SIZE = 1L << 30;
    // endregion constants


    // region preamble variables
    // endregion preamble variables

    @HashTableAnnotations.EmptyStateValue
    // @NullStateValue@ from \QQueryConstants.NULL_LONG\E, @StateValueType@ from \Qlong\E
    private static final long EMPTY_RIGHT_VALUE = QueryConstants.NULL_LONG;

    // mixin getStateValue
    // region overflow pivot
    private static final long OVERFLOW_PIVOT_VALUE = DUPLICATE_RIGHT_VALUE;
    // endregion overflow pivot
    // endmixin getStateValue

    // the number of slots in our table
    private final int tableSize;

    // how many key columns we have
    private final int keyColumnCount;


    // the keys for our hash entries
    private final ArrayBackedColumnSource<?>[] keySources;
    // the location of any overflow entry in this bucket
    private final IntegerArraySource overflowLocationSource = new IntegerArraySource();

    // we are going to also reuse this for our state entry, so that we do not need additional storage
    @HashTableAnnotations.StateColumnSource
    // @StateColumnSourceType@ from \QLongArraySource\E
    private final LongArraySource rightRowSetSource
            // @StateColumnSourceConstructor@ from \QLongArraySource()\E
            = new LongArraySource();

    // the keys for overflow
    private int nextOverflowLocation = 0;
    private final ArrayBackedColumnSource<?> [] overflowKeySources;
    // the location of the next key in an overflow bucket
    private final IntegerArraySource overflowOverflowLocationSource = new IntegerArraySource();
    // the overflow buckets for the state source
    @HashTableAnnotations.OverflowStateColumnSource
    // @StateColumnSourceType@ from \QLongArraySource\E
    private final LongArraySource overflowRightRowSetSource
            // @StateColumnSourceConstructor@ from \QLongArraySource()\E
            = new LongArraySource();

    // the type of each of our key chunks
    private final ChunkType[] keyChunkTypes;

    // the operators for hashing and various equality methods
    private final ChunkHasher[] chunkHashers;
    private final ChunkEquals[] chunkEquals;
    private final PermuteKernel[] chunkCopiers;


    // region extra variables
    // we always store left RowSet values parallel to the keys; we may want to optimize for single left indices to avoid
    // object allocation, but we do have fairly efficient single range indices at this point
    private final ObjectArraySource<WritableRowSet> leftRowSetSource;
    private final ObjectArraySource<WritableRowSet> overflowLeftRowSetSource;

    // we must maintain our cookie for modified state tracking
    private final LongArraySource modifiedTrackerCookieSource;
    private final LongArraySource overflowModifiedTrackerCookieSource;
    // endregion extra variables

    RightIncrementalChunkedNaturalJoinStateManager(ColumnSource<?>[] tableKeySources
                                         , int tableSize
                                                   // region constructor arguments
                                         , ColumnSource<?>[] tableKeySourcesForErrors
                                              // endregion constructor arguments
    ) {
        // region super
        super(tableKeySourcesForErrors);
        // endregion super
        keyColumnCount = tableKeySources.length;

        this.tableSize = tableSize;
        Require.leq(tableSize, "tableSize", MAX_TABLE_SIZE);
        Require.gtZero(tableSize, "tableSize");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);

        overflowKeySources = new ArrayBackedColumnSource[keyColumnCount];
        keySources = new ArrayBackedColumnSource[keyColumnCount];

        keyChunkTypes = new ChunkType[keyColumnCount];
        chunkHashers = new ChunkHasher[keyColumnCount];
        chunkEquals = new ChunkEquals[keyColumnCount];
        chunkCopiers = new PermuteKernel[keyColumnCount];

        for (int ii = 0; ii < keyColumnCount; ++ii) {
            // the sources that we will use to store our hash table
            keySources[ii] = ArrayBackedColumnSource.getMemoryColumnSource(tableSize, tableKeySources[ii].getType());
            keyChunkTypes[ii] = tableKeySources[ii].getChunkType();

            overflowKeySources[ii] = ArrayBackedColumnSource.getMemoryColumnSource(CHUNK_SIZE, tableKeySources[ii].getType());

            chunkHashers[ii] = ChunkHasher.makeHasher(keyChunkTypes[ii]);
            chunkEquals[ii] = ChunkEquals.makeEqual(keyChunkTypes[ii]);
            chunkCopiers[ii] = PermuteKernel.makePermuteKernel(keyChunkTypes[ii]);
        }


        // region constructor
        leftRowSetSource = new ObjectArraySource<>(WritableRowSet.class);
        overflowLeftRowSetSource = new ObjectArraySource<>(WritableRowSet.class);
        modifiedTrackerCookieSource = new LongArraySource();
        overflowModifiedTrackerCookieSource = new LongArraySource();
        // endregion constructor

        ensureCapacity(tableSize);
    }

    private void ensureCapacity(int tableSize) {
        rightRowSetSource.ensureCapacity(tableSize);
        overflowLocationSource.ensureCapacity(tableSize);
        for (int ii = 0; ii < keyColumnCount; ++ii) {
            keySources[ii].ensureCapacity(tableSize);
        }
        // region ensureCapacity
        leftRowSetSource.ensureCapacity(tableSize);
        modifiedTrackerCookieSource.ensureCapacity(tableSize);
        // endregion ensureCapacity
    }

    private void ensureOverflowCapacity(WritableIntChunk<ChunkPositions> chunkPositionsToInsertInOverflow) {
        final int locationsToAllocate = chunkPositionsToInsertInOverflow.size();
        final int newCapacity = nextOverflowLocation + locationsToAllocate;
        overflowOverflowLocationSource.ensureCapacity(newCapacity);
        overflowRightRowSetSource.ensureCapacity(newCapacity);
        //noinspection ForLoopReplaceableByForEach
        for (int ii = 0; ii < overflowKeySources.length; ++ii) {
            overflowKeySources[ii].ensureCapacity(newCapacity);
        }
        // region ensureOverflowCapacity
        overflowLeftRowSetSource.ensureCapacity(newCapacity);
        overflowModifiedTrackerCookieSource.ensureCapacity(newCapacity);
        // endregion ensureOverflowCapacity
    }

    // region build wrappers
    private static class InitialBuildContext implements RightIncrementalNaturalJoinStateManager.InitialBuildContext {
        final LongArraySource leftHashSlots = new LongArraySource();

        @Override
        public void close() {
        }
    }

    @Override
    public RightIncrementalNaturalJoinStateManager.InitialBuildContext makeInitialBuildContext(Table leftTable) {
        return new InitialBuildContext();
    }

    @Override
    public void buildFromLeftSide(final Table leftTable, ColumnSource<?>[] leftSources, final RightIncrementalNaturalJoinStateManager.InitialBuildContext initialBuildContext) {
        if (leftTable.isEmpty()) {
            return;
        }
        final InitialBuildContext ibc = (InitialBuildContext)initialBuildContext;
        ibc.leftHashSlots.ensureCapacity(leftTable.size());
        try (final BuildContext bc = makeBuildContext(leftSources, leftTable.size())) {
            buildTable(bc, leftTable.getRowSet(), leftSources, ibc.leftHashSlots);
        }
    }

    private void addLeftIndex(long tableLocation, long keyToAdd) {
        final WritableRowSet rowSet = leftRowSetSource.get(tableLocation);
        if (rowSet == null) {
            leftRowSetSource.set(tableLocation, RowSetFactory.fromKeys(keyToAdd));
        } else {
            rowSet.insert(keyToAdd);
        }
    }


    private void addRightIndex(long tableLocation, long keyToAdd) {
        final long existingRightIndex = rightRowSetSource.getLong(tableLocation);
        if (existingRightIndex == NO_RIGHT_ENTRY_VALUE) {
            rightRowSetSource.set(tableLocation, keyToAdd);
        } else {
            rightRowSetSource.set(tableLocation, DUPLICATE_RIGHT_VALUE);
        }
    }

    private void removeRightIndex(long tableLocation, long keyToRemove) {
        final long existingRightIndex = rightRowSetSource.getLong(tableLocation);
        if (existingRightIndex == keyToRemove) {
            rightRowSetSource.set(tableLocation, NO_RIGHT_ENTRY_VALUE);
        } else {
            throw Assert.statementNeverExecuted("Existing Right TrackingWritableRowSet: " + existingRightIndex + " remove of " + keyToRemove + ", key=" + keyString(tableLocation));
        }
    }

    private void shiftRightIndex(long tableLocation, long shiftedKey, long shiftDelta) {
        final long existingRightIndex = rightRowSetSource.getLong(tableLocation);
        if (existingRightIndex == shiftedKey - shiftDelta) {
            rightRowSetSource.set(tableLocation, shiftedKey);
        } else {
            throw Assert.statementNeverExecuted("Existing Right TrackingWritableRowSet: " + existingRightIndex + " shift of " + (shiftedKey - shiftDelta) + ", key=" + keyString(tableLocation));
        }
    }

    private void removeRightIndexOverflow(long overflowLocation, long keyToRemove) {
        final long existingRightIndex = overflowRightRowSetSource.getLong(overflowLocation);
        if (existingRightIndex == keyToRemove) {
            overflowRightRowSetSource.set(overflowLocation, NO_RIGHT_ENTRY_VALUE);
        } else {
            throw Assert.statementNeverExecuted("Existing Right TrackingWritableRowSet: " + existingRightIndex + " remove of " + keyToRemove + ", key=" + keyString(overflowLocationToHashLocation(overflowLocation)));
        }
    }

    private void shiftRightIndexOverflow(long overflowLocation, long shiftedKey, long shiftDelta) {
        final long existingRightIndex = overflowRightRowSetSource.getLong(overflowLocation);
        if (existingRightIndex == shiftedKey - shiftDelta) {
            overflowRightRowSetSource.set(overflowLocation, shiftedKey);
        } else {
            throw Assert.statementNeverExecuted("Existing Right TrackingWritableRowSet: " + existingRightIndex + " shift of " + (shiftedKey - shiftDelta) + ", key=" + keyString(overflowLocationToHashLocation(overflowLocation)));
        }
    }

    private void addLeftIndexOverflow(long overflowLocation, long keyToAdd) {
        final WritableRowSet rowSet = overflowLeftRowSetSource.get(overflowLocation);
        if (rowSet == null) {
            overflowLeftRowSetSource.set(overflowLocation, RowSetFactory.fromKeys(keyToAdd));
        } else {
            rowSet.insert(keyToAdd);
        }
    }

    private void addRightIndexOverflow(long overflowLocation, long keyToAdd) {
        final long existingRightIndex = overflowRightRowSetSource.getLong(overflowLocation);
        if (existingRightIndex == NO_RIGHT_ENTRY_VALUE) {
            overflowRightRowSetSource.set(overflowLocation, keyToAdd);
        } else {
            overflowRightRowSetSource.set(overflowLocation, DUPLICATE_RIGHT_VALUE);
        }
    }

    private void addModifiedMain(NaturalJoinModifiedSlotTracker modifiedSlotTracker, long tableLocation, byte flag) {
        if (modifiedSlotTracker != null) {
            final long originalIndex = rightRowSetSource.getLong(tableLocation);
            modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getLong(tableLocation), tableLocation, originalIndex, flag));
        }
    }

    private void addModifiedOverflow(NaturalJoinModifiedSlotTracker modifiedSlotTracker, long overflowLocation, byte flag) {
        if (modifiedSlotTracker != null) {
            final long originalIndex = overflowRightRowSetSource.getLong(overflowLocation);
            overflowModifiedTrackerCookieSource.set(overflowLocation, modifiedSlotTracker.addOverflow(overflowModifiedTrackerCookieSource.getLong(overflowLocation), overflowLocation, originalIndex, flag));
        }
    }
    // endregion build wrappers

    class BuildContext implements Context {
        final int chunkSize;

        final LongIntTimsortKernel.LongIntSortKernelContext sortContext;
        final ColumnSource.FillContext stateSourceFillContext;
        final ColumnSource.FillContext overflowFillContext;
        final ColumnSource.FillContext overflowOverflowFillContext;

        // the chunk of hashcodes
        final WritableIntChunk<HashCodes> hashChunk;
        // the chunk of positions within our table
        final WritableLongChunk<RowKeys> tableLocationsChunk;

        final ResettableWritableChunk<Values>[] writeThroughChunks = getResettableWritableKeyChunks();
        final WritableIntChunk<ChunkPositions> sourcePositions;
        final WritableIntChunk<ChunkPositions> destinationLocationPositionInWriteThrough;

        final WritableBooleanChunk<Any> filledValues;
        final WritableBooleanChunk<Any> equalValues;

        // the overflow locations that we need to get from the overflowLocationSource (or overflowOverflowLocationSource)
        final WritableLongChunk<RowKeys> overflowLocationsToFetch;
        // the overflow position in the working key chunks, parallel to the overflowLocationsToFetch
        final WritableIntChunk<ChunkPositions> overflowPositionInSourceChunk;

        // the position with our hash table that we should insert a value into
        final WritableLongChunk<RowKeys> insertTableLocations;
        // the position in our chunk, parallel to the workingChunkInsertTablePositions
        final WritableIntChunk<ChunkPositions> insertPositionsInSourceChunk;

        // we sometimes need to check two positions within a single chunk for equality, this contains those positions as pairs
        final WritableIntChunk<ChunkPositions> chunkPositionsToCheckForEquality;
        // While processing overflow insertions, parallel to the chunkPositions to check for equality, the overflow location that
        // is represented by the first of the pairs in chunkPositionsToCheckForEquality
        final WritableLongChunk<RowKeys> overflowLocationForEqualityCheck;

        // the chunk of state values that we read from the hash table
        // @WritableStateChunkType@ from \QWritableLongChunk<Values>\E
        final WritableLongChunk<Values> workingStateEntries;

        // the chunks for getting key values from the hash table
        final WritableChunk<Values>[] workingKeyChunks;
        final WritableChunk<Values>[] overflowKeyChunks;

        // when fetching from the overflow, we record which chunk position we are fetching for
        final WritableIntChunk<ChunkPositions> chunkPositionsForFetches;
        // which positions in the chunk we are inserting into the overflow
        final WritableIntChunk<ChunkPositions> chunkPositionsToInsertInOverflow;
        // which table locations we are inserting into the overflow
        final WritableLongChunk<ChunkPositions> tableLocationsToInsertInOverflow;

        // values we have read from the overflow locations sources
        final WritableIntChunk<Values> overflowLocations;


        final SharedContext sharedFillContext;
        final ColumnSource.FillContext[] workingFillContexts;
        final SharedContext sharedOverflowContext;
        final ColumnSource.FillContext[] overflowContexts;
        final SharedContext sharedBuildContext;
        final ChunkSource.GetContext[] buildContexts;

        // region build context
        // endregion build context

        final boolean haveSharedContexts;

        private BuildContext(ColumnSource<?>[] buildSources,
                            int chunkSize
                            // region build context constructor args
                            // endregion build context constructor args
                            ) {
            Assert.gtZero(chunkSize, "chunkSize");
            this.chunkSize = chunkSize;
            haveSharedContexts = buildSources.length > 1;
            if (haveSharedContexts) {
                sharedFillContext = SharedContext.makeSharedContext();
                sharedOverflowContext = SharedContext.makeSharedContext();
                sharedBuildContext = SharedContext.makeSharedContext();
            } else {
                // no point in the additional work implied by these not being null.
                sharedFillContext = null;
                sharedOverflowContext = null;
                sharedBuildContext = null;
            }
            workingFillContexts = makeFillContexts(keySources, sharedFillContext, chunkSize);
            overflowContexts = makeFillContexts(overflowKeySources, sharedOverflowContext, chunkSize);
            buildContexts = makeGetContexts(buildSources, sharedBuildContext, chunkSize);
            // region build context constructor
            // endregion build context constructor
            sortContext = LongIntTimsortKernel.createContext(chunkSize);
            stateSourceFillContext = rightRowSetSource.makeFillContext(chunkSize);
            overflowFillContext = overflowLocationSource.makeFillContext(chunkSize);
            overflowOverflowFillContext = overflowOverflowLocationSource.makeFillContext(chunkSize);
            hashChunk = WritableIntChunk.makeWritableChunk(chunkSize);
            tableLocationsChunk = WritableLongChunk.makeWritableChunk(chunkSize);
            sourcePositions = WritableIntChunk.makeWritableChunk(chunkSize);
            destinationLocationPositionInWriteThrough = WritableIntChunk.makeWritableChunk(chunkSize);
            filledValues = WritableBooleanChunk.makeWritableChunk(chunkSize);
            equalValues = WritableBooleanChunk.makeWritableChunk(chunkSize);
            overflowLocationsToFetch = WritableLongChunk.makeWritableChunk(chunkSize);
            overflowPositionInSourceChunk = WritableIntChunk.makeWritableChunk(chunkSize);
            insertTableLocations = WritableLongChunk.makeWritableChunk(chunkSize);
            insertPositionsInSourceChunk = WritableIntChunk.makeWritableChunk(chunkSize);
            chunkPositionsToCheckForEquality = WritableIntChunk.makeWritableChunk(chunkSize * 2);
            overflowLocationForEqualityCheck = WritableLongChunk.makeWritableChunk(chunkSize);
            // @WritableStateChunkName@ from \QWritableLongChunk\E
            workingStateEntries = WritableLongChunk.makeWritableChunk(chunkSize);
            workingKeyChunks = getWritableKeyChunks(chunkSize);
            overflowKeyChunks = getWritableKeyChunks(chunkSize);
            chunkPositionsForFetches = WritableIntChunk.makeWritableChunk(chunkSize);
            chunkPositionsToInsertInOverflow = WritableIntChunk.makeWritableChunk(chunkSize);
            tableLocationsToInsertInOverflow = WritableLongChunk.makeWritableChunk(chunkSize);
            overflowLocations = WritableIntChunk.makeWritableChunk(chunkSize);
        }

        private void resetSharedContexts() {
            if (!haveSharedContexts) {
                return;
            }
            sharedFillContext.reset();
            sharedOverflowContext.reset();
            sharedBuildContext.reset();
        }

        private void closeSharedContexts() {
            if (!haveSharedContexts) {
                return;
            }
            sharedFillContext.close();
            sharedOverflowContext.close();
            sharedBuildContext.close();
        }

        @Override
        public void close() {
            sortContext.close();
            stateSourceFillContext.close();
            overflowFillContext.close();
            overflowOverflowFillContext.close();
            closeArray(workingFillContexts);
            closeArray(overflowContexts);
            closeArray(buildContexts);

            hashChunk.close();
            tableLocationsChunk.close();
            closeArray(writeThroughChunks);

            sourcePositions.close();
            destinationLocationPositionInWriteThrough.close();
            filledValues.close();
            equalValues.close();
            overflowLocationsToFetch.close();
            overflowPositionInSourceChunk.close();
            insertTableLocations.close();
            insertPositionsInSourceChunk.close();
            chunkPositionsToCheckForEquality.close();
            overflowLocationForEqualityCheck.close();
            workingStateEntries.close();
            closeArray(workingKeyChunks);
            closeArray(overflowKeyChunks);
            chunkPositionsForFetches.close();
            chunkPositionsToInsertInOverflow.close();
            tableLocationsToInsertInOverflow.close();
            overflowLocations.close();
            // region build context close
            // endregion build context close
            closeSharedContexts();
        }

    }

    public BuildContext makeBuildContext(ColumnSource<?>[] buildSources,
                                  long maxSize
                                  // region makeBuildContext args
                                  // endregion makeBuildContext args
    ) {
        return new BuildContext(buildSources, (int)Math.min(CHUNK_SIZE, maxSize)
                // region makeBuildContext arg pass
                // endregion makeBuildContext arg pass
        );
    }

    private void buildTable(final BuildContext bc,
                            final RowSequence buildIndex,
                            ColumnSource<?>[] buildSources
            // region extra build arguments
            , final LongArraySource resultSource
                            // endregion extra build arguments
    ) {
        long hashSlotOffset = 0;
        // region build start
        // endregion build start

        try (final RowSequence.Iterator rsIt = buildIndex.getRowSequenceIterator();
             // region build initialization try
             // endregion build initialization try
        ) {
            // region build initialization
            // the destination hash slots for each left-hand-side entry
            final WritableLongChunk<RowKeys> sourceChunkLeftHashSlots = WritableLongChunk.makeWritableChunk(bc.chunkSize);
            // the chunk of source indices that are parallel to the sourceChunks
            final WritableLongChunk<OrderedRowKeys> sourceIndexKeys = WritableLongChunk.makeWritableChunk(bc.chunkSize);
            // endregion build initialization

            // chunks to write through to the table key sources


            //noinspection unchecked
            final Chunk<Values> [] sourceKeyChunks = new Chunk[buildSources.length];

            while (rsIt.hasMore()) {
                // we reset early to avoid carrying around state for old RowSequence which can't be reused.
                bc.resetSharedContexts();

                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(bc.chunkSize);

                getKeyChunks(buildSources, bc.buildContexts, sourceKeyChunks, chunkOk);
                hashKeyChunks(bc.hashChunk, sourceKeyChunks);

                // region build loop initialization
                chunkOk.fillRowKeyChunk(sourceIndexKeys);
                sourceChunkLeftHashSlots.setSize(bc.hashChunk.size());
                // endregion build loop initialization

                // turn hash codes into indices within our table
                convertHashToTableLocations(bc.hashChunk, bc.tableLocationsChunk);

                // now fetch the values from the table, note that we do not order these fetches
                fillKeys(bc.workingFillContexts, bc.workingKeyChunks, bc.tableLocationsChunk);

                // and the corresponding states, if a value is null, we've found our insertion point
                rightRowSetSource.fillChunkUnordered(bc.stateSourceFillContext, bc.workingStateEntries, bc.tableLocationsChunk);

                // find things that exist
                // @StateChunkIdentityName@ from \QLongChunk\E
                LongChunkEquals.notEqual(bc.workingStateEntries, EMPTY_RIGHT_VALUE, bc.filledValues);

                // to be equal, the location must exist; and each of the keyChunks must match
                bc.equalValues.setSize(bc.filledValues.size());
                bc.equalValues.copyFromChunk(bc.filledValues, 0, 0, bc.filledValues.size());
                checkKeyEquality(bc.equalValues, bc.workingKeyChunks, sourceKeyChunks);

                bc.overflowPositionInSourceChunk.setSize(0);
                bc.overflowLocationsToFetch.setSize(0);
                bc.insertPositionsInSourceChunk.setSize(0);
                bc.insertTableLocations.setSize(0);

                for (int ii = 0; ii < bc.equalValues.size(); ++ii) {
                    final long tableLocation = bc.tableLocationsChunk.get(ii);
                    if (bc.equalValues.get(ii)) {
                        // region build found main
                        // we know what hash slot this maps to
                        sourceChunkLeftHashSlots.set(ii, tableLocation);
                        addLeftIndex(tableLocation, sourceIndexKeys.get(ii));
                        // endregion build found main
                    } else if (bc.filledValues.get(ii)) {
                        // we must handle this as part of the overflow bucket
                        bc.overflowPositionInSourceChunk.add(ii);
                        bc.overflowLocationsToFetch.add(tableLocation);
                    } else {
                        // for the values that are empty, we record them in the insert chunks
                        bc.insertPositionsInSourceChunk.add(ii);
                        bc.insertTableLocations.add(tableLocation);
                    }
                }

                // we first sort by position; so that we'll not insert things into the table twice or overwrite
                // collisions
                LongIntTimsortKernel.sort(bc.sortContext, bc.insertPositionsInSourceChunk, bc.insertTableLocations);

                // the first and last valid table location in our writeThroughChunks
                long firstBackingChunkLocation = -1;
                long lastBackingChunkLocation = -1;

                bc.chunkPositionsToCheckForEquality.setSize(0);
                bc.destinationLocationPositionInWriteThrough.setSize(0);
                bc.sourcePositions.setSize(0);

                for (int ii = 0; ii < bc.insertPositionsInSourceChunk.size(); ) {
                    final int firstChunkPositionForHashLocation = bc.insertPositionsInSourceChunk.get(ii);
                    final long currentHashLocation = bc.insertTableLocations.get(ii);

                    // region main insert
                    final long keyToAdd = sourceIndexKeys.get(firstChunkPositionForHashLocation);
                    rightRowSetSource.set(currentHashLocation, NO_RIGHT_ENTRY_VALUE);
                    addLeftIndex(currentHashLocation, keyToAdd);
                    sourceChunkLeftHashSlots.set(firstChunkPositionForHashLocation, currentHashLocation);
                    // endregion main insert

                    if (currentHashLocation > lastBackingChunkLocation) {
                        flushWriteThrough(bc.sourcePositions, sourceKeyChunks, bc.destinationLocationPositionInWriteThrough, bc.writeThroughChunks);
                        firstBackingChunkLocation = updateWriteThroughChunks(bc.writeThroughChunks, currentHashLocation, keySources);
                        lastBackingChunkLocation = firstBackingChunkLocation + bc.writeThroughChunks[0].size() - 1;
                    }

                    bc.sourcePositions.add(firstChunkPositionForHashLocation);
                    bc.destinationLocationPositionInWriteThrough.add((int)(currentHashLocation - firstBackingChunkLocation));

                    final int currentHashValue = bc.hashChunk.get(firstChunkPositionForHashLocation);

                    while (++ii < bc.insertTableLocations.size() && bc.insertTableLocations.get(ii) == currentHashLocation) {
                        // if this thing is equal to the first one; we should mark the appropriate slot, we don't
                        // know the types and don't want to make the virtual calls, so we need to just accumulate
                        // the things to check for equality afterwards
                        final int chunkPosition = bc.insertPositionsInSourceChunk.get(ii);
                        if (bc.hashChunk.get(chunkPosition) != currentHashValue) {
                            // we must be an overflow
                            bc.overflowPositionInSourceChunk.add(chunkPosition);
                            bc.overflowLocationsToFetch.add(currentHashLocation);
                        } else {
                            // we need to check equality, equal things are the same slot; unequal things are overflow
                            bc.chunkPositionsToCheckForEquality.add(firstChunkPositionForHashLocation);
                            bc.chunkPositionsToCheckForEquality.add(chunkPosition);
                        }
                    }
                }

                flushWriteThrough(bc.sourcePositions, sourceKeyChunks, bc.destinationLocationPositionInWriteThrough, bc.writeThroughChunks);

                checkPairEquality(bc.chunkPositionsToCheckForEquality, sourceKeyChunks, bc.equalValues);

                for (int ii = 0; ii < bc.equalValues.size(); ii++) {
                    final int chunkPosition = bc.chunkPositionsToCheckForEquality.get(ii * 2 + 1);
                    final long tableLocation = bc.tableLocationsChunk.get(chunkPosition);

                    if (bc.equalValues.get(ii)) {
                        // region build main duplicate
                        // we match the first element, so should use it
                        sourceChunkLeftHashSlots.set(chunkPosition, tableLocation);
                        addLeftIndex(tableLocation, sourceIndexKeys.get(chunkPosition));
                        // endregion build main duplicate
                    } else {
                        // we are an overflow element
                        bc.overflowPositionInSourceChunk.add(chunkPosition);
                        bc.overflowLocationsToFetch.add(tableLocation);
                    }
                }

                // now handle overflow
                if (bc.overflowPositionInSourceChunk.size() > 0) {
                    // on the first pass we fill from the table's locations
                    overflowLocationSource.fillChunkUnordered(bc.overflowFillContext, bc.overflowLocations, bc.overflowLocationsToFetch);
                    bc.chunkPositionsToInsertInOverflow.setSize(0);
                    bc.tableLocationsToInsertInOverflow.setSize(0);

                    // overflow slots now contains the positions in the overflow columns

                    while (bc.overflowPositionInSourceChunk.size() > 0) {
                        // now we have the overflow slot for each of the things we are interested in.
                        // if the slot is null, then we can insert it and we are complete.

                        bc.overflowLocationsToFetch.setSize(0);
                        bc.chunkPositionsForFetches.setSize(0);

                        // TODO: Crunch it down
                        for (int ii = 0; ii < bc.overflowLocations.size(); ++ii) {
                            final int overflowLocation = bc.overflowLocations.get(ii);
                            final int chunkPosition = bc.overflowPositionInSourceChunk.get(ii);
                            if (overflowLocation == QueryConstants.NULL_INT) {
                                // insert me into overflow in the next free overflow slot
                                bc.chunkPositionsToInsertInOverflow.add(chunkPosition);
                                bc.tableLocationsToInsertInOverflow.add(bc.tableLocationsChunk.get(chunkPosition));
                            } else {
                                // add to the key positions to fetch
                                bc.chunkPositionsForFetches.add(chunkPosition);
                                bc.overflowLocationsToFetch.add(overflowLocation);
                            }
                        }

                        // if the slot is non-null, then we need to fetch the overflow values for comparison
                        fillOverflowKeys(bc.overflowContexts, bc.overflowKeyChunks, bc.overflowLocationsToFetch);

                        // now compare the value in our overflowKeyChunk to the value in the sourceChunk
                        checkLhsPermutedEquality(bc.chunkPositionsForFetches, sourceKeyChunks, bc.overflowKeyChunks, bc.equalValues);

                        int writePosition = 0;
                        for (int ii = 0; ii < bc.equalValues.size(); ++ii) {
                            final int chunkPosition = bc.chunkPositionsForFetches.get(ii);
                            final long overflowLocation = bc.overflowLocationsToFetch.get(ii);
                            if (bc.equalValues.get(ii)) {
                                // region build overflow found
                                // if we are equal, then it's great and we know what our left-hand side slot is
                                // (represented as a negative number to indicate overflow)
                                sourceChunkLeftHashSlots.set(chunkPosition, overflowLocationToHashLocation(overflowLocation));
                                addLeftIndexOverflow(overflowLocation, sourceIndexKeys.get(chunkPosition));
                                // endregion build overflow found
                            } else {
                                // otherwise, we need to repeat the overflow calculation, with our next overflow fetch
                                bc.overflowLocationsToFetch.set(writePosition, overflowLocation);
                                bc.overflowPositionInSourceChunk.set(writePosition++, chunkPosition);
                            }
                        }
                        bc.overflowLocationsToFetch.setSize(writePosition);
                        bc.overflowPositionInSourceChunk.setSize(writePosition);

                        // on subsequent iterations, we are following the overflow chains, so we fill from the overflowOverflowLocationSource
                        if (bc.overflowPositionInSourceChunk.size() > 0) {
                            overflowOverflowLocationSource.fillChunkUnordered(bc.overflowOverflowFillContext, bc.overflowLocations, bc.overflowLocationsToFetch);
                        }
                    }

                    // make sure we actually have enough room to insert stuff where we would like
                    ensureOverflowCapacity(bc.chunkPositionsToInsertInOverflow);

                    firstBackingChunkLocation = -1;
                    lastBackingChunkLocation = -1;
                    bc.destinationLocationPositionInWriteThrough.setSize(0);
                    bc.sourcePositions.setSize(0);

                    // do the overflow insertions, one per table position at a time; until we have no insertions left
                    while (bc.chunkPositionsToInsertInOverflow.size() > 0) {
                        // sort by table position
                        LongIntTimsortKernel.sort(bc.sortContext, bc.chunkPositionsToInsertInOverflow, bc.tableLocationsToInsertInOverflow);

                        bc.chunkPositionsToCheckForEquality.setSize(0);
                        bc.overflowLocationForEqualityCheck.setSize(0);

                        for (int ii = 0; ii < bc.chunkPositionsToInsertInOverflow.size(); ) {
                            final long tableLocation = bc.tableLocationsToInsertInOverflow.get(ii);
                            final int chunkPosition = bc.chunkPositionsToInsertInOverflow.get(ii);

                            final int allocatedOverflowLocation = allocateOverflowLocation();

                            // we are inserting into the head of the list, so we move the existing overflow into our overflow
                            overflowOverflowLocationSource.set(allocatedOverflowLocation, overflowLocationSource.getUnsafe(tableLocation));
                            // and we point the overflow at our slot
                            overflowLocationSource.set(tableLocation, allocatedOverflowLocation);

                            // region build overflow insert
                            sourceChunkLeftHashSlots.set(chunkPosition, overflowLocationToHashLocation(allocatedOverflowLocation));
                            // we set the right RowSet to indicate it is empty, but exists
                            addLeftIndexOverflow(allocatedOverflowLocation, sourceIndexKeys.get(chunkPosition));
                            overflowRightRowSetSource.set(allocatedOverflowLocation, NO_RIGHT_ENTRY_VALUE);
                            // endregion build overflow insert


                            // get the backing chunk from the overflow keys
                            if (allocatedOverflowLocation > lastBackingChunkLocation || allocatedOverflowLocation < firstBackingChunkLocation) {
                                flushWriteThrough(bc.sourcePositions, sourceKeyChunks, bc.destinationLocationPositionInWriteThrough, bc.writeThroughChunks);
                                firstBackingChunkLocation = updateWriteThroughChunks(bc.writeThroughChunks, allocatedOverflowLocation, overflowKeySources);
                                lastBackingChunkLocation = firstBackingChunkLocation + bc.writeThroughChunks[0].size() - 1;
                            }

                            // now we must set all of our key values in the overflow
                            bc.sourcePositions.add(chunkPosition);
                            bc.destinationLocationPositionInWriteThrough.add((int)(allocatedOverflowLocation - firstBackingChunkLocation));

                            while (++ii < bc.tableLocationsToInsertInOverflow.size() && bc.tableLocationsToInsertInOverflow.get(ii) == tableLocation) {
                                bc.overflowLocationForEqualityCheck.add(allocatedOverflowLocation);
                                bc.chunkPositionsToCheckForEquality.add(chunkPosition);
                                bc.chunkPositionsToCheckForEquality.add(bc.chunkPositionsToInsertInOverflow.get(ii));
                            }
                        }

                        // now we need to do the equality check; so that we can mark things appropriately
                        int remainingInserts = 0;

                        checkPairEquality(bc.chunkPositionsToCheckForEquality, sourceKeyChunks, bc.equalValues);
                        for (int ii = 0; ii < bc.equalValues.size(); ii++) {
                            final int chunkPosition = bc.chunkPositionsToCheckForEquality.get(ii * 2 + 1);
                            final long tableLocation = bc.tableLocationsChunk.get(chunkPosition);

                            if (bc.equalValues.get(ii)) {
                                final long insertedOverflowLocation = bc.overflowLocationForEqualityCheck.get(ii);
                                // region build overflow duplicate
                                // we match the first element, so should use the overflow slow we allocated for it
                                sourceChunkLeftHashSlots.set(chunkPosition, overflowLocationToHashLocation(insertedOverflowLocation));
                                addLeftIndexOverflow(insertedOverflowLocation, sourceIndexKeys.get(chunkPosition));
                                // endregion build overflow duplicate
                            } else {
                                // we need to try this element again in the next round
                                bc.chunkPositionsToInsertInOverflow.set(remainingInserts, chunkPosition);
                                bc.tableLocationsToInsertInOverflow.set(remainingInserts++, tableLocation);
                            }
                        }

                        bc.chunkPositionsToInsertInOverflow.setSize(remainingInserts);
                        bc.tableLocationsToInsertInOverflow.setSize(remainingInserts);
                    }
                    flushWriteThrough(bc.sourcePositions, sourceKeyChunks, bc.destinationLocationPositionInWriteThrough, bc.writeThroughChunks);
                }

                // region copy hash slots
                for (int ii = 0; ii < sourceChunkLeftHashSlots.size(); ++ii) {
                    resultSource.set(hashSlotOffset + ii, sourceChunkLeftHashSlots.get(ii));
                }
                // endregion copy hash slots
                hashSlotOffset += chunkOk.size();
            }
            // region post build loop
            sourceChunkLeftHashSlots.close();
            sourceIndexKeys.close();

            // compact indices that were possibly built piecemeal
            for (int ii = 0; ii < tableSize; ++ii) {
                final WritableRowSet rowSet = leftRowSetSource.get(ii);
                if (rowSet != null) {
                    rowSet.compact();
                }
            }
            for (int ii = 0; ii < nextOverflowLocation; ++ii) {
                final WritableRowSet rowSet = overflowLeftRowSetSource.get(ii);
                if (rowSet != null) {
                    rowSet.compact();
                }
            }
            // endregion post build loop
        }
    }


    private int allocateOverflowLocation() {
        return nextOverflowLocation++;
    }

    private static long updateWriteThroughChunks(ResettableWritableChunk<Values>[] writeThroughChunks, long currentHashLocation, ArrayBackedColumnSource<?>[] sources) {
        final long firstBackingChunkPosition = sources[0].resetWritableChunkToBackingStore(writeThroughChunks[0], currentHashLocation);
        for (int jj = 1; jj < sources.length; ++jj) {
            if (sources[jj].resetWritableChunkToBackingStore(writeThroughChunks[jj], currentHashLocation) != firstBackingChunkPosition) {
                throw new IllegalStateException("ArrayBackedColumnSources have different block sizes!");
            }
            if (writeThroughChunks[jj].size() != writeThroughChunks[0].size()) {
                throw new IllegalStateException("ArrayBackedColumnSources have different block sizes!");
            }
        }
        return firstBackingChunkPosition;
    }

    private void flushWriteThrough(WritableIntChunk<ChunkPositions> sourcePositions, Chunk<Values>[] sourceKeyChunks, WritableIntChunk<ChunkPositions> destinationLocationPositionInWriteThrough, WritableChunk<Values>[] writeThroughChunks) {
        if (sourcePositions.size() < 0) {
            return;
        }
        for (int jj = 0; jj < keySources.length; ++jj) {
            chunkCopiers[jj].permute(sourcePositions, sourceKeyChunks[jj], destinationLocationPositionInWriteThrough, writeThroughChunks[jj]);
        }
        sourcePositions.setSize(0);
        destinationLocationPositionInWriteThrough.setSize(0);
    }


    private void checkKeyEquality(WritableBooleanChunk<Any> equalValues, WritableChunk<Values>[] workingKeyChunks, Chunk<Values>[] sourceKeyChunks) {
        for (int ii = 0; ii < sourceKeyChunks.length; ++ii) {
            chunkEquals[ii].andEqual(workingKeyChunks[ii], sourceKeyChunks[ii], equalValues);
        }
    }

    private void checkLhsPermutedEquality(WritableIntChunk<ChunkPositions> chunkPositionsForFetches, Chunk<Values>[] sourceKeyChunks, WritableChunk<Values>[] overflowKeyChunks, WritableBooleanChunk<Any> equalValues) {
        chunkEquals[0].equalLhsPermuted(chunkPositionsForFetches, sourceKeyChunks[0], overflowKeyChunks[0], equalValues);
        for (int ii = 1; ii < overflowKeySources.length; ++ii) {
            chunkEquals[ii].andEqualLhsPermuted(chunkPositionsForFetches, sourceKeyChunks[ii], overflowKeyChunks[ii], equalValues);
        }
    }

    private void checkPairEquality(WritableIntChunk<ChunkPositions> chunkPositionsToCheckForEquality, Chunk<Values>[] sourceKeyChunks, WritableBooleanChunk<Any> equalPairs) {
        chunkEquals[0].equalPairs(chunkPositionsToCheckForEquality, sourceKeyChunks[0], equalPairs);
        for (int ii = 1; ii < keyColumnCount; ++ii) {
            chunkEquals[ii].andEqualPairs(chunkPositionsToCheckForEquality, sourceKeyChunks[ii], equalPairs);
        }
    }

    private void fillKeys(ColumnSource.FillContext[] fillContexts, WritableChunk<Values>[] keyChunks, WritableLongChunk<RowKeys> tableLocationsChunk) {
        fillKeys(keySources, fillContexts, keyChunks, tableLocationsChunk);
    }

    private void fillOverflowKeys(ColumnSource.FillContext[] fillContexts, WritableChunk<Values>[] keyChunks, WritableLongChunk<RowKeys> overflowLocationsChunk) {
        fillKeys(overflowKeySources, fillContexts, keyChunks, overflowLocationsChunk);
    }

    private static void fillKeys(ArrayBackedColumnSource<?>[] keySources, ColumnSource.FillContext[] fillContexts, WritableChunk<Values>[] keyChunks, WritableLongChunk<RowKeys> keyIndices) {
        for (int ii = 0; ii < keySources.length; ++ii) {
            keySources[ii].fillChunkUnordered(fillContexts[ii], keyChunks[ii], keyIndices);
        }
    }

    private void hashKeyChunks(WritableIntChunk<HashCodes> hashChunk, Chunk<Values>[] sourceKeyChunks) {
        chunkHashers[0].hashInitial(sourceKeyChunks[0], hashChunk);
        for (int ii = 1; ii < sourceKeyChunks.length; ++ii) {
            chunkHashers[ii].hashUpdate(sourceKeyChunks[ii], hashChunk);
        }
    }

    private void getKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts, Chunk<? extends Values>[] chunks, RowSequence rowSequence) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getChunk(contexts[ii], rowSequence);
        }
    }

    // mixin prev
    private void getPrevKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts, Chunk<? extends Values>[] chunks, RowSequence rowSequence) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getPrevChunk(contexts[ii], rowSequence);
        }
    }
    // endmixin prev

    // region probe wrappers
    @Override
    protected void decorateLeftSide(RowSet leftRowSet, ColumnSource<?>[] leftSources, LongArraySource leftRedirections) {
        // TODO: FIGURE OUT THE RIGHT INTERFACE HERE
        throw new UnsupportedOperationException();
    }

    @Override
    public void addRightSide(RowSequence rightIndex, ColumnSource<?> [] rightSources) {
        if (rightIndex.isEmpty()) {
            return;
        }
        try (final ProbeContext pc = makeProbeContext(rightSources, rightIndex.size())) {
            decorationProbe(pc, rightIndex, rightSources, false, false, true, false, false, 0, null);
        }
    }

    @Override
    public void addRightSide(final Context pc, RowSequence rightIndex, ColumnSource<?> [] rightSources, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (rightIndex.isEmpty()) {
            return;
        }
        decorationProbe((ProbeContext)pc, rightIndex, rightSources, false, false, true, false, false, 0, modifiedSlotTracker);
    }

    @Override
    public void removeRight(final Context pc, RowSequence rightIndex, ColumnSource<?> [] rightSources, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker)  {
        if (rightIndex.isEmpty()) {
            return;
        }
        decorationProbe((ProbeContext)pc, rightIndex, rightSources, true, false, false, true, false, 0, modifiedSlotTracker);
    }

    @Override
    public void modifyByRight(final Context pc, RowSet modified, ColumnSource<?>[] rightSources, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (modified.isEmpty()) {
            return;
        }
        decorationProbe((ProbeContext)pc, modified, rightSources, false, true, false, false, false, 0, modifiedSlotTracker);
    }

    @Override
    public void applyRightShift(Context pc, ColumnSource<?> [] rightSources, RowSet shiftedRowSet, long shiftDelta, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (shiftedRowSet.isEmpty()) {
            return;
        }
        decorationProbe((ProbeContext)pc, shiftedRowSet, rightSources, false, false, false, false, true, shiftDelta, modifiedSlotTracker);
    }
    // endregion probe wrappers

    // mixin decorationProbe
    class ProbeContext implements Context {
        final int chunkSize;

        final ColumnSource.FillContext stateSourceFillContext;
        final ColumnSource.FillContext overflowFillContext;
        final ColumnSource.FillContext overflowOverflowFillContext;

        final SharedContext sharedFillContext;
        final ColumnSource.FillContext[] workingFillContexts;
        final SharedContext sharedOverflowContext;
        final ColumnSource.FillContext[] overflowContexts;

        // the chunk of hashcodes
        final WritableIntChunk<HashCodes> hashChunk;
        // the chunk of positions within our table
        final WritableLongChunk<RowKeys> tableLocationsChunk;

        // the chunk of right indices that we read from the hash table, the empty right index is used as a sentinel that the
        // state exists; otherwise when building from the left it is always null
        // @WritableStateChunkType@ from \QWritableLongChunk<Values>\E
        final WritableLongChunk<Values> workingStateEntries;

        // the overflow locations that we need to get from the overflowLocationSource (or overflowOverflowLocationSource)
        final WritableLongChunk<RowKeys> overflowLocationsToFetch;
        // the overflow position in the working keychunks, parallel to the overflowLocationsToFetch
        final WritableIntChunk<ChunkPositions> overflowPositionInWorkingChunk;
        // values we have read from the overflow locations sources
        final WritableIntChunk<Values> overflowLocations;
        // when fetching from the overflow, we record which chunk position we are fetching for
        final WritableIntChunk<ChunkPositions> chunkPositionsForFetches;

        final WritableBooleanChunk<Any> equalValues;
        final WritableChunk<Values>[] workingKeyChunks;

        final SharedContext sharedProbeContext;
        // the contexts for filling from our key columns
        final ChunkSource.GetContext[] probeContexts;

        // region probe context

        // the chunk of indices created from our RowSequence, used to write into the hash table
        final WritableLongChunk<OrderedRowKeys> keyIndices;

        // endregion probe context
        final boolean haveSharedContexts;

        private ProbeContext(ColumnSource<?>[] probeSources,
                             int chunkSize
                             // region probe context constructor args
                             // endregion probe context constructor args
                            ) {
            Assert.gtZero(chunkSize, "chunkSize");
            this.chunkSize = chunkSize;
            haveSharedContexts = probeSources.length > 1;
            if (haveSharedContexts) {
                sharedFillContext = SharedContext.makeSharedContext();
                sharedOverflowContext = SharedContext.makeSharedContext();
                sharedProbeContext = SharedContext.makeSharedContext();
            } else {
                // No point in the additional work implied by these being non null.
                sharedFillContext = null;
                sharedOverflowContext = null;
                sharedProbeContext = null;
            }
            workingFillContexts = makeFillContexts(keySources, sharedFillContext, chunkSize);
            overflowContexts = makeFillContexts(overflowKeySources, sharedOverflowContext, chunkSize);
            probeContexts = makeGetContexts(probeSources, sharedProbeContext, chunkSize);
            // region probe context constructor
            keyIndices = WritableLongChunk.makeWritableChunk(chunkSize);
            // endregion probe context constructor
            stateSourceFillContext = rightRowSetSource.makeFillContext(chunkSize);
            overflowFillContext = overflowLocationSource.makeFillContext(chunkSize);
            overflowOverflowFillContext = overflowOverflowLocationSource.makeFillContext(chunkSize);
            hashChunk = WritableIntChunk.makeWritableChunk(chunkSize);
            tableLocationsChunk = WritableLongChunk.makeWritableChunk(chunkSize);
            // @WritableStateChunkName@ from \QWritableLongChunk\E
            workingStateEntries = WritableLongChunk.makeWritableChunk(chunkSize);
            overflowLocationsToFetch = WritableLongChunk.makeWritableChunk(chunkSize);
            overflowPositionInWorkingChunk = WritableIntChunk.makeWritableChunk(chunkSize);
            overflowLocations = WritableIntChunk.makeWritableChunk(chunkSize);
            chunkPositionsForFetches = WritableIntChunk.makeWritableChunk(chunkSize);
            equalValues = WritableBooleanChunk.makeWritableChunk(chunkSize);
            workingKeyChunks = getWritableKeyChunks(chunkSize);
        }

        private void resetSharedContexts() {
            if (!haveSharedContexts) {
                return;
            }
            sharedFillContext.reset();
            sharedOverflowContext.reset();
            sharedProbeContext.reset();
        }

        private void closeSharedContexts() {
            if (!haveSharedContexts) {
                return;
            }
            sharedFillContext.close();
            sharedOverflowContext.close();
            sharedProbeContext.close();
        }

        @Override
        public void close() {
            stateSourceFillContext.close();
            overflowFillContext.close();
            overflowOverflowFillContext.close();
            closeArray(workingFillContexts);
            closeArray(overflowContexts);
            closeArray(probeContexts);
            hashChunk.close();
            tableLocationsChunk.close();
            workingStateEntries.close();
            overflowLocationsToFetch.close();
            overflowPositionInWorkingChunk.close();
            overflowLocations.close();
            chunkPositionsForFetches.close();
            equalValues.close();
            closeArray(workingKeyChunks);
            closeSharedContexts();
            // region probe context close
            keyIndices.close();
            // endregion probe context close
            closeSharedContexts();
        }
    }

    public ProbeContext makeProbeContext(ColumnSource<?>[] probeSources,
                                  long maxSize
                                  // region makeProbeContext args
                                  // endregion makeProbeContext args
    ) {
        return new ProbeContext(probeSources, (int)Math.min(maxSize, CHUNK_SIZE)
                // region makeProbeContext arg pass
                // endregion makeProbeContext arg pass
        );
    }

    private void decorationProbe(ProbeContext pc
                                , RowSequence probeIndex
                                , final ColumnSource<?>[] probeSources
                                 // mixin prev
                                , boolean usePrev
                                 // endmixin prev
                                 // region additional probe arguments
                                 , final boolean probeOnly
                                 , final boolean addRight
                                 , final boolean removeRight
                                 , final boolean shiftRight
                                 , final long shiftDelta
                                 , @Nullable final NaturalJoinModifiedSlotTracker modifiedSlotTracker
                                 // endregion additional probe arguments
    )  {
        // region probe start
        Assert.assertion(probeOnly ^ addRight ^ removeRight ^ shiftRight, "probeOnly ^ addRight ^ removeRight ^ shiftRight");
        if (shiftRight) {
            Assert.neqZero(shiftDelta, "shiftDelta");
        }
        final byte modifiedFlag = shiftRight ? NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT : probeOnly ? NaturalJoinModifiedSlotTracker.FLAG_RIGHT_MODIFY_PROBE : NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE;
        // endregion probe start
        long hashSlotOffset = 0;

        try (final RowSequence.Iterator rsIt = probeIndex.getRowSequenceIterator();
             // region probe additional try resources
             // endregion probe additional try resources
            ) {
            //noinspection unchecked
            final Chunk<Values> [] sourceKeyChunks = new Chunk[keyColumnCount];

            // region probe initialization
            // endregion probe initialization

            while (rsIt.hasMore()) {
                // we reset shared contexts early to avoid carrying around state that can't be reused.
                pc.resetSharedContexts();
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(pc.chunkSize);
                final int chunkSize = chunkOk.intSize();

                // region probe loop initialization
                chunkOk.fillRowKeyChunk(pc.keyIndices);
                // endregion probe loop initialization

                // get our keys, hash them, and convert them to table locations
                // mixin prev
                if (usePrev) {
                    getPrevKeyChunks(probeSources, pc.probeContexts, sourceKeyChunks, chunkOk);
                } else {
                    // endmixin prev
                    getKeyChunks(probeSources, pc.probeContexts, sourceKeyChunks, chunkOk);
                    // mixin prev
                }
                // endmixin prev
                hashKeyChunks(pc.hashChunk, sourceKeyChunks);
                convertHashToTableLocations(pc.hashChunk, pc.tableLocationsChunk);

                // get the keys from the table
                fillKeys(pc.workingFillContexts, pc.workingKeyChunks, pc.tableLocationsChunk);

                // and the corresponding states
                // - if a value is empty; we don't care about it
                // - otherwise we check for equality; if we are equal, we have found our thing to set
                //   (or to complain if we are already set)
                // - if we are not equal, then we are an overflow block
                rightRowSetSource.fillChunkUnordered(pc.stateSourceFillContext, pc.workingStateEntries, pc.tableLocationsChunk);

                // @StateChunkIdentityName@ from \QLongChunk\E
                LongChunkEquals.notEqual(pc.workingStateEntries, EMPTY_RIGHT_VALUE, pc.equalValues);
                checkKeyEquality(pc.equalValues, pc.workingKeyChunks, sourceKeyChunks);

                pc.overflowPositionInWorkingChunk.setSize(0);
                pc.overflowLocationsToFetch.setSize(0);

                for (int ii = 0; ii < pc.equalValues.size(); ++ii) {
                    if (pc.equalValues.get(ii)) {
                        // region probe main found
                        final long tableLocation = pc.tableLocationsChunk.get(ii);
                        addModifiedMain(modifiedSlotTracker, tableLocation, modifiedFlag);

                        if (probeOnly) {
                            continue;
                        }

                        final long indexKey = pc.keyIndices.get(ii);
                        if (addRight) {
                            addRightIndex(tableLocation, indexKey);
                        } else if (removeRight) {
                            removeRightIndex(tableLocation, indexKey);
                        } else if (shiftRight) {
                            shiftRightIndex(tableLocation, indexKey, shiftDelta);
                        }
                        // endregion probe main found
                    } else if (pc.workingStateEntries.get(ii) != EMPTY_RIGHT_VALUE) {
                        // we must handle this as part of the overflow bucket
                        pc.overflowPositionInWorkingChunk.add(ii);
                        pc.overflowLocationsToFetch.add(pc.tableLocationsChunk.get(ii));
                    } else {
                        // region probe main not found
                        // endregion probe main not found
                    }
                }

                overflowLocationSource.fillChunkUnordered(pc.overflowFillContext, pc.overflowLocations, pc.overflowLocationsToFetch);

                while (pc.overflowLocationsToFetch.size() > 0) {
                    pc.overflowLocationsToFetch.setSize(0);
                    pc.chunkPositionsForFetches.setSize(0);
                    for (int ii = 0; ii < pc.overflowLocations.size(); ++ii) {
                        final int overflowLocation = pc.overflowLocations.get(ii);
                        final int chunkPosition = pc.overflowPositionInWorkingChunk.get(ii);

                        // if the overflow slot is null, this state is not responsive to the join so we can ignore it
                        if (overflowLocation != QueryConstants.NULL_INT) {
                            pc.overflowLocationsToFetch.add(overflowLocation);
                            pc.chunkPositionsForFetches.add(chunkPosition);
                        } else {
                            // region probe overflow not found
                            // endregion probe overflow not found
                        }
                    }

                    // if the slot is non-null, then we need to fetch the overflow values for comparison
                    fillOverflowKeys(pc.overflowContexts, pc.workingKeyChunks, pc.overflowLocationsToFetch);

                    // region probe overflow state source fill
                    // endregion probe overflow state source fill

                    // now compare the value in our workingKeyChunks to the value in the sourceChunk
                    checkLhsPermutedEquality(pc.chunkPositionsForFetches, sourceKeyChunks, pc.workingKeyChunks, pc.equalValues);

                    // we write back into the overflowLocationsToFetch, so we can't set its size to zero.  Instead
                    // we overwrite the elements in the front of the chunk referenced by a position cursor
                    int overflowPosition = 0;
                    for (int ii = 0; ii < pc.equalValues.size(); ++ii) {
                        final long overflowLocation = pc.overflowLocationsToFetch.get(ii);
                        final int chunkPosition = pc.chunkPositionsForFetches.get(ii);

                        if (pc.equalValues.get(ii)) {
                            // region probe overflow found
                            addModifiedOverflow(modifiedSlotTracker, overflowLocation, modifiedFlag);
                            if (probeOnly) {
                                continue;
                            }

                            final long indexKey = pc.keyIndices.get(chunkPosition);

                            if (addRight) {
                                addRightIndexOverflow(overflowLocation, indexKey);
                            } else if (removeRight) {
                                removeRightIndexOverflow(overflowLocation, indexKey);
                            } else if (shiftRight) {
                                shiftRightIndexOverflow(overflowLocation, indexKey, shiftDelta);
                            }
                            // endregion probe overflow found
                        } else {
                            // otherwise, we need to repeat the overflow calculation, with our next overflow fetch
                            pc.overflowLocationsToFetch.set(overflowPosition, overflowLocation);
                            pc.overflowPositionInWorkingChunk.set(overflowPosition, chunkPosition);
                            overflowPosition++;
                        }
                    }
                    pc.overflowLocationsToFetch.setSize(overflowPosition);
                    pc.overflowPositionInWorkingChunk.setSize(overflowPosition);

                    overflowOverflowLocationSource.fillChunkUnordered(pc.overflowOverflowFillContext, pc.overflowLocations, pc.overflowLocationsToFetch);
                }

                // region probe complete
                // endregion probe complete
                hashSlotOffset += chunkSize;
            }

            // region probe cleanup
            // endregion probe cleanup
        }
        // region probe final
        // endregion probe final
    }
    // endmixin decorationProbe

    private void convertHashToTableLocations(WritableIntChunk<HashCodes> hashChunk, WritableLongChunk<RowKeys> tablePositionsChunk) {

        // turn hash codes into indices within our table
        for (int ii = 0; ii < hashChunk.size(); ++ii) {
            final int hash = hashChunk.get(ii);
            final int location = hashToTableLocation(hash);
            tablePositionsChunk.set(ii, location);
        }
        tablePositionsChunk.setSize(hashChunk.size());
    }

    private int hashToTableLocation(
            int hash) {
        final int location = hash & (tableSize - 1);
        return location;
    }

    // region extraction functions
    static long overflowToSlot(long overflow) {
        return overflowLocationToHashLocation(overflow);
    }

    @Override
    public RowSet getLeftIndex(long slot) {
        if (isOverflowLocation(slot)) {
            return overflowLeftRowSetSource.get(hashLocationToOverflowLocation(slot));
        } else {
            return leftRowSetSource.get(slot);
        }
    }

    @Override
    public long getRightIndex(long slot) {
        final long rightIndex;
        if (isOverflowLocation(slot)) {
            rightIndex = overflowRightRowSetSource.getLong(hashLocationToOverflowLocation(slot));
        } else {
            rightIndex = rightRowSetSource.getLong(slot);
        }

        return rightIndex;
    }

    @Override
    public String keyString(long slot) {
        final WritableChunk<Values>[] keyChunk = getWritableKeyChunks(1);
        try (final WritableLongChunk<RowKeys> slotChunk = WritableLongChunk.makeWritableChunk(1)) {
            if (isOverflowLocation(slot)) {
                slotChunk.set(0, hashLocationToOverflowLocation(slot));
                final ColumnSource.FillContext[] contexts = makeFillContexts(overflowKeySources, null, 1);
                try {
                    fillOverflowKeys(contexts, keyChunk, slotChunk);
                } finally {
                    for (Context c : contexts) {
                        c.close();
                    }
                }
            } else {
                slotChunk.set(0, slot);
                final ColumnSource.FillContext[] contexts = makeFillContexts(keySources, null, 1);
                try {
                    fillKeys(contexts, keyChunk, slotChunk);
                } finally {
                    for (Context c : contexts) {
                        c.close();
                    }
                }
            }
            return ChunkUtils.extractKeyStringFromChunks(keyChunkTypes, keyChunk, 0);
        } finally {
            for (WritableChunk<Values> chunk : keyChunk) {
                chunk.close();
            }
        }
    }

    @Override
    public WritableRowRedirection buildRowRedirectionFromHashSlot(QueryTable leftTable, boolean exactMatch, RightIncrementalNaturalJoinStateManager.InitialBuildContext initialBuildContext, JoinControl.RedirectionType redirectionType) {
        final LongArraySource leftHashSlots = ((InitialBuildContext) initialBuildContext).leftHashSlots;
        return buildRowRedirection(leftTable, exactMatch, position -> getRightSide(leftHashSlots, position), redirectionType);
    }

    private long getRightSide(final LongArraySource leftHashSlots, final long position) {
        final long stateValue = getStateValue(leftHashSlots, position);
        if (stateValue == DUPLICATE_RIGHT_VALUE) {
            final long hashSlot = leftHashSlots.getLong(position);
            throw new IllegalStateException("Natural Join found duplicate right key for " + keyString(hashSlot));
        }
        return stateValue;
    }

    @Override
    public WritableRowRedirection buildRowRedirectionFromHashSlotGrouped(QueryTable leftTable, ObjectArraySource<WritableRowSet> rowSetSource, int groupingSize, boolean exactMatch, RightIncrementalNaturalJoinStateManager.InitialBuildContext initialBuildContext, JoinControl.RedirectionType redirectionType) {
        final LongArraySource leftHashSlots = ((InitialBuildContext) initialBuildContext).leftHashSlots;
        switch (redirectionType) {
            case Contiguous: {
                if (!leftTable.isFlat()) {
                    throw new IllegalStateException("Left table is not flat for contiguous row redirection build!");
                }
                // we can use an array, which is perfect for a small enough flat table
                final long[] innerIndex = new long[leftTable.intSize("contiguous redirection build")];
                for (int ii = 0; ii < groupingSize; ++ii) {
                    final long rightSide = getStateValue(leftHashSlots, ii);
                    final RowSet leftRowSet = rowSetSource.get(ii);
                    assert leftRowSet != null;
                    if (leftRowSet.isNonempty()) {
                        checkExactMatch(exactMatch, leftRowSet.firstRowKey(), rightSide);
                        leftRowSet.forAllRowKeys(li -> {
                            innerIndex[(int)li] = rightSide;
                        });
                    }
                }
                return new ContiguousWritableRowRedirection(innerIndex);
            }
            case Sparse: {
                final LongSparseArraySource sparseRedirections = new LongSparseArraySource();

                for (int ii = 0; ii < groupingSize; ++ii) {
                    final long rightSide = getStateValue(leftHashSlots, ii);
                    final RowSet leftRowSet = rowSetSource.get(ii);
                    assert leftRowSet != null;
                    if (leftRowSet.isNonempty()) {
                        checkExactMatch(exactMatch, leftRowSet.firstRowKey(), rightSide);
                        if (rightSide != NO_RIGHT_ENTRY_VALUE) {
                            leftRowSet.forAllRowKeys(li -> {
                                sparseRedirections.set(li, rightSide);
                            });
                        }
                    }
                }
                return new LongColumnSourceWritableRowRedirection(sparseRedirections);
            }
            case Hash: {
                final WritableRowRedirection rowRedirection = WritableRowRedirectionLockFree.FACTORY.createRowRedirection(leftTable.intSize());

                for (int ii = 0; ii < groupingSize; ++ii) {
                    final long rightSide = getStateValue(leftHashSlots, ii);
                    final RowSet leftRowSet = rowSetSource.get(ii);
                    assert leftRowSet != null;
                    if (leftRowSet.isNonempty()) {
                        checkExactMatch(exactMatch, leftRowSet.firstRowKey(), rightSide);
                        if (rightSide != NO_RIGHT_ENTRY_VALUE) {
                            leftRowSet.forAllRowKeys(li -> {
                                rowRedirection.put(li, rightSide);
                            });
                        }
                    }
                }

                return rowRedirection;
            }
        }
        throw new IllegalStateException("Bad redirectionType: " + redirectionType);
    }

    public void convertLeftGroups(int groupingSize, RightIncrementalNaturalJoinStateManager.InitialBuildContext initialBuildContext, ObjectArraySource<WritableRowSet> rowSetSource) {
        final InitialBuildContext ibc = (InitialBuildContext) initialBuildContext;
        for (int ii = 0; ii < groupingSize; ++ii) {
            final long slot = ibc.leftHashSlots.getUnsafe(ii);
            final RowSet oldRowSet;
            if (isOverflowLocation(slot)) {
                oldRowSet = overflowLeftRowSetSource.getAndSetUnsafe(hashLocationToOverflowLocation(slot), rowSetSource.get(ii));
            } else {
                oldRowSet = leftRowSetSource.getAndSetUnsafe(slot, rowSetSource.get(ii));
            }
            Assert.eq(oldRowSet.size(), "oldRowSet.size()", 1);
            Assert.eq(oldRowSet.get(0), "oldRowSet.get(0)", ii, "ii");
        }
    }
    // endregion extraction functions

    @NotNull
    private static ColumnSource.FillContext[] makeFillContexts(ColumnSource<?>[] keySources, final SharedContext sharedContext, int chunkSize) {
        final ColumnSource.FillContext[] workingFillContexts = new ColumnSource.FillContext[keySources.length];
        for (int ii = 0; ii < keySources.length; ++ii) {
            workingFillContexts[ii] = keySources[ii].makeFillContext(chunkSize, sharedContext);
        }
        return workingFillContexts;
    }

    private static ColumnSource.GetContext[] makeGetContexts(ColumnSource<?> [] sources, final SharedContext sharedState, int chunkSize) {
        final ColumnSource.GetContext[] contexts = new ColumnSource.GetContext[sources.length];
        for (int ii = 0; ii < sources.length; ++ii) {
            contexts[ii] = sources[ii].makeGetContext(chunkSize, sharedState);
        }
        return contexts;
    }

    @NotNull
    private WritableChunk<Values>[] getWritableKeyChunks(int chunkSize) {
        //noinspection unchecked
        final WritableChunk<Values>[] workingKeyChunks = new WritableChunk[keyChunkTypes.length];
        for (int ii = 0; ii < keyChunkTypes.length; ++ii) {
            workingKeyChunks[ii] = keyChunkTypes[ii].makeWritableChunk(chunkSize);
        }
        return workingKeyChunks;
    }

    @NotNull
    private ResettableWritableChunk<Values>[] getResettableWritableKeyChunks() {
        //noinspection unchecked
        final ResettableWritableChunk<Values>[] workingKeyChunks = new ResettableWritableChunk[keyChunkTypes.length];
        for (int ii = 0; ii < keyChunkTypes.length; ++ii) {
            workingKeyChunks[ii] = keyChunkTypes[ii].makeResettableWritableChunk();
        }
        return workingKeyChunks;
    }

    // region getStateValue
    private long getStateValue(final LongArraySource hashSlots, final long locationInHashSlots) {
        final long hashSlot = hashSlots.getLong(locationInHashSlots);
        if (isOverflowLocation(hashSlot)) {
            return overflowRightRowSetSource.getLong(hashLocationToOverflowLocation(hashSlot));
        }
        else {
            return rightRowSetSource.getLong(hashSlot);
        }
    }
    // endregion getStateValue

    // region overflowLocationToHashLocation
    static boolean isOverflowLocation(long hashSlot) {
        return hashSlot < OVERFLOW_PIVOT_VALUE;
    }

    static long hashLocationToOverflowLocation(long hashSlot) {
        return -hashSlot - 1 + OVERFLOW_PIVOT_VALUE;
    }

    private static long overflowLocationToHashLocation(long overflowSlot) {
        return OVERFLOW_PIVOT_VALUE - (1 + overflowSlot);
    }
    // endregion overflowLocationToHashLocation


    static int hashTableSize(long initialCapacity) {
        return (int)Math.max(MINIMUM_INITIAL_HASH_SIZE, Math.min(MAX_TABLE_SIZE, Long.highestOneBit(initialCapacity) * 2));
    }

}

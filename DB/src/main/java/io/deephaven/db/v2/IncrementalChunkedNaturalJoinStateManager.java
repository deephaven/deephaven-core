/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.db.v2;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.Assert;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.hashing.*;
// this is ugly to have twice, but we do need it twice for replication
// @StateChunkIdentityName@ from LongChunk
import io.deephaven.db.v2.hashing.LongChunkEquals;
import io.deephaven.db.v2.sort.permute.PermuteKernel;
import io.deephaven.db.v2.sort.timsort.LongIntTimsortKernel;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.utils.*;

// mixin rehash
import java.util.Arrays;
import io.deephaven.db.v2.sort.permute.IntPermuteKernel;
// @StateChunkTypeEnum@ from Long
import io.deephaven.db.v2.sort.permute.LongPermuteKernel;
import io.deephaven.db.v2.utils.compact.IntCompactKernel;
import io.deephaven.db.v2.utils.compact.LongCompactKernel;
// endmixin rehash

import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;

// region extra imports
import io.deephaven.db.tables.Table;
// endregion extra imports

import static io.deephaven.util.SafeCloseable.closeArray;

// region class visibility
// endregion class visibility
class IncrementalChunkedNaturalJoinStateManager
    // region extensions
    extends StaticNaturalJoinStateManager
    implements IncrementalNaturalJoinStateManager
    // endregion extensions
{
    // region constants
    public static final int CHUNK_SIZE = 4096;
    private static final int MINIMUM_INITIAL_HASH_SIZE = CHUNK_SIZE;
    private static final long MAX_TABLE_SIZE = 1L << 30;
    // endregion constants

    // mixin rehash
    static final double DEFAULT_MAX_LOAD_FACTOR = 0.75;
    static final double DEFAULT_TARGET_LOAD_FACTOR = 0.70;
    // endmixin rehash

    // region preamble variables
    // endregion preamble variables

    @ReplicateHashTable.EmptyStateValue
    // @NullStateValue@ from QueryConstants.NULL_LONG, @StateValueType@ from long
    private static final long EMPTY_RIGHT_VALUE = QueryConstants.NULL_LONG;

    // mixin getStateValue
    // region overflow pivot
    private static final long OVERFLOW_PIVOT_VALUE = DUPLICATE_RIGHT_VALUE;
    // endregion overflow pivot
    // endmixin getStateValue

    // the number of slots in our table
    // mixin rehash
    private int tableSize;
    // endmixin rehash
    // altmixin rehash: private final int tableSize;

    // how many key columns we have
    private final int keyColumnCount;

    // mixin rehash
    private long numEntries = 0;

    /** Our table size must be 2^L (i.e. a power of two); and the pivot is between 2^(L-1) and 2^L.
     *
     * <p>When hashing a value, if hashCode % 2^L < tableHashPivot; then the destination location is hashCode % 2^L.
     * If hashCode % 2^L >= tableHashPivot, then the destination location is hashCode % 2^(L-1).  Once the pivot reaches
     * the table size, we can simply double the table size and repeat the process.</p>
     *
     * <p>This has the effect of only using hash table locations < hashTablePivot.  When we want to expand the table
     * we can move some of the entries from the location {@code tableHashPivot - 2^(L-1)} to tableHashPivot.  This
     * provides for incremental expansion of the hash table, without the need for a full rehash.</p>
      */
    private int tableHashPivot;

    // the table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table
    private double targetLoadFactor = DEFAULT_TARGET_LOAD_FACTOR;
    private double maximumLoadFactor = DEFAULT_MAX_LOAD_FACTOR;
    // TODO: We do not yet support contraction
    // private final double minimumLoadFactor = 0.5;

    private final IntegerArraySource freeOverflowLocations = new IntegerArraySource();
    private int freeOverflowCount = 0;
    // endmixin rehash

    // the keys for our hash entries
    private final ArrayBackedColumnSource<?>[] keySources;
    // the location of any overflow entry in this bucket
    private final IntegerArraySource overflowLocationSource = new IntegerArraySource();

    // we are going to also reuse this for our state entry, so that we do not need additional storage
    @ReplicateHashTable.StateColumnSource
    // @StateColumnSourceType@ from LongArraySource
    private final LongArraySource rightIndexSource
            // @StateColumnSourceConstructor@ from LongArraySource\(\)
            = new LongArraySource();

    // the keys for overflow
    private int nextOverflowLocation = 0;
    private final ArrayBackedColumnSource<?> [] overflowKeySources;
    // the location of the next key in an overflow bucket
    private final IntegerArraySource overflowOverflowLocationSource = new IntegerArraySource();
    // the overflow buckets for the right Index
    @ReplicateHashTable.OverflowStateColumnSource
    // @StateColumnSourceType@ from LongArraySource
    private final LongArraySource overflowRightIndexSource
            // @StateColumnSourceConstructor@ from LongArraySource\(\)
            = new LongArraySource();

    // the type of each of our key chunks
    private final ChunkType[] keyChunkTypes;

    // the operators for hashing and various equality methods
    private final ChunkHasher[] chunkHashers;
    private final ChunkEquals[] chunkEquals;
    private final PermuteKernel[] chunkCopiers;

    // mixin rehash
    // If we have objects in our key columns, then we should null them out if we delete an overflow row, this only
    // applies to ObjectArraySources, for primitives we are content to leave the dead entries in the tables, because
    // they will not affect GC.
    private final ObjectArraySource<?>[] overflowKeyColumnsToNull;
    // endmixin rehash

    // region extra variables
    // we assume that our right indices are going to be unique; in which case we do not actually want to store an index
    // however, for inactive states, we must store the complete index value, so that we can remove values from it in
    // case it does become active at some point.  If we have duplicate right hand side values, we store a reference
    // into this table
    private final ObjectArraySource<Index> rightIndexStorage;
    private int nextRightIndexLocation;
    private final Index freeRightIndexLocations = Index.CURRENT_FACTORY.getEmptyIndex();

    // we always store left index values parallel to the keys; we may want to optimize for single left indices to avoid
    // object allocation, but we do have fairly efficient single range indices at this point
    private final ObjectArraySource<Index> leftIndexSource;
    private final ObjectArraySource<Index> overflowLeftIndexSource;

    // we must maintain our cookie for modified state tracking
    private final LongArraySource modifiedTrackerCookieSource;
    private final LongArraySource overflowModifiedTrackerCookieSource;
    // endregion extra variables

    IncrementalChunkedNaturalJoinStateManager(ColumnSource<?>[] tableKeySources
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
        // mixin rehash
        this.tableHashPivot = tableSize;
        // endmixin rehash

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

        // mixin rehash
        overflowKeyColumnsToNull = Arrays.stream(overflowKeySources).filter(x -> x instanceof ObjectArraySource).map(x -> (ObjectArraySource)x).toArray(ObjectArraySource[]::new);
        // endmixin rehash

        // region constructor
        rightIndexStorage = new ObjectArraySource<>(Index.class);
        leftIndexSource = new ObjectArraySource<>(Index.class);
        overflowLeftIndexSource = new ObjectArraySource<>(Index.class);
        modifiedTrackerCookieSource = new LongArraySource();
        overflowModifiedTrackerCookieSource = new LongArraySource();
        // endregion constructor

        ensureCapacity(tableSize);
    }

    private void ensureCapacity(int tableSize) {
        rightIndexSource.ensureCapacity(tableSize);
        overflowLocationSource.ensureCapacity(tableSize);
        for (int ii = 0; ii < keyColumnCount; ++ii) {
            keySources[ii].ensureCapacity(tableSize);
        }
        // region ensureCapacity
        leftIndexSource.ensureCapacity(tableSize);
        modifiedTrackerCookieSource.ensureCapacity(tableSize);
        // endregion ensureCapacity
    }

    private void ensureOverflowCapacity(WritableIntChunk<ChunkPositions> chunkPositionsToInsertInOverflow) {
        final int locationsToAllocate = chunkPositionsToInsertInOverflow.size();
        // mixin rehash
        if (freeOverflowCount >= locationsToAllocate) {
            return;
        }
        final int newCapacity = nextOverflowLocation + locationsToAllocate - freeOverflowCount;
        // endmixin rehash
        // altmixin rehash: final int newCapacity = nextOverflowLocation + locationsToAllocate;
        overflowOverflowLocationSource.ensureCapacity(newCapacity);
        overflowRightIndexSource.ensureCapacity(newCapacity);
        //noinspection ForLoopReplaceableByForEach
        for (int ii = 0; ii < overflowKeySources.length; ++ii) {
            overflowKeySources[ii].ensureCapacity(newCapacity);
        }
        // region ensureOverflowCapacity
        overflowLeftIndexSource.ensureCapacity(newCapacity);
        overflowModifiedTrackerCookieSource.ensureCapacity(newCapacity);
        // endregion ensureOverflowCapacity
    }

    // region build wrappers
    void addRightSide(final BuildContext bc, OrderedKeys rightIndex, ColumnSource<?> [] rightSources, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (rightIndex.isEmpty()) {
            return;
        }
        buildTable(bc, rightIndex, rightSources, null, modifiedSlotTracker);
    }

    void addLeftSide(final BuildContext bc, OrderedKeys leftIndex, ColumnSource<?>[] leftSources, LongArraySource leftRedirections, NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (leftIndex.isEmpty()) {
            return;
        }
        buildTable(bc, leftIndex, leftSources,  leftRedirections, modifiedSlotTracker);
    }

    @Override
    void decorateLeftSide(Index leftIndex, ColumnSource<?>[] leftSources, LongArraySource leftRedirections) {
        if (leftIndex.isEmpty()) {
            return;
        }
        try (final BuildContext bc = makeBuildContext(leftSources, leftIndex.size())) {
            leftRedirections.ensureCapacity(leftIndex.size());
            buildTable(bc, leftIndex, leftSources, leftRedirections, null);
        }
    }

    void buildFromRightSide(final Table rightTable, ColumnSource<?> [] rightSources) {
        if (rightTable.isEmpty()) {
            return;
        }
        try (final BuildContext bc = makeBuildContext(rightSources, rightTable.size())) {
            buildTable(bc, rightTable.getIndex(), rightSources, null, null);
        }
    }

    private long allocateRightIndexSlot() {
        if (freeRightIndexLocations.size() > 0) {
            final long allocatedKey = freeRightIndexLocations.lastKey();
            freeRightIndexLocations.remove(allocatedKey);
            return allocatedKey;
        }
        return nextRightIndexLocation++;
    }

    private void ensureRightIndexCapacity() {
        final long freeSlots = freeRightIndexLocations.size();
        if (freeSlots > CHUNK_SIZE) {
            return;
        }
        rightIndexStorage.ensureCapacity(nextRightIndexLocation + CHUNK_SIZE - freeSlots);
    }

    private void addLeftIndex(long tableLocation, long keyToAdd) {
        final Index index = leftIndexSource.get(tableLocation);
        if (index == null) {
            leftIndexSource.set(tableLocation, Index.CURRENT_FACTORY.getIndexByValues(keyToAdd));
        } else {
            index.insert(keyToAdd);
        }
    }

    /**
     * Do we have multiple right indices for this value?
     *
     * @param existingRightIndex the value in our rightIndex column source
     * @return true if we should reference our rightIndexStorage column source
     */
    private boolean isDuplicateRightIndex(long existingRightIndex) {
        return existingRightIndex < DUPLICATE_RIGHT_VALUE;
    }

    /**
     * Given a reference in our state source, {@code isDuplicateRightIndex(existingRightIndex) == true}, produce the
     * location integer we can use to reference rightIndexStorage.
     *
     * @param existingRightIndex an entry from our state source
     * @return the location in rightIndexStorage
     */
    private long getRightIndexSlot(long existingRightIndex) {
        return -existingRightIndex - 1 + DUPLICATE_RIGHT_VALUE;
    }

    /**
     * Given a location in rightIndexStorage, produce a reference for use in the rightIndexSource
     *
     * @param rightIndexSlot a location in rightIndexStorage
     * @return a reference to rightIndexStorage that can be recovered by calling getRightIndexSlot
     */
    private long rightIndexSlotToStorage(long rightIndexSlot) {
        return DUPLICATE_RIGHT_VALUE - rightIndexSlot - 1;
    }

    private void addRightIndex(long tableLocation, long keyToAdd) {
        final long existingRightIndex = rightIndexSource.getUnsafe(tableLocation);
        if (existingRightIndex == NO_RIGHT_ENTRY_VALUE) {
            rightIndexSource.set(tableLocation, keyToAdd);
        }
        else if (isDuplicateRightIndex(existingRightIndex)) {
            final long rightIndexSlot = getRightIndexSlot(existingRightIndex);
            final Index indexToUpdate = rightIndexStorage.getUnsafe(rightIndexSlot);
            indexToUpdate.insert(keyToAdd);
        } else {
            final long rightIndexSlot = allocateRightIndexSlot();
            rightIndexSource.set(tableLocation, rightIndexSlotToStorage(rightIndexSlot));
            rightIndexStorage.set(rightIndexSlot, Index.CURRENT_FACTORY.getIndexByValues(existingRightIndex, keyToAdd));
        }
    }

    private void removeRightIndex(long tableLocation, long keyToRemove) {
        final long existingRightIndex = rightIndexSource.getUnsafe(tableLocation);
        if (existingRightIndex == keyToRemove) {
            rightIndexSource.set(tableLocation, NO_RIGHT_ENTRY_VALUE);
        } else {
            removeFromDuplicateIndex(rightIndexSource, tableLocation, keyToRemove, existingRightIndex);
        }
    }

    private void shiftRightIndex(long tableLocation, long shiftedKey, long shiftDelta) {
        final long existingRightIndex = rightIndexSource.getUnsafe(tableLocation);
        if (existingRightIndex == shiftedKey - shiftDelta) {
            rightIndexSource.set(tableLocation, shiftedKey);
        } else {
            if (!isDuplicateRightIndex(existingRightIndex)) {
                throw Assert.statementNeverExecuted("Existing Right Index: " + existingRightIndex + ", shiftedKey" + shiftedKey + ", shiftDelta=" + shiftDelta + ", key=" + keyString(tableLocation));
            }

            shiftDuplicateRightIndex(shiftedKey, shiftDelta, existingRightIndex);
        }
    }

    private void shiftLeftIndex(long tableLocation, long shiftedKey, long shiftDelta) {
        final Index existingLeftIndex = leftIndexSource.get(tableLocation);
        final long sizeBefore = existingLeftIndex.size();
        existingLeftIndex.remove(shiftedKey - shiftDelta);
        existingLeftIndex.insert(shiftedKey);
        Assert.eq(existingLeftIndex.size(), "existingLeftIndex.size()", sizeBefore, "sizeBefore");
    }

    private void shiftLeftIndexOverflow(long overflowLocation, long shiftedKey, long shiftDelta) {
        final Index existingLeftIndex = overflowLeftIndexSource.get(overflowLocation);
        final long sizeBefore = existingLeftIndex.size();
        existingLeftIndex.remove(shiftedKey - shiftDelta);
        existingLeftIndex.insert(shiftedKey);
        Assert.eq(existingLeftIndex.size(), "existingLeftIndex.size()", sizeBefore, "sizeBefore");
    }

    private void removeRightIndexOverflow(long overflowLocation, long keyToRemove) {
        final long existingRightIndex = overflowRightIndexSource.getUnsafe(overflowLocation);
        if (existingRightIndex == keyToRemove) {
            overflowRightIndexSource.set(overflowLocation, NO_RIGHT_ENTRY_VALUE);
        } else {
            removeFromDuplicateIndex(overflowRightIndexSource, overflowLocation, keyToRemove, existingRightIndex);
        }
    }

    private void shiftRightIndexOverflow(long overflowLocation, long shiftedKey, long shiftDelta) {
        final long existingRightIndex = overflowRightIndexSource.getUnsafe(overflowLocation);
        if (existingRightIndex == shiftedKey - shiftDelta) {
            overflowRightIndexSource.set(overflowLocation, shiftedKey);
        } else {
            shiftDuplicateRightIndex(shiftedKey, shiftDelta, existingRightIndex);
        }
    }

    private void shiftDuplicateRightIndex(long shiftedKey, long shiftDelta, long existingRightIndex) {
        if (!isDuplicateRightIndex(existingRightIndex)) {
            throw Assert.statementNeverExecuted("Existing Right Index: " + existingRightIndex + ", shiftedKey" + shiftedKey + ", shiftDelta=" + shiftDelta);
        }

        final long rightIndexSlot = getRightIndexSlot(existingRightIndex);
        final Index indexToUpdate = rightIndexStorage.get(rightIndexSlot);
        indexToUpdate.remove(shiftedKey - shiftDelta);
        indexToUpdate.insert(shiftedKey);
    }

    private void removeFromDuplicateIndex(LongArraySource indexSource, long location, long keyToRemove, long existingRightIndex) {
        if (!isDuplicateRightIndex(existingRightIndex)) {
            throw Assert.statementNeverExecuted("Existing Right Index: " + existingRightIndex + ", keyToRemove=" + keyToRemove);
        }

        final long rightIndexSlot = getRightIndexSlot(existingRightIndex);
        final Index indexToUpdate = rightIndexStorage.get(rightIndexSlot);
        indexToUpdate.remove(keyToRemove);
        if (indexToUpdate.empty()) {
            throw Assert.statementNeverExecuted();
        } else if (indexToUpdate.size() == 1) {
            indexSource.set(location, indexToUpdate.firstKey());
            rightIndexStorage.set(rightIndexSlot, null);
            freeRightIndexLocations.insert(rightIndexSlot);
        }
    }

    private void addLeftIndexOverflow(long overflowLocation, long keyToAdd) {
        final Index index = overflowLeftIndexSource.get(overflowLocation);
        if (index == null) {
            overflowLeftIndexSource.set(overflowLocation, Index.CURRENT_FACTORY.getIndexByValues(keyToAdd));
        } else {
            index.insert(keyToAdd);
        }
    }

    private void addRightIndexOverflow(long overflowLocation, long keyToAdd) {
        final long existingRightIndex = overflowRightIndexSource.getUnsafe(overflowLocation);
        if (existingRightIndex == NO_RIGHT_ENTRY_VALUE) {
            overflowRightIndexSource.set(overflowLocation, keyToAdd);
        }
        else if (isDuplicateRightIndex(existingRightIndex)) {
            final long rightIndexSlot = getRightIndexSlot(existingRightIndex);
            final Index indexToUpdate = rightIndexStorage.get(rightIndexSlot);
            indexToUpdate.insert(keyToAdd);
        } else {
            final long rightIndexSlot = allocateRightIndexSlot();
            overflowRightIndexSource.set(overflowLocation, rightIndexSlotToStorage(rightIndexSlot));
            rightIndexStorage.set(rightIndexSlot, Index.CURRENT_FACTORY.getIndexByValues(existingRightIndex, keyToAdd));
        }
    }

    private void addModifiedMain(NaturalJoinModifiedSlotTracker modifiedSlotTracker, long tableLocation, byte flag) {
        if (modifiedSlotTracker != null) {
            final long originalIndex = rightIndexSource.getUnsafe(tableLocation);
            modifiedTrackerCookieSource.set(tableLocation, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(tableLocation), tableLocation, originalIndex, flag));
        }
    }

    private void addModifiedOverflow(NaturalJoinModifiedSlotTracker modifiedSlotTracker, long overflowLocation, byte flag) {
        if (modifiedSlotTracker != null) {
            final long originalIndex = overflowRightIndexSource.getUnsafe(overflowLocation);
            overflowModifiedTrackerCookieSource.set(overflowLocation, modifiedSlotTracker.addOverflow(overflowModifiedTrackerCookieSource.getUnsafe(overflowLocation), overflowLocation, originalIndex, flag));
        }
    }

    private void moveModifiedSlot(NaturalJoinModifiedSlotTracker modifiedSlotTracker, long oldHashLocation, long newHashLocation) {
        if (modifiedSlotTracker != null) {
            final long cookie = modifiedTrackerCookieSource.getUnsafe(oldHashLocation);
            modifiedTrackerCookieSource.set(oldHashLocation, QueryConstants.NULL_LONG);
            modifiedSlotTracker.moveTableLocation(cookie, oldHashLocation, newHashLocation);
            modifiedTrackerCookieSource.set(newHashLocation, cookie);
        }
    }

    private void promoteModified(NaturalJoinModifiedSlotTracker modifiedSlotTracker, long overflowLocation, long tableLocation) {
        if (modifiedSlotTracker != null) {
            final long cookie = overflowModifiedTrackerCookieSource.getUnsafe(overflowLocation);
            overflowModifiedTrackerCookieSource.set(overflowLocation, QueryConstants.NULL_LONG);
            modifiedSlotTracker.promoteFromOverflow(cookie, overflowLocation, tableLocation);
            modifiedTrackerCookieSource.set(tableLocation, cookie);
        }
    }

    void compactAll() {
        for (int ii = 0; ii < tableSize; ++ii) {
            final Index index = leftIndexSource.get(ii);
            if (index != null) {
                index.compact();
            }
        }
        for (int ii = 0; ii < nextOverflowLocation; ++ii) {
            final Index index = overflowLeftIndexSource.get(ii);
            if (index != null) {
                index.compact();
            }
        }
    }

    // endregion build wrappers

    class BuildContext implements Context {
        final int chunkSize;

        final LongIntTimsortKernel.LongIntSortKernelContext sortContext;
        final ColumnSource.FillContext stateSourceFillContext;
        // mixin rehash
        final ColumnSource.FillContext overflowStateSourceFillContext;
        // endmixin rehash
        final ColumnSource.FillContext overflowFillContext;
        final ColumnSource.FillContext overflowOverflowFillContext;

        // the chunk of hashcodes
        final WritableIntChunk<HashCode> hashChunk;
        // the chunk of positions within our table
        final WritableLongChunk<KeyIndices> tableLocationsChunk;

        final ResettableWritableChunk<Values>[] writeThroughChunks = getResettableWritableKeyChunks();
        final WritableIntChunk<ChunkPositions> sourcePositions;
        final WritableIntChunk<ChunkPositions> destinationLocationPositionInWriteThrough;

        final WritableBooleanChunk<Any> filledValues;
        final WritableBooleanChunk<Any> equalValues;

        // the overflow locations that we need to get from the overflowLocationSource (or overflowOverflowLocationSource)
        final WritableLongChunk<KeyIndices> overflowLocationsToFetch;
        // the overflow position in the working key chunks, parallel to the overflowLocationsToFetch
        final WritableIntChunk<ChunkPositions> overflowPositionInSourceChunk;

        // the position with our hash table that we should insert a value into
        final WritableLongChunk<KeyIndices> insertTableLocations;
        // the position in our chunk, parallel to the workingChunkInsertTablePositions
        final WritableIntChunk<ChunkPositions> insertPositionsInSourceChunk;

        // we sometimes need to check two positions within a single chunk for equality, this contains those positions as pairs
        final WritableIntChunk<ChunkPositions> chunkPositionsToCheckForEquality;
        // While processing overflow insertions, parallel to the chunkPositions to check for equality, the overflow location that
        // is represented by the first of the pairs in chunkPositionsToCheckForEquality
        final WritableLongChunk<KeyIndices> overflowLocationForEqualityCheck;

        // the chunk of state values that we read from the hash table
        // @WritableStateChunkType@ from WritableLongChunk<Values>
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

        // mixin rehash
        final WritableLongChunk<KeyIndices> rehashLocations;
        final WritableIntChunk<Values> overflowLocationsToMigrate;
        final WritableLongChunk<KeyIndices> overflowLocationsAsKeyIndices;
        final WritableBooleanChunk<Any> shouldMoveBucket;

        final ResettableWritableLongChunk<Any> overflowLocationForPromotionLoop = ResettableWritableLongChunk.makeResettableChunk();

        // mixin allowUpdateWriteThroughState
        // @WritableStateChunkType@ from WritableLongChunk<Values>, @WritableStateChunkName@ from WritableLongChunk
        final ResettableWritableLongChunk<Values> writeThroughState = ResettableWritableLongChunk.makeResettableChunk();
        // endmixin allowUpdateWriteThroughState
        final ResettableWritableIntChunk<Values> writeThroughOverflowLocations = ResettableWritableIntChunk.makeResettableChunk();
        // endmixin rehash

        final SharedContext sharedFillContext;
        final ColumnSource.FillContext[] workingFillContexts;
        final SharedContext sharedOverflowContext;
        final ColumnSource.FillContext[] overflowContexts;
        final SharedContext sharedBuildContext;
        final ChunkSource.GetContext[] buildContexts;

        // region build context
        final WritableLongChunk<OrderedKeyIndices> sourceIndexKeys;
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
            sourceIndexKeys = WritableLongChunk.makeWritableChunk(chunkSize);
            // endregion build context constructor
            sortContext = LongIntTimsortKernel.createContext(chunkSize);
            stateSourceFillContext = rightIndexSource.makeFillContext(chunkSize);
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
            // @WritableStateChunkName@ from WritableLongChunk
            workingStateEntries = WritableLongChunk.makeWritableChunk(chunkSize);
            workingKeyChunks = getWritableKeyChunks(chunkSize);
            overflowKeyChunks = getWritableKeyChunks(chunkSize);
            chunkPositionsForFetches = WritableIntChunk.makeWritableChunk(chunkSize);
            chunkPositionsToInsertInOverflow = WritableIntChunk.makeWritableChunk(chunkSize);
            tableLocationsToInsertInOverflow = WritableLongChunk.makeWritableChunk(chunkSize);
            overflowLocations = WritableIntChunk.makeWritableChunk(chunkSize);
            // mixin rehash
            rehashLocations = WritableLongChunk.makeWritableChunk(chunkSize);
            overflowStateSourceFillContext = overflowRightIndexSource.makeFillContext(chunkSize);
            overflowLocationsToMigrate = WritableIntChunk.makeWritableChunk(chunkSize);
            overflowLocationsAsKeyIndices = WritableLongChunk.makeWritableChunk(chunkSize);
            shouldMoveBucket = WritableBooleanChunk.makeWritableChunk(chunkSize);
            // endmixin rehash
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
            // mixin rehash
            overflowStateSourceFillContext.close();
            // endmixin rehash
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
            // mixin rehash
            rehashLocations.close();
            overflowLocationsToMigrate.close();
            overflowLocationsAsKeyIndices.close();
            shouldMoveBucket.close();
            overflowLocationForPromotionLoop.close();
            // mixin allowUpdateWriteThroughState
            writeThroughState.close();
            // endmixin allowUpdateWriteThroughState
            writeThroughOverflowLocations.close();
            // endmixin rehash
            // region build context close
            sourceIndexKeys.close();
            // endregion build context close
            closeSharedContexts();
        }

    }

    BuildContext makeBuildContext(ColumnSource<?>[] buildSources,
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
                            final OrderedKeys buildIndex,
                            ColumnSource<?>[] buildSources
                            // region extra build arguments
                            , final LongArraySource leftRedirections
                            , final NaturalJoinModifiedSlotTracker modifiedSlotTracker
                            // endregion extra build arguments
    ) {
        long hashSlotOffset = 0;
        // region build start
        final boolean isLeftSide = leftRedirections != null;
        // endregion build start

        try (final OrderedKeys.Iterator okIt = buildIndex.getOrderedKeysIterator();
             // region build initialization try
             // the chunk of source indices that are parallel to the sourceChunks
             final WritableLongChunk<KeyIndices> workingLeftRedirections = isLeftSide ? WritableLongChunk.makeWritableChunk(bc.chunkSize) : null
             // endregion build initialization try
        ) {
            // region build initialization
            // endregion build initialization

            // chunks to write through to the table key sources


            //noinspection unchecked
            final Chunk<Values> [] sourceKeyChunks = new Chunk[buildSources.length];

            while (okIt.hasMore()) {
                // we reset early to avoid carrying around state for old OrderedKeys which can't be reused.
                bc.resetSharedContexts();

                final OrderedKeys chunkOk = okIt.getNextOrderedKeysWithLength(bc.chunkSize);

                getKeyChunks(buildSources, bc.buildContexts, sourceKeyChunks, chunkOk);
                hashKeyChunks(bc.hashChunk, sourceKeyChunks);

                // region build loop initialization
                chunkOk.fillKeyIndicesChunk(bc.sourceIndexKeys);
                if (isLeftSide) {
                    workingLeftRedirections.setSize(bc.sourceIndexKeys.size());
                } else {
                    // we may or may not overflow, but we would like to ensure that we can overflow if necessary
                    ensureRightIndexCapacity();
                }
                // endregion build loop initialization

                // turn hash codes into indices within our table
                convertHashToTableLocations(bc.hashChunk, bc.tableLocationsChunk);

                // now fetch the values from the table, note that we do not order these fetches
                fillKeys(bc.workingFillContexts, bc.workingKeyChunks, bc.tableLocationsChunk);

                // and the corresponding states, if a value is null, we've found our insertion point
                rightIndexSource.fillChunkUnordered(bc.stateSourceFillContext, bc.workingStateEntries, bc.tableLocationsChunk);

                // find things that exist
                // @StateChunkIdentityName@ from LongChunk
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
                        final long keyToAdd = bc.sourceIndexKeys.get(ii);
                        if (isLeftSide) {
                            workingLeftRedirections.set(ii, getResultRightIndex(tableLocation));
                            addLeftIndex(tableLocation, keyToAdd);
                        } else {
                            addModifiedMain(modifiedSlotTracker, tableLocation, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE);
                            addRightIndex(tableLocation, keyToAdd);
                        }
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
                    final long keyToAdd = bc.sourceIndexKeys.get(firstChunkPositionForHashLocation);
                    if (isLeftSide) {
                        rightIndexSource.set(currentHashLocation, NO_RIGHT_ENTRY_VALUE);
                        addLeftIndex(currentHashLocation, keyToAdd);
                        workingLeftRedirections.set(firstChunkPositionForHashLocation, NO_RIGHT_ENTRY_VALUE);
                    } else {
                        rightIndexSource.set(currentHashLocation, keyToAdd);
                        // there was no left hand side, so we should not worry about modifying it
                    }
                    // endregion main insert
                    // mixin rehash
                    numEntries++;
                    // endmixin rehash

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
                        final long keyToAdd = bc.sourceIndexKeys.get(chunkPosition);
                        if (isLeftSide) {
                            // we match the first element, so should use it
                            workingLeftRedirections.set(chunkPosition, NO_RIGHT_ENTRY_VALUE);
                            addLeftIndex(tableLocation, keyToAdd);
                        } else {
                            // we match the first element, so need to mark this key as a duplicate
                            addRightIndex(tableLocation, keyToAdd);
                            // we should already have added it to the modifiedMain builder above
                        }
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
                                final long keyToAdd = bc.sourceIndexKeys.get(chunkPosition);
                                if (isLeftSide) {
                                    // if we are equal, then it's great and we know what our left-hand side is
                                    final long hashSlot = overflowLocationToHashLocation(overflowLocation);
                                    workingLeftRedirections.set(chunkPosition, getResultRightIndex(hashSlot));
                                    addLeftIndexOverflow(overflowLocation, keyToAdd);
                                } else {
                                    addModifiedOverflow(modifiedSlotTracker, overflowLocation, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE);
                                    addRightIndexOverflow(overflowLocation, keyToAdd);
                                }
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
                            final long rightValue;
                            final long keyToAdd = bc.sourceIndexKeys.get(chunkPosition);
                            if (isLeftSide) {
                                workingLeftRedirections.set(chunkPosition, NO_RIGHT_ENTRY_VALUE);
                                // we set the right index to indicate it is empty, but exists
                                rightValue = NO_RIGHT_ENTRY_VALUE;
                                addLeftIndexOverflow(allocatedOverflowLocation, keyToAdd);
                            } else {
                                rightValue = keyToAdd;
                                // we are inserting the value, so there is no left hand side to care about
                            }
                            overflowRightIndexSource.set(allocatedOverflowLocation, rightValue);
                            // endregion build overflow insert

                            // mixin rehash
                            numEntries++;
                            // endmixin rehash

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
                                final long keyToAdd = bc.sourceIndexKeys.get(chunkPosition);
                                // we match the first element, so should use the overflow slow we allocated for it
                                if (isLeftSide) {
                                    workingLeftRedirections.set(chunkPosition, NO_RIGHT_ENTRY_VALUE);
                                    addLeftIndexOverflow(insertedOverflowLocation, keyToAdd);
                                } else {
                                    addRightIndexOverflow(insertedOverflowLocation, keyToAdd);
                                    // we are inserting the value, so there is no left hand side to care about
                                }
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
                    // mixin rehash
                    // region post-build rehash
                    doRehash(bc, modifiedSlotTracker);
                    // endregion post-build rehash
                    // endmixin rehash
                }

                // region copy hash slots
                if (isLeftSide) {
                    for (int ii = 0; ii < workingLeftRedirections.size(); ++ii) {
                        leftRedirections.set(hashSlotOffset + ii, workingLeftRedirections.get(ii));
                    }
                }
                // endregion copy hash slots
                hashSlotOffset += chunkOk.size();
            }
            // region post build loop
            // endregion post build loop
        }
    }

    // mixin rehash
    public void doRehash(BuildContext bc
                          // region extra rehash arguments
                          , NaturalJoinModifiedSlotTracker modifiedSlotTracker
                          // endregion extra rehash arguments
    ) {
        long firstBackingChunkLocation;
        long lastBackingChunkLocation;// mixin rehash
        // region rehash start
        // endregion rehash start
        while (rehashRequired()) {
            // region rehash loop start
            // endregion rehash loop start
            if (tableHashPivot == tableSize) {
                tableSize *= 2;
                ensureCapacity(tableSize);
                // region rehash ensure capacity
                // endregion rehash ensure capacity
            }

            final long targetBuckets = Math.min(MAX_TABLE_SIZE, (long)(numEntries / targetLoadFactor));
            final int bucketsToAdd = Math.max(1, (int)Math.min(Math.min(targetBuckets, tableSize) - tableHashPivot, bc.chunkSize));

            initializeRehashLocations(bc.rehashLocations, bucketsToAdd);

            // fill the overflow bucket locations
            overflowLocationSource.fillChunk(bc.overflowFillContext, bc.overflowLocations, OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(LongChunk.downcast(bc.rehashLocations)));
            // null out the overflow locations in the table
            setOverflowLocationsToNull(tableHashPivot - (tableSize >> 1), bucketsToAdd);

            while (bc.overflowLocations.size() > 0) {
                // figure out which table location each overflow location maps to
                compactOverflowLocations(bc.overflowLocations, bc.overflowLocationsToFetch);
                if (bc.overflowLocationsToFetch.size() == 0) {
                    break;
                }

                fillOverflowKeys(bc.overflowContexts, bc.workingKeyChunks, bc.overflowLocationsToFetch);
                hashKeyChunks(bc.hashChunk, bc.workingKeyChunks);
                convertHashToTableLocations(bc.hashChunk, bc.tableLocationsChunk, tableHashPivot + bucketsToAdd);

                // read the next chunk of overflow locations, which we will be overwriting in the next step
                overflowOverflowLocationSource.fillChunkUnordered(bc.overflowOverflowFillContext, bc.overflowLocations, bc.overflowLocationsToFetch);

                // swap the table's overflow pointer with our location
                swapOverflowPointers(bc.tableLocationsChunk, bc.overflowLocationsToFetch);
            }

            // now rehash the main entries

            rightIndexSource.fillChunkUnordered(bc.stateSourceFillContext, bc.workingStateEntries, bc.rehashLocations);
            // @StateChunkIdentityName@ from LongChunk
            LongChunkEquals.notEqual(bc.workingStateEntries, EMPTY_RIGHT_VALUE, bc.shouldMoveBucket);

            // crush down things that don't exist
            LongCompactKernel.compact(bc.rehashLocations, bc.shouldMoveBucket);

            // get the keys from the table
            fillKeys(bc.workingFillContexts, bc.workingKeyChunks, bc.rehashLocations);
            hashKeyChunks(bc.hashChunk, bc.workingKeyChunks);
            convertHashToTableLocations(bc.hashChunk, bc.tableLocationsChunk, tableHashPivot + bucketsToAdd);

            // figure out which ones must move
            LongChunkEquals.notEqual(bc.tableLocationsChunk, bc.rehashLocations, bc.shouldMoveBucket);

            firstBackingChunkLocation = -1;
            lastBackingChunkLocation = -1;
            // flushWriteThrough will have zero-ed out the sourcePositions and destinationLocationPositionInWriteThrough size

            int moves = 0;
            for (int ii = 0; ii < bc.shouldMoveBucket.size(); ++ii) {
                if (bc.shouldMoveBucket.get(ii)) {
                    moves++;
                    final long newHashLocation = bc.tableLocationsChunk.get(ii);
                    final long oldHashLocation = bc.rehashLocations.get(ii);

                    if (newHashLocation > lastBackingChunkLocation) {
                        flushWriteThrough(bc.sourcePositions, bc.workingKeyChunks, bc.destinationLocationPositionInWriteThrough, bc.writeThroughChunks);
                        firstBackingChunkLocation = updateWriteThroughChunks(bc.writeThroughChunks, newHashLocation, keySources);
                        lastBackingChunkLocation = firstBackingChunkLocation + bc.writeThroughChunks[0].size() - 1;
                    }

                    // @StateValueType@ from long
                    final long stateValueToMove = rightIndexSource.getUnsafe(oldHashLocation);
                    rightIndexSource.set(newHashLocation, stateValueToMove);
                    rightIndexSource.set(oldHashLocation, EMPTY_RIGHT_VALUE);
                    // region rehash move values
                    final Index oldLeftIndexValue = leftIndexSource.get(oldHashLocation);
                    leftIndexSource.set(newHashLocation, oldLeftIndexValue);
                    leftIndexSource.set(oldHashLocation, null);
                    moveModifiedSlot(modifiedSlotTracker, oldHashLocation, newHashLocation);
                    // endregion rehash move values

                    bc.sourcePositions.add(ii);
                    bc.destinationLocationPositionInWriteThrough.add((int)(newHashLocation - firstBackingChunkLocation));
                }
            }
            flushWriteThrough(bc.sourcePositions, bc.workingKeyChunks, bc.destinationLocationPositionInWriteThrough, bc.writeThroughChunks);

            // everything has been rehashed now, but we have some table locations that might have an overflow,
            // without actually having a main entry.  We walk through the empty main entries, pulling non-empty
            // overflow locations into the main table

            // figure out which of the two possible locations is empty, because (1) we moved something from it
            // or (2) we did not move something to it
            bc.overflowLocationsToFetch.setSize(bc.shouldMoveBucket.size());
            final int totalPromotionsToProcess = bc.shouldMoveBucket.size();
            createOverflowPartitions(bc.overflowLocationsToFetch, bc.rehashLocations, bc.shouldMoveBucket, moves);

            for (int loop = 0; loop < 2; loop++) {
                final boolean firstLoop = loop == 0;

                if (firstLoop) {
                    bc.overflowLocationForPromotionLoop.resetFromTypedChunk(bc.overflowLocationsToFetch, 0, moves);
                } else {
                    bc.overflowLocationForPromotionLoop.resetFromTypedChunk(bc.overflowLocationsToFetch, moves, totalPromotionsToProcess - moves);
                }

                overflowLocationSource.fillChunk(bc.overflowFillContext, bc.overflowLocations, OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(bc.overflowLocationForPromotionLoop));
                IntChunkEquals.notEqual(bc.overflowLocations, QueryConstants.NULL_INT, bc.shouldMoveBucket);

                // crunch the chunk down to relevant locations
                LongCompactKernel.compact(bc.overflowLocationForPromotionLoop, bc.shouldMoveBucket);
                IntCompactKernel.compact(bc.overflowLocations, bc.shouldMoveBucket);

                IntToLongCast.castInto(IntChunk.downcast(bc.overflowLocations), bc.overflowLocationsAsKeyIndices);

                // now fetch the overflow key values
                fillOverflowKeys(bc.overflowContexts, bc.workingKeyChunks, bc.overflowLocationsAsKeyIndices);
                // and their state values
                overflowRightIndexSource.fillChunkUnordered(bc.overflowStateSourceFillContext, bc.workingStateEntries, bc.overflowLocationsAsKeyIndices);
                // and where their next pointer is
                overflowOverflowLocationSource.fillChunkUnordered(bc.overflowOverflowFillContext, bc.overflowLocationsToMigrate, bc.overflowLocationsAsKeyIndices);

                // we'll have two sorted regions intermingled in the overflowLocationsToFetch, one of them is before the pivot, the other is after the pivot
                // so that we can use our write through chunks, we first process the things before the pivot; then have a separate loop for those
                // that go after
                firstBackingChunkLocation = -1;
                lastBackingChunkLocation = -1;

                for (int ii = 0; ii < bc.overflowLocationForPromotionLoop.size(); ++ii) {
                    final long tableLocation = bc.overflowLocationForPromotionLoop.get(ii);
                    if ((firstLoop && tableLocation < tableHashPivot) || (!firstLoop && tableLocation >= tableHashPivot)) {
                        if (tableLocation > lastBackingChunkLocation) {
                            if (bc.sourcePositions.size() > 0) {
                                // the permutes here are flushing the write through for the state and overflow locations

                                // mixin allowUpdateWriteThroughState
                                // @StateChunkTypeEnum@ from Long
                                LongPermuteKernel.permute(bc.sourcePositions, bc.workingStateEntries, bc.destinationLocationPositionInWriteThrough, bc.writeThroughState);
                                // endmixin allowUpdateWriteThroughState
                                IntPermuteKernel.permute(bc.sourcePositions, bc.overflowLocationsToMigrate, bc.destinationLocationPositionInWriteThrough, bc.writeThroughOverflowLocations);
                                flushWriteThrough(bc.sourcePositions, bc.workingKeyChunks, bc.destinationLocationPositionInWriteThrough, bc.writeThroughChunks);
                            }

                            firstBackingChunkLocation = updateWriteThroughChunks(bc.writeThroughChunks, tableLocation, keySources);
                            lastBackingChunkLocation = firstBackingChunkLocation + bc.writeThroughChunks[0].size() - 1;
                            // mixin allowUpdateWriteThroughState
                            updateWriteThroughState(bc.writeThroughState, firstBackingChunkLocation, lastBackingChunkLocation);
                            // endmixin allowUpdateWriteThroughState
                            updateWriteThroughOverflow(bc.writeThroughOverflowLocations, firstBackingChunkLocation, lastBackingChunkLocation);
                        }
                        bc.sourcePositions.add(ii);
                        bc.destinationLocationPositionInWriteThrough.add((int)(tableLocation - firstBackingChunkLocation));
                        // region promotion move
                        final long overflowLocation = bc.overflowLocationsAsKeyIndices.get(ii);
                        leftIndexSource.set(tableLocation, overflowLeftIndexSource.get(overflowLocation));
                        overflowLeftIndexSource.set(overflowLocation, null);
                        promoteModified(modifiedSlotTracker, overflowLocation, tableLocation);
                        // endregion promotion move
                    }
                }

                // the permutes are completing the state and overflow promotions write through
                // mixin allowUpdateWriteThroughState
                // @StateChunkTypeEnum@ from Long
                LongPermuteKernel.permute(bc.sourcePositions, bc.workingStateEntries, bc.destinationLocationPositionInWriteThrough, bc.writeThroughState);
                // endmixin allowUpdateWriteThroughState
                IntPermuteKernel.permute(bc.sourcePositions, bc.overflowLocationsToMigrate, bc.destinationLocationPositionInWriteThrough, bc.writeThroughOverflowLocations);
                flushWriteThrough(bc.sourcePositions, bc.workingKeyChunks, bc.destinationLocationPositionInWriteThrough, bc.writeThroughChunks);

                // now mark these overflow locations as free, so that we can reuse them
                freeOverflowLocations.ensureCapacity(freeOverflowCount + bc.overflowLocations.size());
                // by sorting them, they will be more likely to be in the same write through chunk when we pull them from the free list
                bc.overflowLocations.sort();
                for (int ii = 0; ii < bc.overflowLocations.size(); ++ii) {
                    freeOverflowLocations.set(freeOverflowCount++, bc.overflowLocations.get(ii));
                }
                nullOverflowObjectSources(bc.overflowLocations);
            }

            tableHashPivot += bucketsToAdd;
            // region rehash loop end
            // endregion rehash loop end
        }
        // region rehash final
        // endregion rehash final
    }

    public boolean rehashRequired() {
        return numEntries > (tableHashPivot * maximumLoadFactor) && tableHashPivot < MAX_TABLE_SIZE;
    }

    /**
     * This function can be stuck in for debugging if you are breaking the table to make sure each slot still corresponds
     * to the correct location.
     */
    @SuppressWarnings({"unused", "unchecked"})
    private void verifyKeyHashes() {
        final int maxSize = tableHashPivot;

        final ChunkSource.FillContext [] keyFillContext = makeFillContexts(keySources, SharedContext.makeSharedContext(), maxSize);
        final WritableChunk [] keyChunks = getWritableKeyChunks(maxSize);

        try (final WritableLongChunk<KeyIndices> positions = WritableLongChunk.makeWritableChunk(maxSize);
             final WritableBooleanChunk exists = WritableBooleanChunk.makeWritableChunk(maxSize);
             final WritableIntChunk hashChunk = WritableIntChunk.makeWritableChunk(maxSize);
             final WritableLongChunk<KeyIndices> tableLocationsChunk = WritableLongChunk.makeWritableChunk(maxSize);
             final SafeCloseableArray ignored = new SafeCloseableArray<>(keyFillContext);
             final SafeCloseableArray ignored2 = new SafeCloseableArray<>(keyChunks);
             // @StateChunkName@ from LongChunk
             final WritableLongChunk stateChunk = WritableLongChunk.makeWritableChunk(maxSize);
             final ChunkSource.FillContext fillContext = rightIndexSource.makeFillContext(maxSize)) {

            rightIndexSource.fillChunk(fillContext, stateChunk, Index.FACTORY.getFlatIndex(tableHashPivot));

            ChunkUtils.fillInOrder(positions);

            // @StateChunkIdentityName@ from LongChunk
            LongChunkEquals.notEqual(stateChunk, EMPTY_RIGHT_VALUE, exists);

            // crush down things that don't exist
            LongCompactKernel.compact(positions, exists);

            // get the keys from the table
            fillKeys(keyFillContext, keyChunks, positions);
            hashKeyChunks(hashChunk, keyChunks);
            convertHashToTableLocations(hashChunk, tableLocationsChunk, tableHashPivot);

            for (int ii = 0; ii < positions.size(); ++ii) {
                if (tableLocationsChunk.get(ii) != positions.get(ii)) {
                    throw new IllegalStateException();
                }
            }
        }
    }

    void setTargetLoadFactor(final double targetLoadFactor) {
        this.targetLoadFactor = targetLoadFactor;
    }

    void setMaximumLoadFactor(final double maximumLoadFactor) {
        this.maximumLoadFactor = maximumLoadFactor;
    }

    private void createOverflowPartitions(WritableLongChunk<KeyIndices> overflowLocationsToFetch, WritableLongChunk<KeyIndices> rehashLocations, WritableBooleanChunk<Any> shouldMoveBucket, int moves) {
        int startWritePosition = 0;
        int endWritePosition = moves;
        for (int ii = 0; ii < shouldMoveBucket.size(); ++ii) {
            if (shouldMoveBucket.get(ii)) {
                final long oldHashLocation = rehashLocations.get(ii);
                // this needs to be promoted, because we moved it
                overflowLocationsToFetch.set(startWritePosition++, oldHashLocation);
            } else {
                // we didn't move anything into the destination slot; so we need to promote its overflow
                final long newEmptyHashLocation = rehashLocations.get(ii) + (tableSize >> 1);
                overflowLocationsToFetch.set(endWritePosition++, newEmptyHashLocation);
            }
        }
    }

    private void setOverflowLocationsToNull(long start, int count) {
        for (int ii = 0; ii < count; ++ii) {
            overflowLocationSource.set(start + ii, QueryConstants.NULL_INT);
        }
    }

    private void initializeRehashLocations(WritableLongChunk<KeyIndices> rehashLocations, int bucketsToAdd) {
        rehashLocations.setSize(bucketsToAdd);
        for (int ii = 0; ii < bucketsToAdd; ++ii) {
            rehashLocations.set(ii, tableHashPivot + ii - (tableSize >> 1));
        }
    }

    private void compactOverflowLocations(IntChunk<Values> overflowLocations, WritableLongChunk<KeyIndices> overflowLocationsToFetch) {
        overflowLocationsToFetch.setSize(0);
        for (int ii = 0; ii < overflowLocations.size(); ++ii) {
            final int overflowLocation = overflowLocations.get(ii);
            if (overflowLocation != QueryConstants.NULL_INT) {
                overflowLocationsToFetch.add(overflowLocation);
            }
        }
    }

    private void swapOverflowPointers(LongChunk<KeyIndices> tableLocationsChunk, LongChunk<KeyIndices> overflowLocationsToFetch) {
        for (int ii = 0; ii < overflowLocationsToFetch.size(); ++ii) {
            final long newLocation = tableLocationsChunk.get(ii);
            final int existingOverflow = overflowLocationSource.getUnsafe(newLocation);
            final long overflowLocation = overflowLocationsToFetch.get(ii);
            overflowOverflowLocationSource.set(overflowLocation, existingOverflow);
            overflowLocationSource.set(newLocation, (int)overflowLocation);
        }
    }

    // mixin allowUpdateWriteThroughState
    // @WritableStateChunkType@ from WritableLongChunk<Values>
    private void updateWriteThroughState(ResettableWritableLongChunk<Values> writeThroughState, long firstPosition, long expectedLastPosition) {
        final long firstBackingChunkPosition = rightIndexSource.resetWritableChunkToBackingStore(writeThroughState, firstPosition);
        if (firstBackingChunkPosition != firstPosition) {
            throw new IllegalStateException("ArrayBackedColumnSources have different block sizes!");
        }
        if (firstBackingChunkPosition + writeThroughState.size() - 1 != expectedLastPosition) {
            throw new IllegalStateException("ArrayBackedColumnSources have different block sizes!");
        }
    }
    // endmixin allowUpdateWriteThroughState

    private void updateWriteThroughOverflow(ResettableWritableIntChunk writeThroughOverflow, long firstPosition, long expectedLastPosition) {
        final long firstBackingChunkPosition = overflowLocationSource.resetWritableChunkToBackingStore(writeThroughOverflow, firstPosition);
        if (firstBackingChunkPosition != firstPosition) {
            throw new IllegalStateException("ArrayBackedColumnSources have different block sizes!");
        }
        if (firstBackingChunkPosition + writeThroughOverflow.size() - 1 != expectedLastPosition) {
            throw new IllegalStateException("ArrayBackedColumnSources have different block sizes!");
        }
    }

    // endmixin rehash

    private int allocateOverflowLocation() {
        // mixin rehash
        if (freeOverflowCount > 0) {
            return freeOverflowLocations.getUnsafe(--freeOverflowCount);
        }
        // endmixin rehash
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

    // mixin rehash
    private void nullOverflowObjectSources(IntChunk<Values> locationsToNull) {
        for (ObjectArraySource<?> objectArraySource : overflowKeyColumnsToNull) {
            for (int ii = 0; ii < locationsToNull.size(); ++ii) {
                objectArraySource.set(locationsToNull.get(ii), null);
            }
        }
        // region nullOverflowObjectSources
        for (int ii = 0; ii < locationsToNull.size(); ++ii) {
            overflowLeftIndexSource.set(locationsToNull.get(ii), null);
        }
        // endregion nullOverflowObjectSources
    }
    // endmixin rehash

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

    private void fillKeys(ColumnSource.FillContext[] fillContexts, WritableChunk<Values>[] keyChunks, WritableLongChunk<KeyIndices> tableLocationsChunk) {
        fillKeys(keySources, fillContexts, keyChunks, tableLocationsChunk);
    }

    private void fillOverflowKeys(ColumnSource.FillContext[] fillContexts, WritableChunk<Values>[] keyChunks, WritableLongChunk<KeyIndices> overflowLocationsChunk) {
        fillKeys(overflowKeySources, fillContexts, keyChunks, overflowLocationsChunk);
    }

    private static void fillKeys(ArrayBackedColumnSource<?>[] keySources, ColumnSource.FillContext[] fillContexts, WritableChunk<Values>[] keyChunks, WritableLongChunk<KeyIndices> keyIndices) {
        for (int ii = 0; ii < keySources.length; ++ii) {
            keySources[ii].fillChunkUnordered(fillContexts[ii], keyChunks[ii], keyIndices);
        }
    }

    private void hashKeyChunks(WritableIntChunk<HashCode> hashChunk, Chunk<Values>[] sourceKeyChunks) {
        chunkHashers[0].hashInitial(sourceKeyChunks[0], hashChunk);
        for (int ii = 1; ii < sourceKeyChunks.length; ++ii) {
            chunkHashers[ii].hashUpdate(sourceKeyChunks[ii], hashChunk);
        }
    }

    private void getKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts, Chunk<? extends Values>[] chunks, OrderedKeys orderedKeys) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getChunk(contexts[ii], orderedKeys);
        }
    }

    // mixin prev
    private void getPrevKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts, Chunk<? extends Values>[] chunks, OrderedKeys orderedKeys) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getPrevChunk(contexts[ii], orderedKeys);
        }
    }
    // endmixin prev

    // region probe wrappers
    void removeLeft(final ProbeContext pc, OrderedKeys leftIndex, ColumnSource<?> [] leftSources)  {
        if (leftIndex.isEmpty()) {
            return;
        }
        decorationProbe(pc, leftIndex, leftSources, true, false, true, false, false, false, 0, null);
    }

    void removeRight(final ProbeContext pc, OrderedKeys rightIndex, ColumnSource<?> [] rightSources, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker)  {
        if (rightIndex.isEmpty()) {
            return;
        }
        decorationProbe(pc, rightIndex, rightSources, true, false, false, true, false, false, 0, modifiedSlotTracker);
    }


    void modifyByRight(final ProbeContext pc, Index modified, ColumnSource<?>[] rightSources, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (modified.isEmpty()) {
            return;
        }
        decorationProbe(pc, modified, rightSources, false, true, false, false, false, false, 0, modifiedSlotTracker);
    }

    void applyLeftShift(ProbeContext pc, ColumnSource<?>[] leftSources, Index shiftedIndex, long shiftDelta) {
        if (shiftedIndex.isEmpty()) {
            return;
        }
        decorationProbe(pc, shiftedIndex, leftSources, false, false, false, false, true, false, shiftDelta, null);
    }

    void applyRightShift(ProbeContext pc, ColumnSource<?> [] rightSources, Index shiftedIndex, long shiftDelta, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (shiftedIndex.isEmpty()) {
            return;
        }
        decorationProbe(pc, shiftedIndex, rightSources, false, false, false, false, false, true, shiftDelta, modifiedSlotTracker);
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
        final WritableIntChunk<HashCode> hashChunk;
        // the chunk of positions within our table
        final WritableLongChunk<KeyIndices> tableLocationsChunk;

        // the chunk of right indices that we read from the hash table, the empty right index is used as a sentinel that the
        // state exists; otherwise when building from the left it is always null
        // @WritableStateChunkType@ from WritableLongChunk<Values>
        final WritableLongChunk<Values> workingStateEntries;

        // the overflow locations that we need to get from the overflowLocationSource (or overflowOverflowLocationSource)
        final WritableLongChunk<KeyIndices> overflowLocationsToFetch;
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

        // the chunk of indices created from our OrderedKeys, used to write into the hash table
        final WritableLongChunk<OrderedKeyIndices> keyIndices;

        final LongArraySource pendingShifts = new LongArraySource();

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
            stateSourceFillContext = rightIndexSource.makeFillContext(chunkSize);
            overflowFillContext = overflowLocationSource.makeFillContext(chunkSize);
            overflowOverflowFillContext = overflowOverflowLocationSource.makeFillContext(chunkSize);
            hashChunk = WritableIntChunk.makeWritableChunk(chunkSize);
            tableLocationsChunk = WritableLongChunk.makeWritableChunk(chunkSize);
            // @WritableStateChunkName@ from WritableLongChunk
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

    ProbeContext makeProbeContext(ColumnSource<?>[] probeSources,
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
                                , OrderedKeys probeIndex
                                , final ColumnSource<?>[] probeSources
                                 // mixin prev
                                , boolean usePrev
                                 // endmixin prev
                                 // region additional probe arguments
                                , final boolean probeOnly
                                , final boolean removeLeft
                                , final boolean removeRight
                                , final boolean shiftLeft
                                , final boolean shiftRight
                                , final long shiftDelta
                                , final NaturalJoinModifiedSlotTracker modifiedSlotTracker
                                 // endregion additional probe arguments
    )  {
        // region probe start
        int pendingShiftPointer = 0;
        if (shiftDelta > 0) {
            pc.pendingShifts.ensureCapacity(probeIndex.size() * 2);
        }
        Assert.assertion(probeOnly ^ shiftLeft ^ shiftRight ^ removeLeft ^ removeRight, "probeOnly ^ shiftLeft ^ shiftRight ^ removeLeft ^ removeRight");
        if (shiftLeft || shiftRight) {
            Assert.neqZero(shiftDelta, "shiftDelta");
        }
        final byte flag;
        if (probeOnly) {
            flag = NaturalJoinModifiedSlotTracker.FLAG_RIGHT_MODIFY_PROBE;
        } else if (shiftRight) {
            flag = NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT;
        } else if (removeRight) {
            flag = NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE;
        } else if (shiftLeft || removeLeft) {
            Assert.eqNull(modifiedSlotTracker, "modifiedSlotTracker");
            flag = 0;
        } else {
            throw Assert.statementNeverExecuted();
        }
        // endregion probe start
        long hashSlotOffset = 0;

        try (final OrderedKeys.Iterator okIt = probeIndex.getOrderedKeysIterator();
             // region probe additional try resources
             // endregion probe additional try resources
            ) {
            //noinspection unchecked
            final Chunk<Values> [] sourceKeyChunks = new Chunk[keyColumnCount];

            // region probe initialization
            // endregion probe initialization

            while (okIt.hasMore()) {
                // we reset shared contexts early to avoid carrying around state that can't be reused.
                pc.resetSharedContexts();
                final OrderedKeys chunkOk = okIt.getNextOrderedKeysWithLength(pc.chunkSize);
                final int chunkSize = chunkOk.intSize();

                // region probe loop initialization
                chunkOk.fillKeyIndicesChunk(pc.keyIndices);
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
                rightIndexSource.fillChunkUnordered(pc.stateSourceFillContext, pc.workingStateEntries, pc.tableLocationsChunk);

                // @StateChunkIdentityName@ from LongChunk
                LongChunkEquals.notEqual(pc.workingStateEntries, EMPTY_RIGHT_VALUE, pc.equalValues);
                checkKeyEquality(pc.equalValues, pc.workingKeyChunks, sourceKeyChunks);

                pc.overflowPositionInWorkingChunk.setSize(0);
                pc.overflowLocationsToFetch.setSize(0);

                for (int ii = 0; ii < pc.equalValues.size(); ++ii) {
                    if (pc.equalValues.get(ii)) {
                        // region probe main found
                        final long tableLocation = pc.tableLocationsChunk.get(ii);
                        addModifiedMain(modifiedSlotTracker, tableLocation, flag);

                        if (probeOnly) {
                            continue;
                        }

                        final long indexKey = pc.keyIndices.get(ii);
                        if (removeLeft) {
                            leftIndexSource.get(tableLocation).remove(indexKey);
                        } else if (removeRight) {
                            removeRightIndex(tableLocation, indexKey);
                        } else if (shiftLeft) {
                            if (shiftDelta < 0) {
                                shiftLeftIndex(tableLocation, indexKey, shiftDelta);
                            } else {
                                pc.pendingShifts.set(pendingShiftPointer++, tableLocation);
                                pc.pendingShifts.set(pendingShiftPointer++, indexKey);
                            }
                        } else if (shiftRight) {
                            if (shiftDelta < 0) {
                                shiftRightIndex(tableLocation, indexKey, shiftDelta);
                            } else {
                                pc.pendingShifts.set(pendingShiftPointer++, tableLocation);
                                pc.pendingShifts.set(pendingShiftPointer++, indexKey);
                            }
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
                            addModifiedOverflow(modifiedSlotTracker, overflowLocation, flag);

                            if (probeOnly) {
                                continue;
                            }

                            final long indexKey = pc.keyIndices.get(chunkPosition);

                            if (removeLeft) {
                                overflowLeftIndexSource.get(overflowLocation).remove(indexKey);
                            } else if (removeRight) {
                                removeRightIndexOverflow(overflowLocation, indexKey);
                            } else if (shiftLeft) {
                                if (shiftDelta < 0) {
                                    shiftLeftIndexOverflow(overflowLocation, indexKey, shiftDelta);
                                } else {
                                    pc.pendingShifts.set(pendingShiftPointer++, overflowLocationToHashLocation(overflowLocation));
                                    pc.pendingShifts.set(pendingShiftPointer++, indexKey);
                                }
                            } else if (shiftRight) {
                                if (shiftDelta < 0) {
                                    shiftRightIndexOverflow(overflowLocation, indexKey, shiftDelta);
                                } else {
                                    pc.pendingShifts.set(pendingShiftPointer++, overflowLocationToHashLocation(overflowLocation));
                                    pc.pendingShifts.set(pendingShiftPointer++, indexKey);
                                }
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
        if (pendingShiftPointer > 0) {
            if (shiftLeft) {
                for (int ii = pendingShiftPointer - 2; ii >= 0; ii -= 2) {
                    final long slot = pc.pendingShifts.getUnsafe(ii);
                    final long indexKey = pc.pendingShifts.getUnsafe(ii + 1);
                    if (isOverflowLocation(slot)) {
                        shiftLeftIndexOverflow(hashLocationToOverflowLocation(slot), indexKey, shiftDelta);
                    } else {
                        shiftLeftIndex(slot, indexKey, shiftDelta);
                    }
                }
            } else {
                for (int ii = pendingShiftPointer - 2; ii >= 0; ii -= 2) {
                    final long slot = pc.pendingShifts.getUnsafe(ii);
                    final long indexKey = pc.pendingShifts.getUnsafe(ii + 1);
                    if (isOverflowLocation(slot)) {
                        shiftRightIndexOverflow(hashLocationToOverflowLocation(slot), indexKey, shiftDelta);
                    } else {
                        shiftRightIndex(slot, indexKey, shiftDelta);
                    }
                }
            }
        }
        // endregion probe final
    }
    // endmixin decorationProbe

    private void convertHashToTableLocations(WritableIntChunk<HashCode> hashChunk, WritableLongChunk<KeyIndices> tablePositionsChunk) {
        // mixin rehash
        // NOTE that this mixin section is a bit ugly, we are spanning the two functions so that we can avoid using tableHashPivot and having the unused pivotPoint parameter
        convertHashToTableLocations(hashChunk, tablePositionsChunk, tableHashPivot);
    }

    private void convertHashToTableLocations(WritableIntChunk<HashCode> hashChunk, WritableLongChunk<KeyIndices> tablePositionsChunk, int pivotPoint) {
        // endmixin rehash

        // turn hash codes into indices within our table
        for (int ii = 0; ii < hashChunk.size(); ++ii) {
            final int hash = hashChunk.get(ii);
            // mixin rehash
            final int location = hashToTableLocation(pivotPoint, hash);
            // endmixin rehash
            // altmixin rehash: final int location = hashToTableLocation(hash);
            tablePositionsChunk.set(ii, location);
        }
        tablePositionsChunk.setSize(hashChunk.size());
    }

    private int hashToTableLocation(
            // mixin rehash
            int pivotPoint,
            // endmixin rehash
            int hash) {
        // altmixin rehash: final \
        int location = hash & (tableSize - 1);
        // mixin rehash
        if (location >= pivotPoint) {
            location -= (tableSize >> 1);
        }
        // endmixin rehash
        return location;
    }

    // region extraction functions
    RedirectionIndex buildRedirectionIndexFromRedirections(QueryTable leftTable, boolean exactMatch, LongArraySource leftRedirections, JoinControl.RedirectionType redirectionType) {
        return buildRedirectionIndex(leftTable, exactMatch, leftRedirections::getUnsafe, redirectionType);
    }

    static long overflowToSlot(long overflow) {
        return overflowLocationToHashLocation(overflow);
    }

    @Override
    public Index getLeftIndex(long slot) {
        if (isOverflowLocation(slot)) {
            return overflowLeftIndexSource.get(hashLocationToOverflowLocation(slot));
        } else {
            return leftIndexSource.get(slot);
        }
    }

    private long getResultRightIndex(long hashSlot) {
        final long rightIndex = getRightIndex(hashSlot);
        if (rightIndex == DUPLICATE_RIGHT_VALUE) {
            throw new IllegalStateException("Duplicate right key for " + keyString(hashSlot));
        }
        return rightIndex;
    }

    @Override
    public long getRightIndex(long slot) {
        final long rightIndex;
        if (isOverflowLocation(slot)) {
            rightIndex = overflowRightIndexSource.getUnsafe(hashLocationToOverflowLocation(slot));
        } else {
            rightIndex = rightIndexSource.getUnsafe(slot);
        }

        if (isDuplicateRightIndex(rightIndex)) {
            return DUPLICATE_RIGHT_VALUE;
        }

        return rightIndex;
    }

    @Override
    public String keyString(long slot) {
        final WritableChunk<Values>[] keyChunk = getWritableKeyChunks(1);
        try (final WritableLongChunk<KeyIndices> slotChunk = WritableLongChunk.makeWritableChunk(1)) {
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
    private long getStateValue(LongArraySource hashSlots, long locationInHashSlots) {
        final long hashSlot = hashSlots.getUnsafe(locationInHashSlots);
        if (isOverflowLocation(hashSlot)) {
            return overflowRightIndexSource.getUnsafe(hashLocationToOverflowLocation(hashSlot));
        }
        else {
            return rightIndexSource.getUnsafe(hashSlot);
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

    // mixin dumpTable
    @SuppressWarnings("unused")
    private void dumpTable() {
        dumpTable(true, true);
    }

    @SuppressWarnings("SameParameterValue")
    private void dumpTable(boolean showBadMain, boolean showBadOverflow) {
        //noinspection unchecked
        final WritableChunk<Values> [] dumpChunks = new WritableChunk[keyColumnCount];
        //noinspection unchecked
        final WritableChunk<Values> [] overflowDumpChunks = new WritableChunk[keyColumnCount];
        // @WritableStateChunkType@ from WritableLongChunk<Values>, @WritableStateChunkName@ from WritableLongChunk
        final WritableLongChunk<Values> stateChunk = WritableLongChunk.makeWritableChunk(tableSize);
        final WritableIntChunk<Values> overflowLocationChunk = WritableIntChunk.makeWritableChunk(tableSize);
        // @WritableStateChunkType@ from WritableLongChunk<Values>, @WritableStateChunkName@ from WritableLongChunk
        final WritableLongChunk<Values> overflowStateChunk = WritableLongChunk.makeWritableChunk(nextOverflowLocation);
        final WritableIntChunk<Values> overflowOverflowLocationChunk = WritableIntChunk.makeWritableChunk(nextOverflowLocation);

        final WritableIntChunk<HashCode> hashChunk = WritableIntChunk.makeWritableChunk(tableSize);
        final WritableIntChunk<HashCode> overflowHashChunk = WritableIntChunk.makeWritableChunk(nextOverflowLocation);


        final OrderedKeys tableLocations = Index.FACTORY.getIndexByRange(0, tableSize - 1);
        final OrderedKeys overflowLocations = nextOverflowLocation > 0 ? Index.FACTORY.getIndexByRange(0, nextOverflowLocation - 1) : OrderedKeys.EMPTY;

        for (int ii = 0; ii < keyColumnCount; ++ii) {
            dumpChunks[ii] = keyChunkTypes[ii].makeWritableChunk(tableSize);
            overflowDumpChunks[ii] = keyChunkTypes[ii].makeWritableChunk(nextOverflowLocation);

            try (final ColumnSource.FillContext fillContext = keySources[ii].makeFillContext(tableSize)) {
                keySources[ii].fillChunk(fillContext, dumpChunks[ii], tableLocations);
            }
            try (final ColumnSource.FillContext fillContext = overflowKeySources[ii].makeFillContext(nextOverflowLocation)) {
                overflowKeySources[ii].fillChunk(fillContext, overflowDumpChunks[ii], overflowLocations);
            }

            if (ii == 0) {
                chunkHashers[ii].hashInitial(dumpChunks[ii], hashChunk);
                chunkHashers[ii].hashInitial(overflowDumpChunks[ii], overflowHashChunk);
            } else {
                chunkHashers[ii].hashUpdate(dumpChunks[ii], hashChunk);
                chunkHashers[ii].hashUpdate(overflowDumpChunks[ii], overflowHashChunk);
            }
        }

        try (final ColumnSource.FillContext fillContext = rightIndexSource.makeFillContext(tableSize)) {
            rightIndexSource.fillChunk(fillContext, stateChunk, tableLocations);
        }
        try (final ColumnSource.FillContext fillContext = overflowLocationSource.makeFillContext(tableSize)) {
            overflowLocationSource.fillChunk(fillContext, overflowLocationChunk, tableLocations);
        }
        try (final ColumnSource.FillContext fillContext = overflowOverflowLocationSource.makeFillContext(nextOverflowLocation)) {
            overflowOverflowLocationSource.fillChunk(fillContext, overflowOverflowLocationChunk, overflowLocations);
        }
        try (final ColumnSource.FillContext fillContext = overflowRightIndexSource.makeFillContext(nextOverflowLocation)) {
            overflowRightIndexSource.fillChunk(fillContext, overflowStateChunk, overflowLocations);
        }

        for (int ii = 0; ii < tableSize; ++ii) {
            System.out.print("Bucket[" + ii + "] ");
            // @StateValueType@ from long
            final long stateValue = stateChunk.get(ii);
            int overflowLocation = overflowLocationChunk.get(ii);

            if (stateValue == EMPTY_RIGHT_VALUE) {
                System.out.println("<empty>");
            } else {
                final int hashCode = hashChunk.get(ii);
                System.out.println(ChunkUtils.extractKeyStringFromChunks(keyChunkTypes, dumpChunks, ii) + " (hash=" + hashCode + ") = " + stateValue);
                final int expectedPosition = hashToTableLocation(tableHashPivot, hashCode);
                if (showBadMain && expectedPosition != ii) {
                    System.out.println("***** BAD HASH POSITION **** (computed " + expectedPosition + ")");
                }
            }

            while (overflowLocation != QueryConstants.NULL_INT) {
                // @StateValueType@ from long
                final long overflowStateValue = overflowStateChunk.get(overflowLocation);
                final int overflowHashCode = overflowHashChunk.get(overflowLocation);
                System.out.println("\tOF[" + overflowLocation + "] " + ChunkUtils.extractKeyStringFromChunks(keyChunkTypes, overflowDumpChunks, overflowLocation) + " (hash=" + overflowHashCode + ") = " + overflowStateValue);
                final int expectedPosition = hashToTableLocation(tableHashPivot, overflowHashCode);
                if (showBadOverflow && expectedPosition != ii) {
                    System.out.println("***** BAD HASH POSITION FOR OVERFLOW **** (computed " + expectedPosition + ")");
                }
                overflowLocation = overflowOverflowLocationChunk.get(overflowLocation);
            }
        }
    }
    // endmixin dumpTable

    static int hashTableSize(long initialCapacity) {
        return (int)Math.max(MINIMUM_INITIAL_HASH_SIZE, Math.min(MAX_TABLE_SIZE, Long.highestOneBit(initialCapacity) * 2));
    }

}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.v2;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rftable.ChunkSource;
import io.deephaven.engine.rftable.Context;
import io.deephaven.engine.rftable.SharedContext;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.structures.rowsequence.RowSequenceUtil;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.v2.hashing.*;
// this is ugly to have twice, but we do need it twice for replication
// @StateChunkIdentityName@ from \QObjectChunkIdentity\E
import io.deephaven.engine.v2.hashing.ObjectChunkIdentityEquals;
import io.deephaven.engine.v2.sort.permute.PermuteKernel;
import io.deephaven.engine.v2.sort.timsort.LongIntTimsortKernel;
import io.deephaven.engine.v2.sources.*;
import io.deephaven.engine.chunk.*;
import io.deephaven.engine.chunk.Attributes.*;
import io.deephaven.engine.v2.utils.*;

// mixin rehash
import java.util.Arrays;
import io.deephaven.engine.v2.sort.permute.IntPermuteKernel;
// @StateChunkTypeEnum@ from \QObject\E
import io.deephaven.engine.v2.sort.permute.ObjectPermuteKernel;
import io.deephaven.engine.v2.utils.compact.IntCompactKernel;
import io.deephaven.engine.v2.utils.compact.LongCompactKernel;
// endmixin rehash

import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;

// region extra imports
// endregion extra imports

import static io.deephaven.util.SafeCloseable.closeArray;

// region class visibility
/**
 * This is our JoinStateManager for cross join when right is static and left is ticking.
 */
// endregion class visibility
class LeftOnlyIncrementalChunkedCrossJoinStateManager
        // region extensions
        extends CrossJoinShiftState
        implements CrossJoinStateManager
    // endregion extensions
{
    // region constants
    private static final int CHUNK_SIZE = 4096;
    private static final int MINIMUM_INITIAL_HASH_SIZE = CHUNK_SIZE;
    private static final long MAX_TABLE_SIZE = 1L << 30;
    // endregion constants

    // mixin rehash
    static final double DEFAULT_MAX_LOAD_FACTOR = 0.75;
    static final double DEFAULT_TARGET_LOAD_FACTOR = 0.70;
    // endmixin rehash

    // region preamble variables
    @FunctionalInterface
    interface StateTrackingCallback {

        /**
         * Invoke a callback that will allow external trackers to record changes to states in build or probe calls.
         *
         * @param stateSlot The state slot (in main table space)
         * @param index     The probed rowSet key
         */
        void invoke(long stateSlot, long index);
    }
    // endregion preamble variables

    @ReplicateHashTable.EmptyStateValue
    // @NullStateValue@ from \Qnull\E, @StateValueType@ from \QTrackingMutableRowSet\E
    private static final TrackingMutableRowSet EMPTY_RIGHT_VALUE = null;

    // mixin getStateValue
    // region overflow pivot
    private static final long OVERFLOW_PIVOT_VALUE = -2;
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
    // @StateColumnSourceType@ from \QObjectArraySource<TrackingMutableRowSet>\E
    private final ObjectArraySource<TrackingMutableRowSet> rightIndexSource
            // @StateColumnSourceConstructor@ from \QObjectArraySource<>(TrackingMutableRowSet.class)\E
            = new ObjectArraySource<>(TrackingMutableRowSet.class);

    // the keys for overflow
    private int nextOverflowLocation = 0;
    private final ArrayBackedColumnSource<?> [] overflowKeySources;
    // the location of the next key in an overflow bucket
    private final IntegerArraySource overflowOverflowLocationSource = new IntegerArraySource();
    // the overflow buckets for the right TrackingMutableRowSet
    @ReplicateHashTable.OverflowStateColumnSource
    // @StateColumnSourceType@ from \QObjectArraySource<TrackingMutableRowSet>\E
    private final ObjectArraySource<TrackingMutableRowSet> overflowRightIndexSource
            // @StateColumnSourceConstructor@ from \QObjectArraySource<>(TrackingMutableRowSet.class)\E
            = new ObjectArraySource<>(TrackingMutableRowSet.class);

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
    // maintain a mapping from left rowSet to its slot
    private final MutableRowRedirection leftIndexToSlot;
    private final ColumnSource<?>[] leftKeySources;
    private final long EMPTY_RIGHT_SLOT = Long.MIN_VALUE;
    private final QueryTable leftTable;
    private long maxRightGroupSize = 0;
    // endregion extra variables

    LeftOnlyIncrementalChunkedCrossJoinStateManager(ColumnSource<?>[] tableKeySources
                                         , int tableSize
                                                    // region constructor arguments
            , QueryTable leftTable
            , int numRightBitsToReserve
                                              // endregion constructor arguments
    ) {
        // region super
        super(numRightBitsToReserve);
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
        leftIndexToSlot = MutableRowRedirection.FACTORY.createRowRedirection(tableSize);
        this.leftKeySources = tableKeySources;
        this.leftTable = leftTable;
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
        // endregion ensureOverflowCapacity
    }

    // region build wrappers
    @NotNull
    MutableRowSet buildLeftTicking(@NotNull final QueryTable leftTable,
                                   @NotNull final QueryTable rightTable,
                                   @NotNull final ColumnSource<?>[] rightKeys) {
        final boolean ignoreMissing = false;

        if (!rightTable.isEmpty()) {
            try (final BuildContext bc = makeBuildContext(rightKeys, rightTable.size())) {
                buildTable(bc, rightTable.getRowSet(), rightKeys, (slot, index) -> {
                    if (isOverflowLocation(slot)) {
                        addToIndex(overflowRightIndexSource, hashLocationToOverflowLocation(slot), index, ignoreMissing);
                    } else {
                        addToIndex(rightIndexSource, slot, index, ignoreMissing);
                    }
                });
            }
        }

        // We can now validate key-space after all of our right rows have been aggregated into groups, which determined
        // how many bits we need for the right indexes.
        validateKeySpaceSize();

        final RowSetBuilderRandom resultIndex = RowSetFactory.builderRandom();
        if (!leftTable.isEmpty()) {
            addLeft(leftTable.getRowSet(), (slot, index) -> {
                final long regionStart = index << getNumShiftBits();
                final RowSet rightRowSet = getRightIndex(slot);
                resultIndex.addRange(regionStart, regionStart + rightRowSet.size() - 1);
            });
        }

        return resultIndex.build();
    }

    void removeLeft(final RowSet leftToRemove, final StateTrackingCallback trackingCallback) {
        if (leftToRemove.isEmpty()) {
            return;
        }
        final boolean usePrev = true;
        try (final ProbeContext pc = makeProbeContext(leftKeySources, leftToRemove.size())) {
            decorationProbe(pc, leftToRemove, leftKeySources, usePrev, null, (stateSlot, leftKey) -> {
                leftIndexToSlot.removeVoid(leftKey);
                trackingCallback.invoke(stateSlot, leftKey);
            });
        }
    }

    void addLeft(final RowSet leftToAdd, final StateTrackingCallback trackingCallback) {
        if (leftToAdd.isEmpty()) {
            return;
        }
        final boolean usePrev = false;
        try (final ProbeContext pc = makeProbeContext(leftKeySources, leftToAdd.size())) {
            decorationProbe(pc, leftToAdd, leftKeySources, usePrev, () -> {
                for (int ii = 0; ii < pc.keyIndices.size(); ++ii) {
                    final long index = pc.keyIndices.get(ii);
                    final long slot = pc.mappedSlots.get(ii);
                    final RowSet rightRowSet = getRightIndex(slot);
                    if (rightRowSet.isNonempty()) {
                        leftIndexToSlot.putVoid(index, slot);
                        trackingCallback.invoke(slot, index);
                    }
                }
            }, null);
        }
    }

    void applyLeftShift(final RowSet prevLeftIndex, final RowSetShiftData shiftData) {
        leftIndexToSlot.applyShift(prevLeftIndex, shiftData);
    }

    void processLeftModifies(final Listener.Update upstream, final Listener.Update downstream, final MutableRowSet resultRowSet) {
        if (upstream.modified.isEmpty()) {
            downstream.modified = RowSetFactory.empty();
            return;
        }
        final boolean usePrev = false;
        final RowSetBuilderSequential addBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential rmBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential modBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential rmResultBuilder = RowSetFactory.builderSequential();
        try (final ProbeContext pc = makeProbeContext(leftKeySources, upstream.modified.size())) {
            final RowSet.Iterator it = upstream.modified.iterator();
            final RowSet.Iterator pit = upstream.getModifiedPreShift().iterator();

            decorationProbe(pc, upstream.modified, leftKeySources, usePrev, () -> {
                for (int ii = 0; ii < pc.keyIndices.size(); ++ii) {
                    final long currKey = it.nextLong();
                    final long prevKey = pit.nextLong();
                    final long prevSlot = leftIndexToSlot.getPrev(prevKey);
                    final long currSlot = pc.mappedSlots.get(ii);
                    final long regionStart = currKey << getNumShiftBits();
                    final RowSet currRowSet = getRightIndex(currSlot);
                    if (prevSlot == currSlot && currRowSet.isNonempty()) {
                        modBuilder.appendRange(regionStart, regionStart + currRowSet.size() - 1);
                    } else if (prevSlot != currSlot) {
                        final RowSet prevRowSet = getRightIndex(prevSlot);
                        final long prevRegionStart = prevKey << getNumShiftBits();
                        if (currSlot == EMPTY_RIGHT_SLOT) {
                            leftIndexToSlot.removeVoid(currKey);
                        } else {
                            leftIndexToSlot.putVoid(currKey, currSlot);
                        }

                        if (prevRowSet.isNonempty()) {
                            // note: removes are in old key space, but we have already shifted our resultRowSet after processing upstream removals
                            rmBuilder.appendRange(prevRegionStart, prevRegionStart + prevRowSet.size() - 1);
                            rmResultBuilder.appendRange(regionStart, regionStart + prevRowSet.size() - 1);
                        }
                        if (currRowSet.isNonempty()) {
                            addBuilder.appendRange(regionStart, regionStart + currRowSet.size() - 1);
                        }
                    }
                }
            }, null);
        }
        try (final RowSet added = addBuilder.build();
             final RowSet removed = rmBuilder.build();
             final RowSet postShiftRemoved = rmResultBuilder.build()) {
            downstream.removed.mutableCast().insert(removed);
            downstream.added.mutableCast().insert(added);
            // must remove before adding as removed.intersect(added) may be non-empty
            resultRowSet.remove(postShiftRemoved);
            resultRowSet.insert(added);
        }
        downstream.modified = modBuilder.build();
    }

    private void addToIndex(final ObjectArraySource<TrackingMutableRowSet> source, final long location, final long keyToAdd, boolean ignoreMissing) {
        final long size;
        final MutableRowSet rowSet = source.get(location);
        if (rowSet == null) {
            if (ignoreMissing) {
                return;
            }

            source.set(location, RowSetFactory.fromKeys(keyToAdd).toTracking());
            size = 1;
        } else {
            rowSet.insert(keyToAdd);
            size = rowSet.size();
        }

        final int numBitsNeeded = CrossJoinShiftState.getMinBits(size - 1);
        if (numBitsNeeded > getNumShiftBits()) {
            setNumShiftBits(numBitsNeeded);
        }
        if (size > maxRightGroupSize) {
            maxRightGroupSize = size;
        }
    }

    void startTrackingPrevValues() {
        leftIndexToSlot.startTrackingPrevValues();
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
        // @WritableStateChunkType@ from \QWritableObjectChunk<TrackingMutableRowSet,Values>\E
        final WritableObjectChunk<TrackingMutableRowSet,Values> workingStateEntries;

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
        final WritableLongChunk<RowKeys> rehashLocations;
        final WritableIntChunk<Values> overflowLocationsToMigrate;
        final WritableLongChunk<RowKeys> overflowLocationsAsKeyIndices;
        final WritableBooleanChunk<Any> shouldMoveBucket;

        final ResettableWritableLongChunk<Any> overflowLocationForPromotionLoop = ResettableWritableLongChunk.makeResettableChunk();

        // mixin allowUpdateWriteThroughState
        // @WritableStateChunkType@ from \QWritableObjectChunk<TrackingMutableRowSet,Values>\E, @WritableStateChunkName@ from \QWritableObjectChunk\E
        final ResettableWritableObjectChunk<TrackingMutableRowSet,Values> writeThroughState = ResettableWritableObjectChunk.makeResettableChunk();
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
        final WritableLongChunk<OrderedRowKeys> sourceIndexKeys;
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
            // @WritableStateChunkName@ from \QWritableObjectChunk\E
            workingStateEntries = WritableObjectChunk.makeWritableChunk(chunkSize);
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
                            final RowSequence buildIndex,
                            ColumnSource<?>[] buildSources
                            // region extra build arguments
            , final StateTrackingCallback trackingCallback
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
                chunkOk.fillRowKeyChunk(bc.sourceIndexKeys);
                // endregion build loop initialization

                // turn hash codes into indices within our table
                convertHashToTableLocations(bc.hashChunk, bc.tableLocationsChunk);

                // now fetch the values from the table, note that we do not order these fetches
                fillKeys(bc.workingFillContexts, bc.workingKeyChunks, bc.tableLocationsChunk);

                // and the corresponding states, if a value is null, we've found our insertion point
                rightIndexSource.fillChunkUnordered(bc.stateSourceFillContext, bc.workingStateEntries, bc.tableLocationsChunk);

                // find things that exist
                // @StateChunkIdentityName@ from \QObjectChunkIdentity\E
                ObjectChunkIdentityEquals.notEqual(bc.workingStateEntries, EMPTY_RIGHT_VALUE, bc.filledValues);

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
                        trackingCallback.invoke(tableLocation, keyToAdd);
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
                    trackingCallback.invoke(currentHashLocation, keyToAdd);
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
                        trackingCallback.invoke(tableLocation, keyToAdd);
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
                                trackingCallback.invoke(overflowLocationToHashLocation(overflowLocation), keyToAdd);
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
                            final long keyToAdd = bc.sourceIndexKeys.get(chunkPosition);
                            trackingCallback.invoke(overflowLocationToHashLocation(allocatedOverflowLocation), keyToAdd);
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
                                trackingCallback.invoke(overflowLocationToHashLocation(insertedOverflowLocation), keyToAdd);
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
                    doRehash(bc);
                    // endregion post-build rehash
                    // endmixin rehash
                }

                // region copy hash slots
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
            overflowLocationSource.fillChunk(bc.overflowFillContext, bc.overflowLocations, RowSequenceUtil.wrapRowKeysChunkAsRowSequence(LongChunk.downcast(bc.rehashLocations)));
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
            // @StateChunkIdentityName@ from \QObjectChunkIdentity\E
            ObjectChunkIdentityEquals.notEqual(bc.workingStateEntries, EMPTY_RIGHT_VALUE, bc.shouldMoveBucket);

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

                    // @StateValueType@ from \QTrackingMutableRowSet\E
                    final TrackingMutableRowSet stateValueToMove = rightIndexSource.getUnsafe(oldHashLocation);
                    rightIndexSource.set(newHashLocation, stateValueToMove);
                    rightIndexSource.set(oldHashLocation, EMPTY_RIGHT_VALUE);
                                // region rehash move values
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

                overflowLocationSource.fillChunk(bc.overflowFillContext, bc.overflowLocations, RowSequenceUtil.wrapRowKeysChunkAsRowSequence(bc.overflowLocationForPromotionLoop));
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
                                // @StateChunkTypeEnum@ from \QObject\E
                                ObjectPermuteKernel.permute(bc.sourcePositions, bc.workingStateEntries, bc.destinationLocationPositionInWriteThrough, bc.writeThroughState);
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
                        // endregion promotion move
                    }
                }

                // the permutes are completing the state and overflow promotions write through
                // mixin allowUpdateWriteThroughState
                // @StateChunkTypeEnum@ from \QObject\E
                ObjectPermuteKernel.permute(bc.sourcePositions, bc.workingStateEntries, bc.destinationLocationPositionInWriteThrough, bc.writeThroughState);
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

        try (final WritableLongChunk<RowKeys> positions = WritableLongChunk.makeWritableChunk(maxSize);
             final WritableBooleanChunk exists = WritableBooleanChunk.makeWritableChunk(maxSize);
             final WritableIntChunk hashChunk = WritableIntChunk.makeWritableChunk(maxSize);
             final WritableLongChunk<RowKeys> tableLocationsChunk = WritableLongChunk.makeWritableChunk(maxSize);
             final SafeCloseableArray ignored = new SafeCloseableArray<>(keyFillContext);
             final SafeCloseableArray ignored2 = new SafeCloseableArray<>(keyChunks);
             // @StateChunkName@ from \QObjectChunk\E
             final WritableObjectChunk stateChunk = WritableObjectChunk.makeWritableChunk(maxSize);
             final ChunkSource.FillContext fillContext = rightIndexSource.makeFillContext(maxSize)) {

            rightIndexSource.fillChunk(fillContext, stateChunk, RowSetFactory.flat(tableHashPivot));

            ChunkUtils.fillInOrder(positions);

            // @StateChunkIdentityName@ from \QObjectChunkIdentity\E
            ObjectChunkIdentityEquals.notEqual(stateChunk, EMPTY_RIGHT_VALUE, exists);

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

    private void createOverflowPartitions(WritableLongChunk<RowKeys> overflowLocationsToFetch, WritableLongChunk<RowKeys> rehashLocations, WritableBooleanChunk<Any> shouldMoveBucket, int moves) {
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

    private void initializeRehashLocations(WritableLongChunk<RowKeys> rehashLocations, int bucketsToAdd) {
        rehashLocations.setSize(bucketsToAdd);
        for (int ii = 0; ii < bucketsToAdd; ++ii) {
            rehashLocations.set(ii, tableHashPivot + ii - (tableSize >> 1));
        }
    }

    private void compactOverflowLocations(IntChunk<Values> overflowLocations, WritableLongChunk<RowKeys> overflowLocationsToFetch) {
        overflowLocationsToFetch.setSize(0);
        for (int ii = 0; ii < overflowLocations.size(); ++ii) {
            final int overflowLocation = overflowLocations.get(ii);
            if (overflowLocation != QueryConstants.NULL_INT) {
                overflowLocationsToFetch.add(overflowLocation);
            }
        }
    }

    private void swapOverflowPointers(LongChunk<RowKeys> tableLocationsChunk, LongChunk<RowKeys> overflowLocationsToFetch) {
        for (int ii = 0; ii < overflowLocationsToFetch.size(); ++ii) {
            final long newLocation = tableLocationsChunk.get(ii);
            final int existingOverflow = overflowLocationSource.getUnsafe(newLocation);
            final long overflowLocation = overflowLocationsToFetch.get(ii);
            overflowOverflowLocationSource.set(overflowLocation, existingOverflow);
            overflowLocationSource.set(newLocation, (int)overflowLocation);
        }
    }

    // mixin allowUpdateWriteThroughState
    // @WritableStateChunkType@ from \QWritableObjectChunk<TrackingMutableRowSet,Values>\E
    private void updateWriteThroughState(ResettableWritableObjectChunk<TrackingMutableRowSet,Values> writeThroughState, long firstPosition, long expectedLastPosition) {
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
            overflowRightIndexSource.set(locationsToNull.get(ii), null);
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

    private void hashKeyChunks(WritableIntChunk<HashCode> hashChunk, Chunk<Values>[] sourceKeyChunks) {
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
        final WritableLongChunk<RowKeys> tableLocationsChunk;

        // the chunk of right indices that we read from the hash table, the empty right rowSet is used as a sentinel that the
        // state exists; otherwise when building from the left it is always null
        // @WritableStateChunkType@ from \QWritableObjectChunk<TrackingMutableRowSet,Values>\E
        final WritableObjectChunk<TrackingMutableRowSet,Values> workingStateEntries;

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
        WritableLongChunk<Values> mappedSlots;

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
            // @WritableStateChunkName@ from \QWritableObjectChunk\E
            workingStateEntries = WritableObjectChunk.makeWritableChunk(chunkSize);
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
            if (mappedSlots != null) {
                mappedSlots.close();
            }
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
                                , RowSequence probeIndex
                                , final ColumnSource<?>[] probeSources
                                 // mixin prev
                                , boolean usePrev
                                 // endmixin prev
                                 // region additional probe arguments
            , final Runnable endProbeCallback
            , final StateTrackingCallback trackingCallback
                                 // endregion additional probe arguments
    )  {
        // region probe start
        int numProbeAccumulatorsFound = 0;
        if (endProbeCallback != null) {
            ++numProbeAccumulatorsFound;
            pc.mappedSlots = WritableLongChunk.makeWritableChunk(pc.chunkSize);
        }
        if (trackingCallback != null) {
            ++numProbeAccumulatorsFound;
        }
        Assert.eq(1, "1", numProbeAccumulatorsFound, "numProbeAccumulatorsFound");
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
                rightIndexSource.fillChunkUnordered(pc.stateSourceFillContext, pc.workingStateEntries, pc.tableLocationsChunk);

                // @StateChunkIdentityName@ from \QObjectChunkIdentity\E
                ObjectChunkIdentityEquals.notEqual(pc.workingStateEntries, EMPTY_RIGHT_VALUE, pc.equalValues);
                checkKeyEquality(pc.equalValues, pc.workingKeyChunks, sourceKeyChunks);

                pc.overflowPositionInWorkingChunk.setSize(0);
                pc.overflowLocationsToFetch.setSize(0);

                for (int ii = 0; ii < pc.equalValues.size(); ++ii) {
                    if (pc.equalValues.get(ii)) {
                        // region probe main found
                        final long tableLocation = pc.tableLocationsChunk.get(ii);
                        if (endProbeCallback != null) {
                            pc.mappedSlots.set(ii, tableLocation);
                        } else if (trackingCallback != null) {
                            trackingCallback.invoke(tableLocation, pc.keyIndices.get(ii));
                        }
                        // endregion probe main found
                    } else if (pc.workingStateEntries.get(ii) != EMPTY_RIGHT_VALUE) {
                        // we must handle this as part of the overflow bucket
                        pc.overflowPositionInWorkingChunk.add(ii);
                        pc.overflowLocationsToFetch.add(pc.tableLocationsChunk.get(ii));
                    } else {
                        // region probe main not found
                        if (endProbeCallback != null) {
                            pc.mappedSlots.set(ii, EMPTY_RIGHT_SLOT);
                        }
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
                            if (endProbeCallback != null) {
                                pc.mappedSlots.set(chunkPosition, EMPTY_RIGHT_SLOT);
                            }
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
                            final long indexKey = pc.keyIndices.get(chunkPosition);
                            final long hashLocation = overflowLocationToHashLocation(overflowLocation);
                            if (endProbeCallback != null) {
                                pc.mappedSlots.set(chunkPosition, hashLocation);
                            } else {
                                trackingCallback.invoke(hashLocation, indexKey);
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
                if (endProbeCallback != null) {
                    pc.mappedSlots.setSize(chunkSize);
                    endProbeCallback.run();
                }
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

    private void convertHashToTableLocations(WritableIntChunk<HashCode> hashChunk, WritableLongChunk<RowKeys> tablePositionsChunk) {
        // mixin rehash
        // NOTE that this mixin section is a bit ugly, we are spanning the two functions so that we can avoid using tableHashPivot and having the unused pivotPoint parameter
        convertHashToTableLocations(hashChunk, tablePositionsChunk, tableHashPivot);
    }

    private void convertHashToTableLocations(WritableIntChunk<HashCode> hashChunk, WritableLongChunk<RowKeys> tablePositionsChunk, int pivotPoint) {
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
    public TrackingRowSet getRightIndex(long slot) {
        TrackingRowSet retVal;
        if (isOverflowLocation(slot)) {
            retVal = overflowRightIndexSource.get(hashLocationToOverflowLocation(slot));
        } else {
            retVal = rightIndexSource.get(slot);
        }
        if (retVal == null) {
            retVal = RowSetFactory.empty().toTracking();
        }
        return retVal;
    }

    @Override
    public TrackingRowSet getRightIndexFromLeftIndex(long leftIndex) {
        long slot = leftIndexToSlot.get(leftIndex);
        if (slot == RowSet.NULL_ROW_KEY) {
            return RowSetFactory.empty().toTracking();
        }
        return getRightIndex(slot);
    }

    @Override
    public TrackingRowSet getRightIndexFromPrevLeftIndex(long leftIndex) {
        long slot = leftIndexToSlot.getPrev(leftIndex);
        if (slot == RowSet.NULL_ROW_KEY) {
            return RowSetFactory.empty().toTracking();
        }
        return getRightIndex(slot);
    }

    public void validateKeySpaceSize() {
        final long leftLastKey = leftTable.getRowSet().lastRowKey();
        final long rightLastKey = maxRightGroupSize - 1;
        final int minLeftBits = CrossJoinShiftState.getMinBits(leftLastKey);
        final int minRightBits = CrossJoinShiftState.getMinBits(rightLastKey);
        final int numShiftBits = getNumShiftBits();
        if (minLeftBits + numShiftBits > 63) {
            throw new OutOfKeySpaceException("join out of rowSet space (left reqBits + right reservedBits > 63): "
                    + "(left table: {size: " + leftTable.getRowSet().size() + " maxIndex: " + leftLastKey + " reqBits: " + minLeftBits + "}) X "
                    + "(right table: {maxIndexUsed: " + rightLastKey + " reqBits: " + minRightBits + " reservedBits: " + numShiftBits + "})"
                    + " exceeds Long.MAX_VALUE. Consider flattening left table or reserving fewer right bits if possible.");
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
    // endregion getStateValue

    // region overflowLocationToHashLocation
    private static boolean isOverflowLocation(long hashSlot) {
        return hashSlot < OVERFLOW_PIVOT_VALUE;
    }

    private static long hashLocationToOverflowLocation(long hashSlot) {
        return -hashSlot - 1 + OVERFLOW_PIVOT_VALUE;
    }

    private static long overflowLocationToHashLocation(long overflowSlot) {
        return -overflowSlot - 1 + OVERFLOW_PIVOT_VALUE;
    }
    // endregion overflowLocationToHashLocation


    static int hashTableSize(long initialCapacity) {
        return (int)Math.max(MINIMUM_INITIAL_HASH_SIZE, Math.min(MAX_TABLE_SIZE, Long.highestOneBit(initialCapacity) * 2));
    }

}

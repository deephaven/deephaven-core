/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.HashTableAnnotations;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.IntColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;

import static io.deephaven.util.SafeCloseable.closeArray;

// region class visibility
public
// endregion class visibility
abstract class StaticChunkedOperatorAggregationStateManagerTypedBase
        // region extensions
        implements ChunkedOperatorAggregationStateManager
// endregion extensions
{
    // region constants
    public static final int CHUNK_SIZE = ChunkedOperatorAggregationHelper.CHUNK_SIZE;
    private static final int MINIMUM_INITIAL_HASH_SIZE = CHUNK_SIZE;
    private static final long MAX_TABLE_SIZE = HashTableColumnSource.MINIMUM_OVERFLOW_HASH_SLOT;
    // endregion constants

    static final double DEFAULT_MAX_LOAD_FACTOR = 0.75;
    static final double DEFAULT_TARGET_LOAD_FACTOR = 0.70;

    // region preamble variables
    // endregion preamble variables

    @HashTableAnnotations.EmptyStateValue
    // @NullStateValue@ from \QQueryConstants.NULL_INT\E, @StateValueType@ from \Qint\E
    protected static final int EMPTY_RIGHT_VALUE = QueryConstants.NULL_INT;

    // mixin getStateValue
    // region overflow pivot
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
    protected long numEntries = 0;

    /**
     * Our table size must be 2^L (i.e. a power of two); and the pivot is between 2^(L-1) and 2^L.
     *
     * <p>
     * When hashing a value, if hashCode % 2^L < tableHashPivot; then the destination location is hashCode % 2^L. If
     * hashCode % 2^L >= tableHashPivot, then the destination location is hashCode % 2^(L-1). Once the pivot reaches the
     * table size, we can simply double the table size and repeat the process.
     * </p>
     *
     * <p>
     * This has the effect of only using hash table locations < hashTablePivot. When we want to expand the table we can
     * move some of the entries from the location {@code tableHashPivot - 2^(L-1)} to tableHashPivot. This provides for
     * incremental expansion of the hash table, without the need for a full rehash.
     * </p>
     */
    protected int tableHashPivot;

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
    protected final ArrayBackedColumnSource<?>[] keySources;
    // the location of any overflow entry in this bucket
    protected final IntegerArraySource overflowLocationSource = new IntegerArraySource();

    // we are going to also reuse this for our state entry, so that we do not need additional storage
    @HashTableAnnotations.StateColumnSource
    // @StateColumnSourceType@ from \QIntegerArraySource\E
    protected final IntegerArraySource stateSource
    // @StateColumnSourceConstructor@ from \QIntegerArraySource()\E
            = new IntegerArraySource();

    // the keys for overflow
    private int nextOverflowLocation = 0;
    protected final ArrayBackedColumnSource<?>[] overflowKeySources;
    // the location of the next key in an overflow bucket
    protected final IntegerArraySource overflowOverflowLocationSource = new IntegerArraySource();
    // the overflow buckets for the state source
    @HashTableAnnotations.OverflowStateColumnSource
    // @StateColumnSourceType@ from \QIntegerArraySource\E
    protected final IntegerArraySource overflowStateSource
    // @StateColumnSourceConstructor@ from \QIntegerArraySource()\E
            = new IntegerArraySource();

    // the type of each of our key chunks
    private final ChunkType[] keyChunkTypes;

    // the operators for hashing and various equality methods
    private final ChunkHasher[] chunkHashers;

    // region extra variables
    private final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();
    // endregion extra variables

    protected StaticChunkedOperatorAggregationStateManagerTypedBase(ColumnSource<?>[] tableKeySources, int tableSize
    // region constructor arguments
            , double maximumLoadFactor, double targetLoadFactor
    // endregion constructor arguments
    ) {
        // region super
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

        for (int ii = 0; ii < keyColumnCount; ++ii) {
            // the sources that we will use to store our hash table
            keySources[ii] = ArrayBackedColumnSource.getMemoryColumnSource(tableSize, tableKeySources[ii].getType());
            keyChunkTypes[ii] = tableKeySources[ii].getChunkType();

            overflowKeySources[ii] =
                    ArrayBackedColumnSource.getMemoryColumnSource(CHUNK_SIZE, tableKeySources[ii].getType());

            chunkHashers[ii] = ChunkHasher.makeHasher(keyChunkTypes[ii]);
        }

        // region constructor
        this.maximumLoadFactor = maximumLoadFactor;
        this.targetLoadFactor = targetLoadFactor;
        // endregion constructor

        ensureCapacity(tableSize);
    }

    private void ensureCapacity(int tableSize) {
        stateSource.ensureCapacity(tableSize);
        overflowLocationSource.ensureCapacity(tableSize);
        for (int ii = 0; ii < keyColumnCount; ++ii) {
            keySources[ii].ensureCapacity(tableSize);
        }
        // region ensureCapacity
        // endregion ensureCapacity
    }

    private void ensureOverflowCapacity(final int locationsToAllocate) {
        // mixin rehash
        if (freeOverflowCount >= locationsToAllocate) {
            return;
        }
        final int newCapacity = nextOverflowLocation + locationsToAllocate - freeOverflowCount;
        // endmixin rehash
        // altmixin rehash: final int newCapacity = nextOverflowLocation + locationsToAllocate;
        overflowOverflowLocationSource.ensureCapacity(newCapacity);
        overflowStateSource.ensureCapacity(newCapacity);
        // noinspection ForLoopReplaceableByForEach
        for (int ii = 0; ii < overflowKeySources.length; ++ii) {
            overflowKeySources[ii].ensureCapacity(newCapacity);
        }
        // region ensureOverflowCapacity
        // endregion ensureOverflowCapacity
    }

    // region build wrappers

    @Override
    public void add(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources,
            MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
        if (rowSequence.isEmpty()) {
            return;
        }
        buildTable((BuildContext) bc, rowSequence, sources, nextOutputPosition, outputPositions);
    }

    @Override
    public SafeCloseable makeAggregationStateBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return makeBuildContext(buildSources, maxSize);
    }

    // endregion build wrappers

    protected abstract void build(StaticAggHashHandler handler, RowSequence rowSequence,
            Chunk<Values>[] sourceKeyChunks);

    class BuildContext implements Context {
        final int chunkSize;

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
                sharedBuildContext = SharedContext.makeSharedContext();
            } else {
                // no point in the additional work implied by these not being null.
                sharedBuildContext = null;
            }
            buildContexts = makeGetContexts(buildSources, sharedBuildContext, chunkSize);
            // region build context constructor
            // endregion build context constructor
            // endmixin rehash
        }

        private void resetSharedContexts() {
            if (!haveSharedContexts) {
                return;
            }
            sharedBuildContext.reset();
        }

        private void closeSharedContexts() {
            if (!haveSharedContexts) {
                return;
            }
            sharedBuildContext.close();
        }

        @Override
        public void close() {
            closeArray(buildContexts);
            closeSharedContexts();
        }
    }

    BuildContext makeBuildContext(ColumnSource<?>[] buildSources,
            long maxSize
    // region makeBuildContext args
    // endregion makeBuildContext args
    ) {
        return new BuildContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize)
        // region makeBuildContext arg pass
        // endregion makeBuildContext arg pass
        );
    }

    public class StaticAggHashHandler implements HashHandler {
        private final MutableInt outputPosition;
        private final WritableIntChunk<RowKeys> outputPositions;

        StaticAggHashHandler(MutableInt outputPosition, WritableIntChunk<RowKeys> outputPositions) {
            this.outputPosition = outputPosition;
            this.outputPositions = outputPositions;
        }

        @Override
        public void doMainInsert(int tableLocation, int chunkPosition) {
            // region main insert
            int position = outputPosition.getAndIncrement();
            outputPositions.set(chunkPosition, position);
            stateSource.set(tableLocation, position);
            outputPositionToHashSlot.set(position, tableLocation);
            // endregion main insert
        }

        @Override
        public void moveMain(int oldTableLocation, int newTableLocation) {
            final int position = stateSource.getUnsafe(newTableLocation);
            outputPositionToHashSlot.set(position, newTableLocation);
        }

        @Override
        public void promoteOverflow(int overflowLocation, int mainInsertLocation) {
            outputPositionToHashSlot.set(stateSource.getUnsafe(mainInsertLocation), mainInsertLocation);
        }

        @Override
        public void doMainFound(int tableLocation, int chunkPosition) {
            // region main found
            outputPositions.set(chunkPosition, stateSource.getUnsafe(tableLocation));
            // endregion main found
        }

        @Override
        public void doOverflowFound(int overflowLocation, int chunkPosition) {
            // region build overflow found
            final int position = overflowStateSource.getUnsafe(overflowLocation);
            outputPositions.set(chunkPosition, position);
            // endregion build overflow found
        }

        @Override
        public void doOverflowInsert(int overflowLocation, int chunkPosition) {
            // region build overflow insert
            final int position = outputPosition.getAndIncrement();
            overflowStateSource.set(overflowLocation, position);
            outputPositions.set(chunkPosition, position);
            outputPositionToHashSlot.set(position,
                    HashTableColumnSource.overflowLocationToHashLocation((int) overflowLocation));
            // endregion build overflow insert
        }
    }


    private void buildTable(final BuildContext bc,
            final RowSequence buildIndex,
            ColumnSource<?>[] buildSources
            // region extra build arguments
            , final MutableInt outputPosition, final WritableIntChunk<RowKeys> outputPositions
    // endregion extra build arguments
    ) {
        final StaticAggHashHandler handler = new StaticAggHashHandler(outputPosition, outputPositions);
        // region build start
        outputPositions.setSize(buildIndex.intSize());
        // endregion build start

        try (final RowSequence.Iterator rsIt = buildIndex.getRowSequenceIterator();
        // region build initialization try
        // endregion build initialization try
        ) {
            // region build initialization
            // endregion build initialization

            // chunks to write through to the table key sources


            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[buildSources.length];

            while (rsIt.hasMore()) {
                // we reset early to avoid carrying around state for old RowSequence which can't be reused.
                bc.resetSharedContexts();

                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(bc.chunkSize);
                ensureOverflowCapacity(chunkOk.intSize());
                outputPositionToHashSlot.ensureCapacity(outputPosition.intValue() + chunkOk.size());

                getKeyChunks(buildSources, bc.buildContexts, sourceKeyChunks, chunkOk);

                build(handler, chunkOk, sourceKeyChunks);
            }

            doRehash(handler);

            // region copy hash slots
            // endregion copy hash slots
        }
        // region post build loop
        // endregion post build loop
    }

    // mixin rehash
    public void doRehash(StaticAggHashHandler handler
    // region extra rehash arguments
    // endregion extra rehash arguments
    ) {
        while (rehashRequired()) {
            // region rehash loop start
            // endregion rehash loop start
            if (tableHashPivot == tableSize) {
                tableSize *= 2;
                ensureCapacity(tableSize);
                // region rehash ensure capacity
                // endregion rehash ensure capacity
            }

            final long targetBuckets = Math.min(MAX_TABLE_SIZE, (long) (numEntries / targetLoadFactor));
            final int bucketsToAdd = Math.max(1, (int) Math.min(targetBuckets, tableSize) - tableHashPivot);

            freeOverflowLocations.ensureCapacity(freeOverflowCount + bucketsToAdd, false);

            for (int ii = 0; ii < bucketsToAdd; ++ii) {
                int checkBucket = tableHashPivot + ii - (tableSize >> 1);
                int destBucket = tableHashPivot + ii;
                // if we were more complicated, we would need a handler for promotion and moving slots
                rehashBucket(handler, checkBucket, destBucket, bucketsToAdd);
            }
            tableHashPivot += bucketsToAdd;
        }
    }

    protected abstract void rehashBucket(StaticAggHashHandler handler, int bucket, int destBucket, int bucketsToAdd);

    public boolean rehashRequired() {
        return numEntries > (tableHashPivot * maximumLoadFactor) && tableHashPivot < MAX_TABLE_SIZE;
    }

    void setTargetLoadFactor(final double targetLoadFactor) {
        this.targetLoadFactor = targetLoadFactor;
    }

    void setMaximumLoadFactor(final double maximumLoadFactor) {
        this.maximumLoadFactor = maximumLoadFactor;
    }

    // endmixin rehash

    protected int allocateOverflowLocation() {
        // mixin rehash
        if (freeOverflowCount > 0) {
            return freeOverflowLocations.getUnsafe(--freeOverflowCount);
        }
        // endmixin rehash
        return nextOverflowLocation++;
    }

    protected void freeOverflowLocation(int location) {
        freeOverflowLocations.ensureCapacity(freeOverflowCount + CHUNK_SIZE, false);
        freeOverflowLocations.set(freeOverflowCount++, location);
    }

    private void getKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts,
            Chunk<? extends Values>[] chunks, RowSequence rowSequence) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getChunk(contexts[ii], rowSequence);
        }
    }

    // region probe wrappers
    // endregion probe wrappers

    protected int hashToTableLocation(
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
    @Override
    public ColumnSource[] getKeyHashTableSources() {
        final WritableRowRedirection resultIndexToHashSlot =
                new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);
        final ColumnSource[] keyHashTableSources = new ColumnSource[keyColumnCount];
        for (int kci = 0; kci < keyColumnCount; ++kci) {
            // noinspection unchecked
            keyHashTableSources[kci] = new RedirectedColumnSource(resultIndexToHashSlot,
                    new HashTableColumnSource(keySources[kci], overflowKeySources[kci]));
        }
        return keyHashTableSources;
    }

    @Override
    abstract public int findPositionForKey(Object key);
    // endregion extraction functions

    private static ColumnSource.GetContext[] makeGetContexts(ColumnSource<?>[] sources, final SharedContext sharedState,
            int chunkSize) {
        final ColumnSource.GetContext[] contexts = new ColumnSource.GetContext[sources.length];
        for (int ii = 0; ii < sources.length; ++ii) {
            contexts[ii] = sources[ii].makeGetContext(chunkSize, sharedState);
        }
        return contexts;
    }
}

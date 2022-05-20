package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.HashTableAnnotations;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.table.impl.util.TypedHasherUtil;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.BuildContext;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.ProbeContext;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

public abstract class RightIncrementalAsOfJoinStateManagerTypedBase extends RightIncrementalHashedAsOfJoinStateManager {
    public static final int CHUNK_SIZE = 4096;
    private static final long MAX_TABLE_SIZE = 1 << 30; // maximum array size

    public static final byte ENTRY_RIGHT_MASK = 0x3;
    public static final byte ENTRY_RIGHT_IS_EMPTY = 0x0;
    public static final byte ENTRY_RIGHT_IS_BUILDER = 0x1;
    public static final byte ENTRY_RIGHT_IS_SSA = 0x2;
    public static final byte ENTRY_RIGHT_IS_INDEX = 0x3;

    public static final byte ENTRY_LEFT_MASK = 0x30;
    public static final byte ENTRY_LEFT_IS_EMPTY = 0x00;
    public static final byte ENTRY_LEFT_IS_BUILDER = 0x10;
    public static final byte ENTRY_LEFT_IS_SSA = 0x20;
    public static final byte ENTRY_LEFT_IS_INDEX = 0x30;

    private static final byte ENTRY_INITIAL_STATE_LEFT = ENTRY_LEFT_IS_BUILDER|ENTRY_RIGHT_IS_EMPTY;
    private static final byte ENTRY_INITIAL_STATE_RIGHT = ENTRY_LEFT_IS_EMPTY|ENTRY_RIGHT_IS_BUILDER;

    // the number of slots in our table
    protected int tableSize;

    // the number of slots in our alternate table, to start with "1" is a lie, but rehashPointer is zero; so our
    // location value is positive and can be compared against rehashPointer safely
    protected int alternateTableSize = 1;

    // how much of the alternate sources are necessary to rehash?
    protected int rehashPointer = 0;

    protected long numEntries = 0;

    // the table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table
    private final double maximumLoadFactor;

    // the keys for our hash entries
    protected final ChunkType[] chunkTypes;
    protected final WritableColumnSource[] mainKeySources;

    /**
     * We use our side source to originally build the RowSet using builders.  When a state is activated (meaning we
     * have a corresponding entry for it on the other side); we'll turn it into an SSA.  If we have updates for an
     * inactive state, then we turn it into a WritableRowSet. The entry state tells us what we have on each side, using
     * a nibble for the left and a nibble for the right.
     */
    protected final ImmutableObjectArraySource<Object> leftRowSetSource;
//    protected final ImmutableObjectArraySource<Object> alternateLeftRowSetSource;
    protected final ImmutableObjectArraySource<Object> rightRowSetSource;
//    protected final ImmutableObjectArraySource<Object> alternateRightRowSetSource;

    private final ByteArraySource stateSource = new ByteArraySource();
    private final ByteArraySource alternateStateSource = new ByteArraySource();

    protected RightIncrementalAsOfJoinStateManagerTypedBase(ColumnSource<?>[] tableKeySources,
                                                            ColumnSource<?>[] keySourcesForErrorMessages, int tableSize, double maximumLoadFactor) {
        // region super
        super(keySourcesForErrorMessages);
        // endregion super

        this.tableSize = tableSize;
        Require.leq(tableSize, "tableSize", MAX_TABLE_SIZE);
        Require.gtZero(tableSize, "tableSize");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        Require.inRange(maximumLoadFactor, 0.0, 0.95, "maximumLoadFactor");

        // region constructor
        mainKeySources = new WritableColumnSource[tableKeySources.length];
        chunkTypes = new ChunkType[tableKeySources.length];
        leftRowSetSource = new ImmutableObjectArraySource<>(Object.class, null);
        rightRowSetSource = new ImmutableObjectArraySource<>(Object.class, null);
        // endregion constructor

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            chunkTypes[ii] = keySourcesForErrorMessages[ii].getChunkType();
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
        }

        this.maximumLoadFactor = maximumLoadFactor;

        ensureCapacity(tableSize);
    }

    private void ensureCapacity(int tableSize) {
        for (WritableColumnSource<?> mainKeySource : mainKeySources) {
            mainKeySource.ensureCapacity(tableSize);
        }
        // region ensureCapacity
        leftRowSetSource.ensureCapacity(tableSize);
        rightRowSetSource.ensureCapacity(tableSize);
        // endregion ensureCapacity
    }

    BuildContext makeBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new BuildContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    ProbeContext makeProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new ProbeContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    static boolean addIndex(ImmutableObjectArraySource<RowSetBuilderSequential> source, long location, long keyToAdd) {
        boolean addedSlot = false;
        RowSetBuilderSequential builder = source.getUnsafe(location);
        if (builder == null) {
            source.set(location, builder = RowSetFactory.builderSequential());
            addedSlot = true;
        }
        builder.appendKey(keyToAdd);
        return addedSlot;
    }

    /**
     * Returns true if this is the first left row key added to this slot.
     */
    protected boolean addLeftIndex(long tableLocation, long keyToAdd) {
        return false;
//        return addIndex(leftRowSetSource, tableLocation, keyToAdd);
    }

    protected void addRightIndex(long tableLocation, long keyToAdd) {
        // noinspection unchecked
        addIndex((ImmutableObjectArraySource) rightRowSetSource, tableLocation, keyToAdd);
    }

    protected void buildTable(
            final BuildContext bc,
            final RowSequence buildRows,
            final ColumnSource<?>[] buildSources,
            final TypedHasherUtil.BuildHandler buildHandler) {
        try (final RowSequence.Iterator rsIt = buildRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[buildSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(bc.chunkSize);
                final int nextChunkSize = chunkOk.intSize();

                if (exceedsCapacity(nextChunkSize)) {
                    throw new IllegalStateException(
                            "Static asOfJoin does not permit rehashing, table must be allocated with sufficient size at the beginning of initialization.");
                }

                getKeyChunks(buildSources, bc.getContexts, sourceKeyChunks, chunkOk);

                buildHandler.doBuild(chunkOk, sourceKeyChunks);

                bc.resetSharedContexts();
            }
        }
    }

    private class StaticBuildHandler implements TypedHasherUtil.BuildHandler {
        final boolean isLeftSide;
        final LongArraySource hashSlots;
        final MutableInt hashOffset;

        private StaticBuildHandler(final boolean isLeftSide, final LongArraySource hashSlots,
                                   final MutableInt hashOffset) {
            this.hashSlots = hashSlots;
            this.isLeftSide = isLeftSide;
            this.hashOffset = hashOffset;
        }

        @Override
        public void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            hashSlots.ensureCapacity(hashOffset.intValue() + chunkOk.intSize());
            if (isLeftSide) {
                buildFromLeftSide(chunkOk, sourceKeyChunks, hashSlots, hashOffset);
            } else {
                buildFromRightSide(chunkOk, sourceKeyChunks, hashSlots, hashOffset);
            }
        }
    }

    @Override
    public int buildFromLeftSide(RowSequence leftRowSet, ColumnSource<?>[] leftSources,
            @NotNull final LongArraySource addedSlots) {
        if (leftRowSet.isEmpty()) {
            return 0;
        }
        try (final BuildContext bc = makeBuildContext(leftSources, leftRowSet.size())) {
            final MutableInt slotCount = new MutableInt(0);
            buildTable(bc, leftRowSet, leftSources, new StaticBuildHandler(true, addedSlots, slotCount));
            return slotCount.intValue();
        }
    }

    protected void probeTable(
            final ProbeContext pc,
            final RowSequence probeRows,
            final boolean usePrev,
            final ColumnSource<?>[] probeSources,
            final TypedHasherUtil.ProbeHandler handler) {
        try (final RowSequence.Iterator rsIt = probeRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[probeSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(pc.chunkSize);

                if (usePrev) {
                    getPrevKeyChunks(probeSources, pc.getContexts, sourceKeyChunks, chunkOk);
                } else {
                    getKeyChunks(probeSources, pc.getContexts, sourceKeyChunks, chunkOk);
                }

                handler.doProbe(chunkOk, sourceKeyChunks);

                pc.resetSharedContexts();
            }
        }
    }

    private class StaticProbeHandler implements TypedHasherUtil.ProbeHandler {
        final boolean isLeftSide;
        final LongArraySource hashSlots;
        final MutableInt hashOffset;
        final RowSetBuilderRandom foundBuilder;

        private StaticProbeHandler(final boolean isLeftSide, final LongArraySource hashSlots,
                                   final MutableInt hashOffset, RowSetBuilderRandom foundBuilder) {
            this.isLeftSide = isLeftSide;
            this.hashSlots = hashSlots;
            this.hashOffset = hashOffset;
            this.foundBuilder = foundBuilder;
        }

        @Override
        public void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            if (isLeftSide) {
                decorateLeftSide(chunkOk, sourceKeyChunks, hashSlots, hashOffset, foundBuilder);
            } else {
                decorateWithRightSide(chunkOk, sourceKeyChunks);
            }
        }
    }




//    @Override
//    public int buildFromLeftSide(RowSequence leftRowSet, ColumnSource<?>[] leftSources, @NotNull final LongArraySource addedSlots) {
//        return 0;
//    }
//    @Override
//    public int buildFromRightSide(RowSequence rightRowSet, ColumnSource<?>[] rightSources, @NotNull final LongArraySource addedSlots, int usedSlots);

    @Override
    public void probeRightInitial(RowSequence rightIndex, ColumnSource<?>[] rightSources) {

    }
    @Override
    public int probeAdditions(RowSet restampAdditions, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return 0;
    }
    @Override
    public int buildAdditions(boolean isLeftSide, RowSet additions, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders){
        return 0;
    }

    @Override
    public SegmentedSortedArray getRightSsa(long slot) {
        return null;
    }
    @Override
    public SegmentedSortedArray getRightSsa(long slot, Function<RowSet, SegmentedSortedArray> ssaFactory) {
        return null;
    }
    @Override
    public SegmentedSortedArray getLeftSsa(long slot) {
        return null;
    }
    @Override
    public SegmentedSortedArray getLeftSsa(long slot, Function<RowSet, SegmentedSortedArray> ssaFactory) {
        return null;
    }

    @Override
    public SegmentedSortedArray getLeftSsaOrIndex(long slot, MutableObject<WritableRowSet> indexOutput) {
        return null;
    }
    @Override
    public SegmentedSortedArray getRightSsaOrIndex(long slot, MutableObject<WritableRowSet> indexOutput) {
        return null;
    }
    @Override
    public void setRightIndex(long slot, RowSet rowSet) {

    }
    @Override
    public void setLeftIndex(long slot, RowSet rowSet) {

    }

    @Override
    public WritableRowSet getAndClearLeftIndex(long slot) {
        return null;
    }

    @Override
    public int markForRemoval(RowSet restampRemovals, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return 0;
    }

    @Override
    public int gatherShiftIndex(RowSet restampAdditions, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return 0;
    }

    @Override
    public int gatherModifications(RowSet restampAdditions, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return 0;
    }

    @Override
    public byte getState(long slot) {
        return 0;
    }






    @Override
    public int getTableSize() {
        return tableSize;
    }

    @Override
    public int getOverflowSize() {
        return 0;
    }

    /**
     * When we get the left RowSet out of our source (after a build or probe); we do it by pulling a sequential builder
     * and then calling build(). We also null out the value in the column source, thus freeing the builder's memory.
     *
     * This also results in clearing out the left hand side of the table between each probe phase for the left
     * refreshing case.
     *
     * @param slot the slot in the table
     * @return the RowSet for this slot
     */
    @Override
    public WritableRowSet getLeftIndex(long slot) {
        RowSetBuilderSequential builder = (RowSetBuilderSequential) leftRowSetSource.getAndSetUnsafe(slot, null);
        if (builder == null) {
            return null;
        }
        return builder.build();
    }

    @Override
    public WritableRowSet getRightIndex(long slot) {
//        if (rightBuildersConverted) {
//            return (WritableRowSet) rightRowSetSource.getUnsafe(slot);
//        }
        RowSetBuilderSequential builder = (RowSetBuilderSequential) rightRowSetSource.getUnsafe(slot);
        if (builder == null) {
            return null;
        }
        return builder.build();
    }

    abstract protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource hashSlots, MutableInt hashSlotOffset);

    abstract protected void buildFromRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource hashSlots, MutableInt hashSlotOffset);

    abstract protected void decorateLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource hashSlots, MutableInt hashSlotOffset, RowSetBuilderRandom foundBuilder);

    abstract protected void decorateWithRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    public boolean exceedsCapacity(int nextChunkSize) {
        return (numEntries + nextChunkSize) >= (tableSize);
    }

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }
}

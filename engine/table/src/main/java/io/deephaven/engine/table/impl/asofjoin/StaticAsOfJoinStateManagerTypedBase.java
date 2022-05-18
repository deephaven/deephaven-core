package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.engine.table.impl.util.TypedHasherUtil;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.BuildContext;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.ProbeContext;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

public abstract class StaticAsOfJoinStateManagerTypedBase extends StaticHashedAsOfJoinStateManager {
    public static final int CHUNK_SIZE = 4096;
    private static final long MAX_TABLE_SIZE = 1 << 30; // maximum array size
    public static final Object EMPTY_RIGHT_STATE = null;

    // the number of slots in our table
    protected int tableSize;

    protected long numEntries = 0;

    // the table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table
    private final double maximumLoadFactor;

    // the keys for our hash entries
    protected final ChunkType[] chunkTypes;
    protected final WritableColumnSource[] mainKeySources;

    protected final ImmutableObjectArraySource<RowSetBuilderSequential> leftRowSetSource; // use immutable

    /**
     * For the ticking case we need to reuse our right rowsets for more than one update. We convert the
     * SequentialBuilders into actual RowSet objects. Before the conversion (which must be during the build phase) we
     * put the sequential builders into rightRowSetSource. After the conversion, the sources store actual rowsets.
     */
    private boolean rightBuildersConverted = false;

    protected final ImmutableObjectArraySource<Object> rightRowSetSource;

    protected StaticAsOfJoinStateManagerTypedBase(ColumnSource<?>[] tableKeySources,
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
        leftRowSetSource = new ImmutableObjectArraySource<>(RowSetBuilderSequential.class, null);
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
        return addIndex(leftRowSetSource, tableLocation, keyToAdd);
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

    @Override
    public int buildFromRightSide(RowSequence rightRowSet, ColumnSource<?>[] rightSources,
            @NotNull final LongArraySource addedSlots) {
        if (rightRowSet.isEmpty()) {
            return 0;
        }
        try (final BuildContext bc = makeBuildContext(rightSources, rightRowSet.size())) {
            final MutableInt slotCount = new MutableInt(0);
            buildTable(bc, rightRowSet, rightSources, new StaticBuildHandler(false, addedSlots, slotCount));
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

    @Override
    public void probeLeft(RowSequence leftRowSet, ColumnSource<?>[] leftSources) {
        if (leftRowSet.isEmpty()) {
            return;
        }
        try (final ProbeContext pc = makeProbeContext(leftSources, leftRowSet.size())) {
            probeTable(pc, leftRowSet, false, leftSources, new StaticProbeHandler(true, null, null, null));
        }
    }

    @Override
    public int probeLeft(RowSequence leftRowSet, ColumnSource<?>[] leftSources, @NotNull final LongArraySource slots,
            RowSetBuilderRandom foundBuilder) {
        if (leftRowSet.isEmpty()) {
            return 0;
        }
        try (final ProbeContext pc = makeProbeContext(leftSources, leftRowSet.size())) {
            final MutableInt slotCount = new MutableInt();
            probeTable(pc, leftRowSet, false, leftSources,
                    new StaticProbeHandler(true, slots, slotCount, foundBuilder));
            return slotCount.intValue();
        }
    }

    @Override
    public void probeRight(RowSequence rightRowSet, ColumnSource<?>[] rightSources) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        try (final ProbeContext pc = makeProbeContext(rightSources, rightRowSet.size())) {
            probeTable(pc, rightRowSet, false, rightSources, new StaticProbeHandler(false, null, null, null));
        }
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
    public RowSet getLeftIndex(long slot) {
        RowSetBuilderSequential builder = (RowSetBuilderSequential) leftRowSetSource.getAndSetUnsafe(slot, null);
        if (builder == null) {
            return null;
        }
        return builder.build();
    }

    @Override
    public RowSet getRightIndex(long slot) {
        if (rightBuildersConverted) {
            return (RowSet) rightRowSetSource.getUnsafe(slot);
        }
        RowSetBuilderSequential builder = (RowSetBuilderSequential) rightRowSetSource.getUnsafe(slot);
        if (builder == null) {
            return null;
        }
        return builder.build();
    }

    @Override
    public void convertRightBuildersToIndex(LongArraySource slots, int slotCount) {
        for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
            final long slot = slots.getLong(slotIndex);
            // this might be empty, if so then set null
            final RowSetBuilderSequential sequentialBuilder =
                    (RowSetBuilderSequential) rightRowSetSource.getUnsafe(slot);
            if (sequentialBuilder != null) {
                WritableRowSet rs = sequentialBuilder.build();
                if (rs.isEmpty()) {
                    rightRowSetSource.set(slot, EMPTY_RIGHT_STATE);
                    rs.close();
                } else {
                    rightRowSetSource.set(slot, rs);
                }
            }
        }
        rightBuildersConverted = true;
    }

    @Override
    public void convertRightGrouping(LongArraySource slots, int slotCount, ObjectArraySource<RowSet> rowSetSource) {
        for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
            final long slot = slots.getLong(slotIndex);

            final RowSetBuilderSequential sequentialBuilder =
                    (RowSetBuilderSequential) rightRowSetSource.getUnsafe(slot);
            if (sequentialBuilder != null) {
                WritableRowSet rs = sequentialBuilder.build();
                if (rs.isEmpty()) {
                    rightRowSetSource.set(slot, EMPTY_RIGHT_STATE);
                    rs.close();
                } else {
                    rightRowSetSource.set(slot, getGroupedIndex(rowSetSource, sequentialBuilder));
                }
            }
        }
        rightBuildersConverted = true;
    }

    private RowSet getGroupedIndex(ObjectArraySource<RowSet> rowSetSource, RowSetBuilderSequential sequentialBuilder) {
        final RowSet groupedRowSet = sequentialBuilder.build();
        if (groupedRowSet.size() != 1) {
            throw new IllegalStateException("Grouped rowSet should have exactly one value: " + groupedRowSet);
        }
        return rowSetSource.getUnsafe(groupedRowSet.get(0));
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

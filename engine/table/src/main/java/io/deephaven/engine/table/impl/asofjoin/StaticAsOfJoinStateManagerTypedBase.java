/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.engine.table.impl.util.TypedHasherUtil;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.BuildContext;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.ProbeContext;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;
import static io.deephaven.engine.table.impl.JoinControl.MAX_TABLE_SIZE;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

public abstract class StaticAsOfJoinStateManagerTypedBase extends StaticHashedAsOfJoinStateManager {

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

    protected final ImmutableObjectArraySource<RowSetBuilderSequential> leftRowSetSource;

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
            chunkTypes[ii] = tableKeySources[ii].getChunkType();
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

    static boolean addIndex(ImmutableObjectArraySource<RowSetBuilderSequential> source, int location, long keyToAdd) {
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
    protected boolean addLeftIndex(int tableLocation, long keyToAdd) {
        return addIndex(leftRowSetSource, tableLocation, keyToAdd);
    }

    protected void addRightIndex(int tableLocation, long keyToAdd) {
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

                doRehash(chunkOk.intSize());

                getKeyChunks(buildSources, bc.getContexts, sourceKeyChunks, chunkOk);

                buildHandler.doBuild(chunkOk, sourceKeyChunks);

                bc.resetSharedContexts();
            }
        }
    }

    private class LeftBuildHandler implements TypedHasherUtil.BuildHandler {
        @Override
        public void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            buildFromLeftSide(chunkOk, sourceKeyChunks);
        }
    }

    private class RightBuildHandler implements TypedHasherUtil.BuildHandler {
        @Override
        public void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            buildFromRightSide(chunkOk, sourceKeyChunks);
        }
    }

    private int fillSlotsFromHashTable(@NotNull final IntegerArraySource slotArray) {
        slotArray.ensureCapacity(tableSize);
        long slotCount = 0;
        for (int slotIdx = 0; slotIdx < tableSize; slotIdx++) {
            if (rightRowSetSource.get(slotIdx) != EMPTY_RIGHT_STATE) {
                slotArray.set(slotCount++, slotIdx);
            }
        }
        return (int) slotCount;
    }

    @Override
    public int buildFromLeftSide(RowSequence leftRowSet, ColumnSource<?>[] leftSources,
            @NotNull final IntegerArraySource addedSlots) {
        if (leftRowSet.isEmpty()) {
            return 0;
        }
        try (final BuildContext bc = makeBuildContext(leftSources, leftRowSet.size())) {
            buildTable(bc, leftRowSet, leftSources, new LeftBuildHandler());
            return fillSlotsFromHashTable(addedSlots);
        }
    }

    @Override
    public int buildFromRightSide(RowSequence rightRowSet, ColumnSource<?>[] rightSources,
            @NotNull final IntegerArraySource addedSlots) {
        if (rightRowSet.isEmpty()) {
            return 0;
        }
        try (final BuildContext bc = makeBuildContext(rightSources, rightRowSet.size())) {
            buildTable(bc, rightRowSet, rightSources, new RightBuildHandler());
            return fillSlotsFromHashTable(addedSlots);
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

    private class LeftProbeHandler implements TypedHasherUtil.ProbeHandler {
        final IntegerArraySource hashSlots;
        final MutableLong hashOffset;
        final RowSetBuilderRandom foundBuilder;

        private LeftProbeHandler() {
            this.hashSlots = null;
            this.hashOffset = null;
            this.foundBuilder = null;
        }

        private LeftProbeHandler(final IntegerArraySource hashSlots, final MutableLong hashOffset,
                RowSetBuilderRandom foundBuilder) {
            this.hashSlots = hashSlots;
            this.hashOffset = hashOffset;
            this.foundBuilder = foundBuilder;
        }

        @Override
        public void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            decorateLeftSide(chunkOk, sourceKeyChunks, hashSlots, hashOffset, foundBuilder);
        }
    }

    private class RightProbeHandler implements TypedHasherUtil.ProbeHandler {
        @Override
        public void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            decorateWithRightSide(chunkOk, sourceKeyChunks);
        }
    }

    @Override
    public void probeLeft(RowSequence leftRowSet, ColumnSource<?>[] leftSources) {
        if (leftRowSet.isEmpty()) {
            return;
        }
        try (final ProbeContext pc = makeProbeContext(leftSources, leftRowSet.size())) {
            probeTable(pc, leftRowSet, false, leftSources, new LeftProbeHandler());
        }
    }

    @Override
    public int probeLeft(RowSequence leftRowSet, ColumnSource<?>[] leftSources, @NotNull final IntegerArraySource slots,
            RowSetBuilderRandom foundBuilder) {
        if (leftRowSet.isEmpty()) {
            return 0;
        }
        try (final ProbeContext pc = makeProbeContext(leftSources, leftRowSet.size())) {
            final MutableLong slotCount = new MutableLong();
            probeTable(pc, leftRowSet, false, leftSources, new LeftProbeHandler(slots, slotCount, foundBuilder));
            return slotCount.intValue();
        }
    }

    @Override
    public void probeRight(RowSequence rightRowSet, ColumnSource<?>[] rightSources) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        try (final ProbeContext pc = makeProbeContext(rightSources, rightRowSet.size())) {
            probeTable(pc, rightRowSet, false, rightSources, new RightProbeHandler());
        }
    }

    @Override
    public int getTableSize() {
        return tableSize;
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
    public RowSet getLeftIndex(int slot) {
        RowSetBuilderSequential builder = (RowSetBuilderSequential) leftRowSetSource.getAndSetUnsafe(slot, null);
        if (builder == null) {
            return null;
        }
        return builder.build();
    }

    @Override
    public RowSet getRightIndex(int slot) {
        if (rightBuildersConverted) {
            return (RowSet) rightRowSetSource.getUnsafe(slot);
        }
        throw new IllegalStateException(
                "getRightIndex() may not be called before convertRightBuildersToIndex() or convertRightGrouping()");
    }

    @Override
    public void convertRightBuildersToIndex(IntegerArraySource slots, int slotCount) {
        for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
            final int slot = slots.getInt(slotIndex);
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
    public void convertRightGrouping(IntegerArraySource slots, int slotCount, ObjectArraySource<RowSet> rowSetSource) {
        for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
            final int slot = slots.getInt(slotIndex);

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

    abstract protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    abstract protected void buildFromRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    abstract protected void decorateLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            IntegerArraySource hashSlots, MutableLong hashSlotOffset, RowSetBuilderRandom foundBuilder);

    abstract protected void decorateWithRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    public void doRehash(final int nextChunkSize) {
        final int oldSize = tableSize;
        while (rehashRequired(nextChunkSize)) {
            tableSize *= 2;
            if (tableSize < 0 || tableSize > MAX_TABLE_SIZE) {
                throw new UnsupportedOperationException("Hash table exceeds maximum size!");
            }
        }
        if (tableSize > oldSize) {
            rehashInternalFull(oldSize);
        }
    }

    public boolean rehashRequired(int nextChunkSize) {
        return (numEntries + nextChunkSize) > (tableSize * maximumLoadFactor);
    }

    abstract protected void rehashInternalFull(final int oldSize);

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }
}

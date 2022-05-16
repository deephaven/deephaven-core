package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.util.TypedHasherUtil;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.BuildContext;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.ProbeContext;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

public abstract class StaticAsOfJoinStateManagerTypedBase extends StaticHashedAsOfJoinStateManager {
    public static final int CHUNK_SIZE = 4096;
    private static final long MAX_TABLE_SIZE = 1 << 30; // maximum array size
    public static final long NO_RIGHT_STATE_VALUE = RowSet.NULL_ROW_KEY;
    public static final long EMPTY_RIGHT_STATE = QueryConstants.NULL_LONG;
    public static final long DUPLICATE_RIGHT_STATE = -2;

    // the number of slots in our table
    protected int tableSize;

    protected long numEntries = 0;

    // the table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table
    private final double maximumLoadFactor;

    // the keys for our hash entries
    protected final ChunkType[] chunkTypes;
    protected final WritableColumnSource[] mainKeySources;

    protected ImmutableLongArraySource mainRightRowKey = new ImmutableLongArraySource();

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

        mainKeySources = new WritableColumnSource[tableKeySources.length];
        chunkTypes = new ChunkType[tableKeySources.length];

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            chunkTypes[ii] = keySourcesForErrorMessages[ii].getChunkType();
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
        }

        this.maximumLoadFactor = maximumLoadFactor;

        ensureCapacity(tableSize);
    }

    private void ensureCapacity(int tableSize) {
        mainRightRowKey.ensureCapacity(tableSize);
        for (WritableColumnSource<?> mainKeySource : mainKeySources) {
            mainKeySource.ensureCapacity(tableSize);
        }
    }

    BuildContext makeBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new BuildContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    ProbeContext makeProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new ProbeContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    @Override
    public int buildFromLeftSide(RowSequence leftIndex, ColumnSource<?>[] leftSources, @NotNull final LongArraySource addedSlots) {
        return 0;
    }

    @Override
    public int buildFromRightSide(RowSequence rightIndex, ColumnSource<?>[] rightSources, @NotNull final LongArraySource addedSlots) {
        return 0;
    }

    @Override
    public void probeLeft(RowSequence leftIndex, ColumnSource<?>[] leftSources) {

    }

    @Override
    public int probeLeft(RowSequence leftIndex, ColumnSource<?>[] leftSources, LongArraySource slots, RowSetBuilderRandom foundBuilder) {
        return 0;
    }

    @Override
    public void probeRight(RowSequence rightIndex, ColumnSource<?>[] rightSources) {

    }

    @Override
    public int getTableSize() {
        return tableSize;
    }

    @Override
    public int getOverflowSize() {
        return 0;
    }

    @Override
    public RowSet getLeftIndex(long slot) {
        return RowSetFactory.empty();
    }

    @Override
    public RowSet getRightIndex(long slot) {
        return RowSetFactory.empty();
    }

    @Override
    public void convertRightBuildersToIndex(LongArraySource slots, int slotCount) {

    }

    @Override
    public void convertRightGrouping(LongArraySource slots, int slotCount, ObjectArraySource<RowSet> rowSetSource) {

    }








    abstract protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
                                              LongArraySource leftHashSlots, int hashSlotOffset);

    abstract protected void buildFromRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    abstract protected void decorateLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
                                             LongArraySource leftRedirections, int hashSlotOffset);
    abstract protected void decorateWithRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);


    private class LeftBuildHandler implements TypedHasherUtil.BuildHandler {
        final LongArraySource leftHashSlots;
        int offset = 0;

        private LeftBuildHandler(LongArraySource leftHashSlots) {
            this.leftHashSlots = leftHashSlots;
        }

        @Override
        public void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            leftHashSlots.ensureCapacity(offset + chunkOk.intSize());
            buildFromLeftSide(chunkOk, sourceKeyChunks, leftHashSlots, offset);
            offset += chunkOk.intSize();
        }
    }

    private class LeftProbeHandler implements TypedHasherUtil.ProbeHandler {
        final LongArraySource leftHashSlots;
        int offset = 0;

        private LeftProbeHandler(LongArraySource leftHashSlots) {
            this.leftHashSlots = leftHashSlots;
        }

        @Override
        public void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            leftHashSlots.ensureCapacity(offset + chunkOk.intSize());
            decorateLeftSide(chunkOk, sourceKeyChunks, leftHashSlots, offset);
            offset += chunkOk.intSize();
        }
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

    public boolean exceedsCapacity(int nextChunkSize) {
        return (numEntries + nextChunkSize) >= (tableSize);
    }

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }

    public void errorOnDuplicatesGrouped(LongArraySource leftHashSlots, long size,
            ObjectArraySource<RowSet> rowSetSource) {
        errorOnDuplicates(leftHashSlots, size,
                (long groupPosition) -> mainRightRowKey.getUnsafe(leftHashSlots.getUnsafe(groupPosition)),
                (long row) -> rowSetSource.getUnsafe(row).firstRowKey());
    }

    public void errorOnDuplicatesSingle(LongArraySource leftHashSlots, long size, RowSet rowSet) {
        errorOnDuplicates(leftHashSlots, size,
                (long position) -> mainRightRowKey.getUnsafe(leftHashSlots.getUnsafe(position)), rowSet::get);
    }
}

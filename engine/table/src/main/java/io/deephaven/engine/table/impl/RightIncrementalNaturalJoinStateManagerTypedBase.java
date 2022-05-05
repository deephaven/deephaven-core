package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.SafeCloseable.closeArray;

public abstract class RightIncrementalNaturalJoinStateManagerTypedBase extends RightIncrementalNaturalJoinStateManager {
    public static final int CHUNK_SIZE = 4096;
    private static final long MAX_TABLE_SIZE = 1 << 30; // maximum array size

    // the number of slots in our table
    protected int tableSize;

    protected long numEntries = 0;

    // the table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table
    private final double maximumLoadFactor;

    // the keys for our hash entries
    protected final ChunkType[] chunkTypes;
    protected final WritableColumnSource[] mainKeySources;

    protected ImmutableObjectArraySource<WritableRowSet> leftRowSet =
            new ImmutableObjectArraySource(WritableRowSet.class, null);
    protected ImmutableLongArraySource rightRowKey = new ImmutableLongArraySource();
    protected ImmutableLongArraySource modifiedTrackerCookieSource = new ImmutableLongArraySource();

    protected RightIncrementalNaturalJoinStateManagerTypedBase(ColumnSource<?>[] tableKeySources,
            ColumnSource<?>[] keySourcesForErrorMessages, int tableSize, double maximumLoadFactor) {
        super(keySourcesForErrorMessages);

        // we start out with a chunk sized table, and will grow by rehashing the left as states are added
        this.tableSize = CHUNK_SIZE;
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
        leftRowSet.ensureCapacity(tableSize);
        rightRowKey.ensureCapacity(tableSize);
        modifiedTrackerCookieSource.ensureCapacity(tableSize);
        for (WritableColumnSource<?> mainKeySource : mainKeySources) {
            mainKeySource.ensureCapacity(tableSize);
        }
    }

    public static class BuildContext extends BuildOrProbeContext {
        private BuildContext(ColumnSource<?>[] buildSources, int chunkSize) {
            super(buildSources, chunkSize);
        }
    }

    public static class ProbeContext extends BuildOrProbeContext {
        private ProbeContext(ColumnSource<?>[] buildSources, int chunkSize) {
            super(buildSources, chunkSize);
        }
    }

    private static class BuildOrProbeContext implements Context {
        final int chunkSize;
        final SharedContext sharedContext;
        final ChunkSource.GetContext[] getContexts;

        private BuildOrProbeContext(ColumnSource<?>[] buildSources, int chunkSize) {
            Assert.gtZero(chunkSize, "chunkSize");
            this.chunkSize = chunkSize;
            if (buildSources.length > 1) {
                sharedContext = SharedContext.makeSharedContext();
            } else {
                sharedContext = null;
            }
            getContexts = makeGetContexts(buildSources, sharedContext, chunkSize);
        }

        void resetSharedContexts() {
            if (sharedContext != null) {
                sharedContext.reset();
            }
        }

        void closeSharedContexts() {
            if (sharedContext != null) {
                sharedContext.close();
            }
        }

        @Override
        public void close() {
            closeArray(getContexts);
            closeSharedContexts();
        }
    }

    BuildContext makeBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new BuildContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    @Override
    public ProbeContext makeProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new ProbeContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    protected void buildTable(
            final BuildContext bc,
            final RowSequence buildRows,
            final ColumnSource<?>[] buildSources,
            final BuildHandler buildHandler) {
        try (final RowSequence.Iterator rsIt = buildRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[buildSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(bc.chunkSize);
                final int nextChunkSize = chunkOk.intSize();
                doRehash(nextChunkSize);

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
            final ProbeHandler handler) {
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

    @FunctionalInterface
    public interface ProbeHandler {
        void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks);
    }

    @FunctionalInterface
    public interface BuildHandler {
        void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks);
    }

    public void doRehash(int nextChunkSize) {
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

    private static void getKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts,
            Chunk<? extends Values>[] chunks, RowSequence rowSequence) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getChunk(contexts[ii], rowSequence);
        }
    }

    private static void getPrevKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts,
            Chunk<? extends Values>[] chunks, RowSequence rowSequence) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getPrevChunk(contexts[ii], rowSequence);
        }
    }

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }

    private static ColumnSource.GetContext[] makeGetContexts(ColumnSource<?>[] sources, final SharedContext sharedState,
            int chunkSize) {
        final ColumnSource.GetContext[] contexts = new ColumnSource.GetContext[sources.length];
        for (int ii = 0; ii < sources.length; ++ii) {
            contexts[ii] = sources[ii].makeGetContext(chunkSize, sharedState);
        }
        return contexts;
    }

    @Override
    public long getRightIndex(long slot) {
        return rightRowKey.getUnsafe(slot);
    }

    @Override
    public RowSet getLeftIndex(long slot) {
        return leftRowSet.getUnsafe(slot);
    }

    @Override
    public String keyString(long slot) {
        throw new UnsupportedOperationException();
    }

    @Override
    void buildFromLeftSide(Table leftTable, ColumnSource<?>[] leftSources, InitialBuildContext ibc) {
        if (leftTable.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(leftTable.size(), CHUNK_SIZE);
        try (BuildContext bc = new BuildContext(leftSources, chunkSize)) {
            buildTable(bc, leftTable.getRowSet(), leftSources, new BuildHandler() {
                @Override
                public void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
                    buildFromLeftSide(chunkOk, sourceKeyChunks);
                }
            });
        }
    }

    protected abstract void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    @Override
    void convertLeftGroups(int groupingSize, InitialBuildContext ibc,
            ObjectArraySource<WritableRowSet> rowSetSource) {
        for (int ii = 0; ii < tableSize; ++ii) {
            final WritableRowSet leftRowSet = this.leftRowSet.getUnsafe(ii);
            if (leftRowSet != null) {
                if (leftRowSet.isEmpty()) {
                    throw new IllegalStateException(
                            "When converting left group position an empty LHS rowset was found!");
                }
                if (leftRowSet.size() != 1) {
                    throw new IllegalStateException(
                            "When converting left group position to row keys more than one LHS value was found!");
                }
                final long groupPosition = leftRowSet.get(0);
                this.leftRowSet.set(ii, rowSetSource.get(groupPosition));
            }
        }
    }

    @Override
    void addRightSide(RowSequence rightRowSet, ColumnSource<?>[] rightSources) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(rightRowSet.size(), CHUNK_SIZE);
        try (ProbeContext pc = new ProbeContext(rightSources, chunkSize)) {
            probeTable(pc, rightRowSet, false, rightSources,
                    (chunkOk, sourceKeyChunks) -> addRightSide(chunkOk, sourceKeyChunks));
        }
    }

    protected abstract void addRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    @Override
    WritableRowRedirection buildRowRedirectionFromHashSlot(QueryTable leftTable, boolean exactMatch,
            InitialBuildContext ibc, JoinControl.RedirectionType redirectionType) {

        // TODO: CHECK EXACT MATCH

        switch (redirectionType) {
            case Contiguous: {
                if (!leftTable.isFlat() || leftTable.getRowSet().lastRowKey() > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Left table is not flat for contiguous row redirection build!");
                }
                // we can use an array, which is perfect for a small enough flat table
                final long[] innerIndex = new long[leftTable.intSize("contiguous redirection build")];

                for (int ii = 0; ii < tableSize; ++ii) {
                    final WritableRowSet leftRowSet = this.leftRowSet.getUnsafe(ii);
                    if (leftRowSet != null) {
                        final long rightRowKeyForState = rightRowKey.getUnsafe(ii);
                        leftRowSet.forAllRowKeys(pos -> innerIndex[(int) pos] = rightRowKeyForState);
                    }
                }

                return new ContiguousWritableRowRedirection(innerIndex);
            }
            case Sparse: {
                final LongSparseArraySource sparseRedirections = new LongSparseArraySource();
                for (int ii = 0; ii < tableSize; ++ii) {
                    final WritableRowSet leftRowSet = this.leftRowSet.getUnsafe(ii);
                    if (leftRowSet != null) {
                        final long rightRowKeyForState = rightRowKey.getUnsafe(ii);
                        if (rightRowKeyForState != RowSet.NULL_ROW_KEY) {
                            leftRowSet.forAllRowKeys(pos -> sparseRedirections.set(pos, rightRowKeyForState));
                        }
                    }
                }
                return new LongColumnSourceWritableRowRedirection(sparseRedirections);
            }
            case Hash: {
                final WritableRowRedirection rowRedirection =
                        WritableRowRedirectionLockFree.FACTORY.createRowRedirection(leftTable.intSize());
                for (int ii = 0; ii < tableSize; ++ii) {
                    final WritableRowSet leftRowSet = this.leftRowSet.getUnsafe(ii);
                    if (leftRowSet != null) {
                        final long rightRowKeyForState = rightRowKey.getUnsafe(ii);
                        if (rightRowKeyForState != RowSet.NULL_ROW_KEY) {
                            leftRowSet.forAllRowKeys(pos -> rowRedirection.put(pos, rightRowKeyForState));
                        }
                    }
                }
                return rowRedirection;
            }
        }
        throw new IllegalStateException("Bad redirectionType: " + redirectionType);

    }

    @Override
    WritableRowRedirection buildRowRedirectionFromHashSlotGrouped(QueryTable leftTable,
            ObjectArraySource<WritableRowSet> rowSetSource, int groupingSize, boolean exactMatch,
            InitialBuildContext ibc, JoinControl.RedirectionType redirectionType) {
        return buildRowRedirectionFromHashSlot(leftTable, exactMatch, ibc, redirectionType);
    }

    @Override
    void applyRightShift(Context pc, ColumnSource<?>[] rightSources, RowSet shiftedRowSet, long shiftDelta,
            @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        probeTable((ProbeContext) pc, shiftedRowSet, false, rightSources, (chunkOk,
                sourceKeyChunks) -> applyRightShift(chunkOk, sourceKeyChunks, shiftDelta, modifiedSlotTracker));
    }

    protected abstract void applyRightShift(RowSequence rowSequence, Chunk[] sourceKeyChunks, long shiftDelta,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    void modifyByRight(Context pc, RowSet modified, ColumnSource<?>[] rightSources,
            @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        probeTable((ProbeContext) pc, modified, false, rightSources,
                (chunkOk, sourceKeyChunks) -> modifyByRight(chunkOk, sourceKeyChunks, modifiedSlotTracker));
    }

    protected abstract void modifyByRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    void removeRight(Context pc, RowSequence rightRowSet, ColumnSource<?>[] rightSources,
            @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        probeTable((ProbeContext) pc, rightRowSet, true, rightSources,
                (chunkOk, sourceKeyChunks) -> removeRight(chunkOk, sourceKeyChunks, modifiedSlotTracker));
    }

    protected abstract void removeRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    void addRightSide(Context pc, RowSequence rightRowSet, ColumnSource<?>[] rightSources,
            @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        probeTable((ProbeContext) pc, rightRowSet, false, rightSources,
                (chunkOk, sourceKeyChunks) -> addRightSide(chunkOk, sourceKeyChunks, modifiedSlotTracker));
    }

    protected abstract void addRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    protected void decorateLeftSide(RowSet leftRowSet, ColumnSource<?>[] leftSources,
            LongArraySource leftRedirections) {
        throw new UnsupportedOperationException("Not used with right incremental.");
    }

    @Override
    public InitialBuildContext makeInitialBuildContext(Table leftTable) {
        // we don't need this for our open addressed rehashing version
        return null;
    }
}

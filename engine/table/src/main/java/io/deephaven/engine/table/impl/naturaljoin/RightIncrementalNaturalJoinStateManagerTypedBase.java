//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.NaturalJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.RightIncrementalNaturalJoinStateManager;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.BuildContext;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.ProbeContext;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;
import static io.deephaven.engine.table.impl.JoinControl.MAX_TABLE_SIZE;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

public abstract class RightIncrementalNaturalJoinStateManagerTypedBase extends RightIncrementalNaturalJoinStateManager {

    // the number of slots in our table
    protected int tableSize;

    protected long numEntries = 0;

    // the table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table
    private final double maximumLoadFactor;

    // the keys for our hash entries
    protected final ChunkType[] chunkTypes;
    protected final WritableColumnSource<?>[] mainKeySources;

    protected ImmutableObjectArraySource<WritableRowSet> leftRowSet =
            new ImmutableObjectArraySource<>(WritableRowSet.class, null);
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
            chunkTypes[ii] = tableKeySources[ii].getChunkType();
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
            final TypedHasherUtil.BuildHandler buildHandler) {
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

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }

    @Override
    public long getRightIndex(int slot) {
        return rightRowKey.getUnsafe(slot);
    }

    @Override
    public RowSet getLeftIndex(int slot) {
        return leftRowSet.getUnsafe(slot);
    }

    @Override
    public String keyString(int slot) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void buildFromLeftSide(Table leftTable, ColumnSource<?>[] leftSources, InitialBuildContext ibc) {
        if (leftTable.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(leftTable.size(), CHUNK_SIZE);
        try (BuildContext bc = new BuildContext(leftSources, chunkSize)) {
            buildTable(bc, leftTable.getRowSet(), leftSources, this::buildFromLeftSide);
        }
    }

    protected abstract void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    @Override
    public void convertLeftDataIndex(int groupingSize, InitialBuildContext ibc, ColumnSource<RowSet> rowSetSource) {
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
                this.leftRowSet.set(ii, rowSetSource.get(leftRowSet.firstRowKey()).copy());
            }
        }
    }

    @Override
    public void addRightSide(RowSequence rightRowSet, ColumnSource<?>[] rightSources) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(rightRowSet.size(), CHUNK_SIZE);
        try (ProbeContext pc = makeProbeContext(rightSources, chunkSize)) {
            probeTable(pc, rightRowSet, false, rightSources,
                    (chunkOk, sourceKeyChunks) -> addRightSide(chunkOk, sourceKeyChunks));
        }
    }

    protected abstract void addRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    @Override
    public WritableRowRedirection buildRowRedirectionFromHashSlot(QueryTable leftTable, boolean exactMatch,
            InitialBuildContext ibc, JoinControl.RedirectionType redirectionType) {

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
                        checkExactMatch(exactMatch, leftRowSet.firstRowKey(), rightRowKeyForState);
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
                            checkExactMatch(exactMatch, leftRowSet.firstRowKey(), rightRowKeyForState);
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
                            checkExactMatch(exactMatch, leftRowSet.firstRowKey(), rightRowKeyForState);
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
    public WritableRowRedirection buildRowRedirectionFromHashSlotIndexed(QueryTable leftTable,
            ColumnSource<RowSet> rowSetSource, int groupingSize, boolean exactMatch,
            InitialBuildContext ibc, JoinControl.RedirectionType redirectionType) {
        return buildRowRedirectionFromHashSlot(leftTable, exactMatch, ibc, redirectionType);
    }

    @Override
    public void applyRightShift(Context pc, ColumnSource<?>[] rightSources, RowSet shiftedRowSet, long shiftDelta,
            @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (shiftedRowSet.isEmpty()) {
            return;
        }
        probeTable((ProbeContext) pc, shiftedRowSet, false, rightSources, (chunkOk,
                sourceKeyChunks) -> applyRightShift(chunkOk, sourceKeyChunks, shiftDelta, modifiedSlotTracker));
    }

    protected abstract void applyRightShift(RowSequence rowSequence, Chunk[] sourceKeyChunks, long shiftDelta,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    public void modifyByRight(Context pc, RowSet modified, ColumnSource<?>[] rightSources,
            @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (modified.isEmpty()) {
            return;
        }
        probeTable((ProbeContext) pc, modified, false, rightSources,
                (chunkOk, sourceKeyChunks) -> modifyByRight(chunkOk, sourceKeyChunks, modifiedSlotTracker));
    }

    protected abstract void modifyByRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    public void removeRight(Context pc, RowSequence rightRowSet, ColumnSource<?>[] rightSources,
            @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        probeTable((ProbeContext) pc, rightRowSet, true, rightSources,
                (chunkOk, sourceKeyChunks) -> removeRight(chunkOk, sourceKeyChunks, modifiedSlotTracker));
    }

    protected abstract void removeRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    public void addRightSide(Context pc, RowSequence rightRowSet, ColumnSource<?>[] rightSources,
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

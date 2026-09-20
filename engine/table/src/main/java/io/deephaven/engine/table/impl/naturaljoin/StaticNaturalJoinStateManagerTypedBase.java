//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.util.TypedHasherUtil;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.BuildContext;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.ProbeContext;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.QueryConstants;

import java.util.function.LongUnaryOperator;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;
import static io.deephaven.engine.table.impl.JoinControl.MAX_TABLE_SIZE;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

public abstract class StaticNaturalJoinStateManagerTypedBase extends StaticHashedNaturalJoinStateManager {

    public static final long NO_RIGHT_STATE_VALUE = RowSet.NULL_ROW_KEY;
    public static final long EMPTY_RIGHT_STATE = QueryConstants.NULL_LONG;
    // errorOnDuplicates recognizes duplicate slots by comparing the stored state against DUPLICATE_RIGHT_VALUE
    public static final long DUPLICATE_RIGHT_STATE = DUPLICATE_RIGHT_VALUE;

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

    protected StaticNaturalJoinStateManagerTypedBase(
            ColumnSource<?>[] tableKeySources,
            ColumnSource<?>[] keySourcesForErrorMessages,
            int tableSize,
            double maximumLoadFactor,
            NaturalJoinType joinType,
            boolean addOnly) {
        super(keySourcesForErrorMessages, joinType, addOnly);

        this.tableSize = tableSize;
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

    private class LeftBuildHandler implements TypedHasherUtil.BuildHandler {
        final IntegerArraySource leftHashSlots;
        long offset = 0;

        private LeftBuildHandler(IntegerArraySource leftHashSlots) {
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
        final LongArraySource leftRedirections;
        /** maps a probed row key to a row key of {@code keySourcesForErrorMessages}, for the duplicate key error */
        final LongUnaryOperator probedRowKeyToErrorRowKey;
        long offset = 0;

        private LeftProbeHandler(LongArraySource leftRedirections, LongUnaryOperator probedRowKeyToErrorRowKey) {
            this.leftRedirections = leftRedirections;
            this.probedRowKeyToErrorRowKey = probedRowKeyToErrorRowKey;
        }

        @Override
        public void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            leftRedirections.ensureCapacity(offset + chunkOk.intSize());
            decorateLeftSide(chunkOk, sourceKeyChunks, leftRedirections, offset, probedRowKeyToErrorRowKey);
            offset += chunkOk.intSize();
        }
    }

    @Override
    public void buildFromLeftSide(Table leftTable, ColumnSource<?>[] leftSources, IntegerArraySource leftHashSlots) {
        if (leftTable.isEmpty()) {
            return;
        }
        try (final BuildContext bc = makeBuildContext(leftSources, leftTable.size())) {
            buildTable(bc, leftTable.getRowSet(), leftSources, new LeftBuildHandler(leftHashSlots));
        }
    }

    abstract protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            IntegerArraySource leftHashSlots, long hashSlotOffset);

    @Override
    public void buildFromRightSide(Table rightTable, ColumnSource<?>[] rightSources) {
        if (rightTable.isEmpty()) {
            return;
        }
        try (final BuildContext bc = makeBuildContext(rightSources, rightTable.size())) {
            buildTable(bc, rightTable.getRowSet(), rightSources,
                    this::buildFromRightSide);
        }
    }

    abstract protected void buildFromRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    @Override
    public void decorateLeftSide(RowSet leftRowSet, ColumnSource<?>[] leftSources, LongArraySource leftRedirections) {
        // the probed rows are left table rows, which is the keyspace of the error message key sources
        decorateLeftSide(leftRowSet, leftSources, leftRedirections, LongUnaryOperator.identity());
    }

    @Override
    public void decorateLeftSideIndexed(RowSet indexTableRowSet, ColumnSource<?>[] indexSources,
            ColumnSource<RowSet> indexRowSets, LongArraySource leftRedirections) {
        // the probed rows are data index table rows, so a duplicate key error is rendered from the first left row of
        // the offending group
        decorateLeftSide(indexTableRowSet, indexSources, leftRedirections,
                (long indexRowKey) -> indexRowSets.get(indexRowKey).firstRowKey());
    }

    private void decorateLeftSide(RowSet probeRowSet, ColumnSource<?>[] probeSources,
            LongArraySource leftRedirections, LongUnaryOperator probedRowKeyToErrorRowKey) {
        if (probeRowSet.isEmpty()) {
            return;
        }
        try (final ProbeContext pc = makeProbeContext(probeSources, probeRowSet.size())) {
            probeTable(pc, probeRowSet, false, probeSources,
                    new LeftProbeHandler(leftRedirections, probedRowKeyToErrorRowKey));
        }
    }

    abstract protected void decorateLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource leftRedirections, long redirectionsOffset, LongUnaryOperator probedRowKeyToErrorRowKey);

    @Override
    public void decorateWithRightSide(Table rightTable, ColumnSource<?>[] rightSources) {
        if (rightTable.isEmpty()) {
            return;
        }
        try (final ProbeContext pc = makeProbeContext(rightSources, rightTable.size())) {
            probeTable(pc, rightTable.getRowSet(), false, rightSources,
                    this::decorateWithRightSide);
        }
    }

    abstract protected void decorateWithRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);


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
                            "Static naturalJoin does not permit rehashing, table must be allocated with sufficient size at the beginning of initialization.");
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

    public WritableRowRedirection buildRowRedirectionFromHashSlot(QueryTable leftTable,
            IntegerArraySource leftHashSlots, JoinControl.RedirectionType redirectionType) {
        return buildRowRedirection(leftTable, position -> mainRightRowKey.getUnsafe(leftHashSlots.getUnsafe(position)),
                redirectionType);
    }

    public WritableRowRedirection buildRowRedirectionFromRedirections(QueryTable leftTable,
            LongArraySource leftRedirections, JoinControl.RedirectionType redirectionType) {
        return buildRowRedirection(leftTable, leftRedirections::getUnsafe, redirectionType);
    }

    public WritableRowRedirection buildIndexedRowRedirectionFromRedirections(
            QueryTable leftTable,
            RowSet indexTableRowSet,
            LongArraySource leftRedirections,
            ColumnSource<RowSet> indexRowSets,
            JoinControl.RedirectionType redirectionType) {
        return buildIndexedRowRedirection(leftTable, indexTableRowSet,
                leftRedirections::getUnsafe, indexRowSets, redirectionType, true);
    }

    public WritableRowRedirection buildIndexedRowRedirectionFromHashSlots(
            QueryTable leftTable,
            RowSet indexTableRowSet,
            IntegerArraySource leftHashSlots,
            ColumnSource<RowSet> indexRowSets,
            JoinControl.RedirectionType redirectionType) {
        return buildIndexedRowRedirection(leftTable, indexTableRowSet,
                (long groupPosition) -> mainRightRowKey.getUnsafe(leftHashSlots.getUnsafe(groupPosition)), indexRowSets,
                redirectionType, false);
    }

    @Override
    public void errorOnDuplicatesIndexed(IntegerArraySource leftHashSlots, RowSet indexTableRowSet) {
        // the key sources for error messages are columns of the data index table, so the error row key is the index
        // table row key for the offending group
        errorOnDuplicates(indexTableRowSet.size(),
                (long groupPosition) -> mainRightRowKey.getUnsafe(leftHashSlots.getUnsafe(groupPosition)),
                indexTableRowSet::get);
    }

    @Override
    public void errorOnDuplicatesSingle(IntegerArraySource leftHashSlots, long size, RowSet rowSet) {
        errorOnDuplicates(size, (long position) -> mainRightRowKey.getUnsafe(leftHashSlots.getUnsafe(position)),
                rowSet::get);
    }
}

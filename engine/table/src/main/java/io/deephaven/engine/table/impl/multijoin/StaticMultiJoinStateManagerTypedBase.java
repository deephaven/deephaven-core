/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.multijoin;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.MultiJoinStateManager;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.TypedHasherUtil;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.BuildContext;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.QueryConstants;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;
import static io.deephaven.engine.table.impl.JoinControl.MAX_TABLE_SIZE;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;

public abstract class StaticMultiJoinStateManagerTypedBase implements MultiJoinStateManager {
    protected final ColumnSource<?>[] keySourcesForErrorMessages;
    private final List<LongArraySource> indexSources = new ArrayList<>();

    public static final long NO_RIGHT_STATE_VALUE = RowSet.NULL_ROW_KEY;
    public static final long EMPTY_RIGHT_STATE = QueryConstants.NULL_LONG;
    public static final long DUPLICATE_RIGHT_STATE = -2;

    // The number of slots in our hash table.
    protected int tableSize;

    // The number of entries in our hash table in use.
    protected long numEntries = 0;

    // The table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table.
    private final double maximumLoadFactor;

    // The keys for our hash entries.
    protected final ChunkType[] chunkTypes;
    protected final WritableColumnSource[] mainKeySources;

    // The output sources representing the keys of our joined table.
    protected final WritableColumnSource[] outputKeySources;

    // Store sentinel information and maps hash slots to output row keys.
    protected ImmutableLongArraySource slotToOutputRow = new ImmutableLongArraySource();

    protected StaticMultiJoinStateManagerTypedBase(ColumnSource<?>[] tableKeySources,
            ColumnSource<?>[] keySourcesForErrorMessages,
            int tableSize,
            double maximumLoadFactor) {
        this.keySourcesForErrorMessages = keySourcesForErrorMessages;

        this.tableSize = tableSize;
        Require.leq(tableSize, "tableSize", MAX_TABLE_SIZE);
        Require.gtZero(tableSize, "tableSize");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        Require.inRange(maximumLoadFactor, 0.0, 0.95, "maximumLoadFactor");

        mainKeySources = new WritableColumnSource[tableKeySources.length];
        chunkTypes = new ChunkType[tableKeySources.length];

        outputKeySources = new WritableColumnSource[tableKeySources.length];

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            chunkTypes[ii] = tableKeySources[ii].getChunkType();
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
            outputKeySources[ii] = ArrayBackedColumnSource.getMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
        }

        this.maximumLoadFactor = maximumLoadFactor;

        ensureCapacity(tableSize);
    }

    private void ensureCapacity(int tableSize) {
        slotToOutputRow.ensureCapacity(tableSize);
        for (WritableColumnSource<?> mainKeySource : mainKeySources) {
            mainKeySource.ensureCapacity(tableSize);
        }
    }

    BuildContext makeBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new BuildContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    @Override
    public void build(final Table table, ColumnSource<?>[] keySources, int tableNumber) {
        if (table.isEmpty()) {
            return;
        }
        final LongArraySource tableRedirSource = indexSources.get(tableNumber);
        try (final BuildContext bc = makeBuildContext(keySources, table.size())) {
            buildTable(bc, table.getRowSet(), keySources, new LeftBuildHandler(tableRedirSource, tableNumber));
        }
    }

    private class LeftBuildHandler implements TypedHasherUtil.BuildHandler {
        final LongArraySource tableRedirSource;
        final long tableNumber;

        private LeftBuildHandler(LongArraySource tableRedirSource, long tableNumber) {
            this.tableRedirSource = tableRedirSource;
            this.tableNumber = tableNumber;
        }

        @Override
        public void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            final long maxSize = numEntries + chunkOk.intSize();
            slotToOutputRow.ensureCapacity(maxSize);
            tableRedirSource.ensureCapacity(maxSize);
            for (WritableColumnSource src : outputKeySources) {
                src.ensureCapacity(maxSize);
            }
            buildFromLeftSide(chunkOk, sourceKeyChunks, tableRedirSource, tableNumber);
        }
    }

    protected abstract void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, long tableNumber);

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

    // produce a pretty key for error messages
    protected String keyString(Chunk[] sourceKeyChunks, int chunkPosition) {
        return ChunkUtils.extractKeyStringFromChunks(chunkTypes, sourceKeyChunks, chunkPosition);
    }

    @Override
    public long getResultSize() {
        return numEntries;
    }

    @Override
    public ColumnSource<?>[] getKeyHashTableSources() {
        return outputKeySources;
    }

    @Override
    public WritableRowRedirection getRowRedirectionForTable(int tableNumber) {
        return new LongColumnSourceWritableRowRedirection(indexSources.get(tableNumber));
    }

    @Override
    public void ensureTableCapacity(int tables) {
        while (indexSources.size() < tables) {
            final LongArraySource newRedirection = new LongArraySource();
            newRedirection.ensureCapacity(numEntries);
            indexSources.add(newRedirection);
        }
    }

    @Override
    public void setTargetLoadFactor(double targetLoadFactor) {}

    @Override
    public void setMaximumLoadFactor(double maximumLoadFactor) {}
}

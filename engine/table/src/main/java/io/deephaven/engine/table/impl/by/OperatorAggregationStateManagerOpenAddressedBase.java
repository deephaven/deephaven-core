/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;

import static io.deephaven.util.SafeCloseable.closeArray;

public abstract class OperatorAggregationStateManagerOpenAddressedBase
        implements OperatorAggregationStateManager {
    public static final int CHUNK_SIZE = ChunkedOperatorAggregationHelper.CHUNK_SIZE;
    private static final long MAX_TABLE_SIZE = 1 << 30; // maximum array size

    // the number of slots in our table
    protected int tableSize;

    protected long numEntries = 0;

    // the table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table
    private final double maximumLoadFactor;

    // the keys for our hash entries
    protected final ColumnSource[] mainKeySources;

    protected OperatorAggregationStateManagerOpenAddressedBase(ColumnSource<?>[] tableKeySources, int tableSize,
            double maximumLoadFactor) {
        this.tableSize = tableSize;
        Require.leq(tableSize, "tableSize", MAX_TABLE_SIZE);
        Require.gtZero(tableSize, "tableSize");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        Require.inRange(maximumLoadFactor, 0.0, 0.95, "maximumLoadFactor");

        mainKeySources = new ColumnSource[tableKeySources.length];

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
        }

        this.maximumLoadFactor = maximumLoadFactor;
    }

    protected abstract void build(RowSequence rowSequence, Chunk<Values>[] sourceKeyChunks);

    // protected abstract void probe(HashHandler handler, RowSequence rowSequence, Chunk<Values>[] sourceKeyChunks);

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

    // public ProbeContext makeProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
    // return new ProbeContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    // }

    protected abstract void onNextChunk(int nextChunkSize);

    protected void buildTable(
            final BuildContext bc,
            final RowSequence buildRows,
            final ColumnSource<?>[] buildSources) {
        try (final RowSequence.Iterator rsIt = buildRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[buildSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(bc.chunkSize);
                final int nextChunkSize = chunkOk.intSize();
                onNextChunk(nextChunkSize);
                doRehash(nextChunkSize);

                getKeyChunks(buildSources, bc.getContexts, sourceKeyChunks, chunkOk);

                build(chunkOk, sourceKeyChunks);

                bc.resetSharedContexts();
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

    protected abstract void rehashInternalFull(int oldSize);

    public boolean rehashRequired(int nextChunkSize) {
        return (numEntries + nextChunkSize) > (tableSize * maximumLoadFactor);
    }

    private void getKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts,
            Chunk<? extends Values>[] chunks, RowSequence rowSequence) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getChunk(contexts[ii], rowSequence);
        }
    }

    private void getPrevKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts,
            Chunk<? extends Values>[] chunks, RowSequence rowSequence) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getPrevChunk(contexts[ii], rowSequence);
        }
    }

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }

    @Override
    abstract public int findPositionForKey(Object key);

    private static ColumnSource.GetContext[] makeGetContexts(ColumnSource<?>[] sources, final SharedContext sharedState,
            int chunkSize) {
        final ColumnSource.GetContext[] contexts = new ColumnSource.GetContext[sources.length];
        for (int ii = 0; ii < sources.length; ++ii) {
            contexts[ii] = sources[ii].makeGetContext(chunkSize, sharedState);
        }
        return contexts;
    }
}

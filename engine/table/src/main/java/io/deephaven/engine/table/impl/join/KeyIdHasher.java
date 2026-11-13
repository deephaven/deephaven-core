//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;
import static io.deephaven.engine.table.impl.JoinControl.MAX_TABLE_SIZE;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

/**
 * An open-addressed hash table that gives each distinct key a dense integer id.
 * <p>
 * Ids start at zero and stay with their key; moving entries during a rehash does not change them. Callers therefore
 * keep per-key state in their own arrays indexed by id, and never need to update it when the table grows.
 * <p>
 * {@link KeyIdHasherTypedBase} only adds keys. {@link IncrementalKeyIdHasherTypedBase} also removes them, reusing their
 * ids, and grows incrementally.
 */
public abstract class KeyIdHasher {
    /**
     * The id {@link #probe} reports for a key that is not in the table, and the state of a slot that holds no key.
     */
    public static final int NULL_ID = QueryConstants.NULL_INT;

    protected static final int EMPTY_ID = NULL_ID;

    /**
     * Receives the ids for one chunk of rows.
     */
    @FunctionalInterface
    public interface IdChunkConsumer {
        /**
         * @param rows the rows of this chunk
         * @param ids the id of each row's key, parallel to {@code rows}
         */
        void accept(RowSequence rows, IntChunk<Values> ids);
    }

    // the number of slots in our table
    protected int tableSize;

    /** How many slots of the main table are occupied? */
    protected long numEntries = 0;

    protected final double maximumLoadFactor;

    // the keys for our hash entries
    protected final WritableColumnSource[] mainKeySources;

    // the id of the key in each slot, or EMPTY_ID
    protected ImmutableIntArraySource mainId = new ImmutableIntArraySource();

    // ids below nextId have been handed out
    protected int nextId = 0;

    protected KeyIdHasher(ColumnSource<?>[] tableKeySources, int tableSize, double maximumLoadFactor) {
        this.tableSize = tableSize;
        Require.leq(tableSize, "tableSize", MAX_TABLE_SIZE);
        Require.gtZero(tableSize, "tableSize");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        Require.gtZero(maximumLoadFactor, "maximumLoadFactor");
        Require.leq(maximumLoadFactor, "maximumLoadFactor", 0.95);
        this.maximumLoadFactor = maximumLoadFactor;

        mainKeySources = new WritableColumnSource[tableKeySources.length];
        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
        }
        mainId.ensureCapacity(tableSize);
    }

    /**
     * Find the id of each row's key, adding the keys that are not yet in the table.
     *
     * @param rows the rows to build
     * @param sources the key sources
     * @param consumer receives the ids, one chunk of rows at a time
     */
    public void build(
            @NotNull final RowSequence rows,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final IdChunkConsumer consumer) {
        if (rows.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(CHUNK_SIZE, rows.size());
        try (final BuildOrProbeContext bc = new BuildOrProbeContext(sources, chunkSize);
                final RowSequence.Iterator rsIt = rows.getRowSequenceIterator();
                final WritableIntChunk<Values> ids = WritableIntChunk.makeWritableChunk(chunkSize)) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[sources.length];
            startBuild();
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(chunkSize);
                final int nextChunkSize = chunkOk.intSize();
                prepareForChunk(nextChunkSize);
                getKeyChunks(sources, bc.getContexts, sourceKeyChunks, chunkOk);
                ids.setSize(nextChunkSize);
                final long oldEntries = numEntries;
                build(chunkOk, sourceKeyChunks, ids);
                onChunkBuilt(numEntries - oldEntries);
                consumer.accept(chunkOk, ids);
                bc.resetSharedContexts();
            }
        }
    }

    /**
     * Find the id of each row's key, reporting {@link #NULL_ID} for a key that is not in the table.
     *
     * @param rows the rows to probe
     * @param sources the key sources
     * @param usePrev whether to read the previous values of {@code sources}
     * @param consumer receives the ids, one chunk of rows at a time
     */
    public void probe(
            @NotNull final RowSequence rows,
            @NotNull final ColumnSource<?>[] sources,
            final boolean usePrev,
            @NotNull final IdChunkConsumer consumer) {
        if (rows.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(CHUNK_SIZE, rows.size());
        try (final BuildOrProbeContext pc = new BuildOrProbeContext(sources, chunkSize);
                final RowSequence.Iterator rsIt = rows.getRowSequenceIterator();
                final WritableIntChunk<Values> ids = WritableIntChunk.makeWritableChunk(chunkSize)) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[sources.length];
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(chunkSize);
                if (usePrev) {
                    getPrevKeyChunks(sources, pc.getContexts, sourceKeyChunks, chunkOk);
                } else {
                    getKeyChunks(sources, pc.getContexts, sourceKeyChunks, chunkOk);
                }
                ids.setSize(chunkOk.intSize());
                probe(chunkOk, sourceKeyChunks, ids);
                consumer.accept(chunkOk, ids);
                pc.resetSharedContexts();
            }
        }
    }

    /**
     * @return one more than the largest id handed out; every id is in {@code [0, idCapacity())}
     */
    public int idCapacity() {
        return nextId;
    }

    /**
     * @return the number of keys in the table
     */
    public abstract long size();

    /**
     * Called once at the start of each {@link #build}.
     */
    protected void startBuild() {}

    /**
     * Make room to insert up to {@code nextChunkSize} keys.
     */
    protected abstract void prepareForChunk(int nextChunkSize);

    /**
     * Called after each chunk is built, with the number of main table slots that it newly occupied.
     */
    protected void onChunkBuilt(final long entriesAdded) {}

    /**
     * Hand out an id for a key inserted into {@code slot}.
     */
    protected abstract int allocateId(int slot);

    protected int hashToTableLocation(final int hash) {
        return hash & (tableSize - 1);
    }

    protected abstract void build(RowSequence rowSequence, Chunk[] sourceKeyChunks, WritableIntChunk<Values> ids);

    protected abstract void probe(RowSequence rowSequence, Chunk[] sourceKeyChunks, WritableIntChunk<Values> ids);

    protected abstract void rehashInternalFull(int oldSize);
}

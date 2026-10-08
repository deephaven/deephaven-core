//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.join;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
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
 * Keys are built or probed a chunk at a time. Each row's result is its key's id and a status: {@link #FOUND} or
 * {@link #ADDED} for a build, {@link #FOUND} or {@link #MISSING} for a probe. The rows can come from a row sequence and
 * column sources, through {@link #build(RowSequence, ColumnSource[], IdChunkConsumer)} and
 * {@link #probe(RowSequence, ColumnSource[], boolean, IdChunkConsumer)}, or from key chunks the caller already holds,
 * through {@link #build(Context, Chunk[])} and {@link #probe(Context, Chunk[])}.
 * <p>
 * {@link KeyIdHasherTypedBase} only adds keys. {@link IncrementalKeyIdHasherTypedBase} also removes them, reusing their
 * ids, and grows incrementally.
 */
public abstract class KeyIdHasher {
    /**
     * The id {@link #probe} reports for a key that is not in the table, and the state of a slot that holds no key.
     */
    public static final int NULL_ID = QueryConstants.NULL_INT;

    /** The status of a row whose key was already in the table. */
    public static final byte FOUND = 0;
    /** The status of a row whose key a build added to the table, with a new or reused id. */
    public static final byte ADDED = 1;
    /** The status of a probed row whose key is not in the table; its id is {@link #NULL_ID}. */
    public static final byte MISSING = 2;

    protected static final int EMPTY_ID = NULL_ID;

    /**
     * Receives the ids for one chunk of rows.
     */
    @FunctionalInterface
    public interface IdChunkConsumer {
        /**
         * @param rows the rows of this chunk
         * @param ids the id of each row's key, parallel to {@code rows}
         * @param statuses the status of each row, parallel to {@code rows}
         */
        void accept(RowSequence rows, IntChunk<Values> ids, ByteChunk<Values> statuses);
    }

    /**
     * The chunks one build or probe writes its results into, and the progress of any incremental rehash across the
     * chunks of one operation. A context belongs to the hasher that made it, and serves one operation at a time.
     */
    public static final class Context implements SafeCloseable {
        private final int capacity;
        private final WritableIntChunk<Values> ids;
        private final WritableByteChunk<Values> statuses;
        /** The rehash work done beyond what this context's insertions required, in entries. */
        int rehashCredits;

        private Context(final int capacity) {
            this.capacity = capacity;
            ids = WritableIntChunk.makeWritableChunk(capacity);
            statuses = WritableByteChunk.makeWritableChunk(capacity);
        }

        /**
         * @return the ids from the last build or probe with this context
         */
        public IntChunk<Values> ids() {
            return ids;
        }

        /**
         * @return the statuses from the last build or probe with this context
         */
        public ByteChunk<Values> statuses() {
            return statuses;
        }

        private void setSize(final int size) {
            Require.leq(size, "size", capacity, "capacity");
            ids.setSize(size);
            statuses.setSize(size);
        }

        @Override
        public void close() {
            ids.close();
            statuses.close();
        }
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
     * @param chunkCapacity the largest chunk the context will build or probe
     * @return a context for building or probing key chunks with this hasher
     */
    public Context makeContext(final int chunkCapacity) {
        return new Context(chunkCapacity);
    }

    /**
     * Find the id of each key in {@code keyChunks}, adding the keys that are not yet in the table. The results are in
     * the context's {@link Context#ids() ids} and {@link Context#statuses() statuses}.
     *
     * @param context a context from {@link #makeContext}
     * @param keyChunks one chunk per key column, all the same size, at most the context's capacity
     */
    public void build(@NotNull final Context context, @NotNull final Chunk<? extends Values>[] keyChunks) {
        final int size = keyChunks[0].size();
        context.setSize(size);
        if (size == 0) {
            return;
        }
        prepareForChunk(context, size);
        final long oldEntries = numEntries;
        build(keyChunks, context.ids, context.statuses);
        onChunkBuilt(context, numEntries - oldEntries);
    }

    /**
     * Find the id of each key in {@code keyChunks}, reporting {@link #NULL_ID} for a key that is not in the table. The
     * results are in the context's {@link Context#ids() ids} and {@link Context#statuses() statuses}.
     *
     * @param context a context from {@link #makeContext}
     * @param keyChunks one chunk per key column, all the same size, at most the context's capacity
     */
    public void probe(@NotNull final Context context, @NotNull final Chunk<? extends Values>[] keyChunks) {
        final int size = keyChunks[0].size();
        context.setSize(size);
        if (size == 0) {
            return;
        }
        probe(keyChunks, context.ids, context.statuses);
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
        forEachChunk(rows, sources, false, true, consumer);
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
        forEachChunk(rows, sources, usePrev, false, consumer);
    }

    private void forEachChunk(
            @NotNull final RowSequence rows,
            @NotNull final ColumnSource<?>[] sources,
            final boolean usePrev,
            final boolean build,
            @NotNull final IdChunkConsumer consumer) {
        if (rows.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(CHUNK_SIZE, rows.size());
        try (final BuildOrProbeContext bc = new BuildOrProbeContext(sources, chunkSize);
                final RowSequence.Iterator rsIt = rows.getRowSequenceIterator();
                final Context context = makeContext(chunkSize)) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[sources.length];
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(chunkSize);
                if (usePrev) {
                    getPrevKeyChunks(sources, bc.getContexts, sourceKeyChunks, chunkOk);
                } else {
                    getKeyChunks(sources, bc.getContexts, sourceKeyChunks, chunkOk);
                }
                if (build) {
                    build(context, sourceKeyChunks);
                } else {
                    probe(context, sourceKeyChunks);
                }
                consumer.accept(chunkOk, context.ids, context.statuses);
                bc.resetSharedContexts();
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
     * Make room to insert up to {@code nextChunkSize} keys.
     */
    protected abstract void prepareForChunk(Context context, int nextChunkSize);

    /**
     * Called after each chunk is built, with the number of main table slots that it newly occupied.
     */
    protected void onChunkBuilt(final Context context, final long entriesAdded) {}

    /**
     * Hand out an id for a key inserted into {@code slot}.
     */
    protected abstract int allocateId(int slot);

    protected int hashToTableLocation(final int hash) {
        return hash & (tableSize - 1);
    }

    protected abstract void build(Chunk[] sourceKeyChunks, WritableIntChunk<Values> ids,
            WritableByteChunk<Values> statuses);

    protected abstract void probe(Chunk[] sourceKeyChunks, WritableIntChunk<Values> ids,
            WritableByteChunk<Values> statuses);

    protected abstract void rehashInternalFull(int oldSize);
}

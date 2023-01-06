/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.datastructures.LongRangeConsumer;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

/**
 *
 * @param <ATTR> the attribute describing what kind of chunks are produced by this source
 */
public interface ChunkSource<ATTR extends Any> extends FillContextMaker, GetContextMaker {

    ChunkSource[] ZERO_LENGTH_CHUNK_SOURCE_ARRAY = new ChunkSource[0];

    FillContext DEFAULT_FILL_INSTANCE = new FillContext() {
        @Override
        public boolean supportsUnboundedFill() {
            return true;
        }
    };

    /**
     * Get the most suitable {@link ChunkType} for use with this ChunkSource.
     *
     * @return The ChunkType
     */
    ChunkType getChunkType();

    /**
     * Returns a chunk of data corresponding to the keys from the given {@link RowSequence}.
     *
     * @param context A context containing all mutable/state related data used in retrieving the Chunk. In particular,
     *        the Context may be used to provide a Chunk data pool
     * @param rowSequence An {@link RowSequence} representing the keys to be fetched
     * @return A chunk of data corresponding to the keys from the given {@link RowSequence}
     *
     * @apiNote
     *          <p>
     *          The returned chunk belongs to the ColumnSource and may be mutated as result of calling getChunk again
     *          under the same context or as a result of the column source itself being mutated. The callee is not
     *          supposed to keep references to the chunk beyond the scope of the call.
     *          </p>
     *          <p>
     *          Post-condition: The retrieved values start at position 0 in the chunk.
     *          </p>
     *          <p>
     *          Post-condition: destination.size() will be equal to rowSequence.size()
     *          </p>
     */
    Chunk<? extends ATTR> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence);

    /**
     * Same as {@link #getChunk(GetContext, RowSequence)}, except that you pass in the begin and last keys representing
     * the begin and last (inclusive) keys of a single range rather than an {@link RowSequence}. Typically you want to
     * call this only if you don't have an {@link RowSequence}, such as during an
     * {@link RowSequence#forAllRowKeyRanges(LongRangeConsumer)} call. In this case, it allows you to avoid creating an
     * intermediary {@link RowSequence} object.
     *
     * @param context A context containing all mutable/state related data used in retrieving the Chunk. In particular,
     *        the Context may be used to provide a Chunk data pool
     * @param firstKey The beginning key (inclusive) of the range to fetch in the chunk
     * @param lastKey The last key (inclusive) of the range to fetch in the chunk
     *
     * @apiNote
     *          <p>
     *          [beginKey,lastRowKey] must be a range that exists in this ChunkSource. This is unchecked.
     *          </p>
     *          <p>
     *          Post-condition: The retrieved values start at position 0 in the chunk.
     *          </p>
     *          <p>
     *          Post-condition: destination.size() will be equal to lastRowKey - beginKey + 1
     *          </p>
     */
    Chunk<? extends ATTR> getChunk(@NotNull GetContext context, long firstKey, long lastKey);

    /**
     * Populates the given destination chunk with data corresponding to the keys from the given {@link RowSequence}.
     *
     * @param context A context containing all mutable/state related data used in retrieving the Chunk.
     * @param destination The chunk to be populated according to {@code rowSequence}. No assumptions shall be made about
     *        the size of the chunk shall be made. The chunk will be populated from position [0,rowSequence.size()).
     * @param rowSequence An {@link RowSequence} representing the keys to be fetched
     * @apiNote
     *          <p>
     *          Post-condition: The retrieved values start at position 0 in the chunk.
     *          </p>
     *          <p>
     *          Post-condition: destination.size() will be equal to rowSequence.size()
     *          </p>
     */
    void fillChunk(
            @NotNull FillContext context,
            @NotNull WritableChunk<? super ATTR> destination,
            @NotNull RowSequence rowSequence);

    /**
     * Marker interface for {@link Context}s that are used in {@link #getChunk(GetContext, RowSequence)}.
     */
    interface GetContext extends Context {
    }

    /**
     * Marker interface for {@link Context}s that are used in
     * {@link #fillChunk(FillContext, WritableChunk, RowSequence)}.
     */
    interface FillContext extends Context {
        /**
         * Returns true if this Context contains internal state that limits its capacity to the originally requested
         * capacity. If false is returned, then fillChunk operations may be arbitrarily large.
         * 
         * @return if this context has a limited capacity
         */
        default boolean supportsUnboundedFill() {
            return false;
        }
    }

    /**
     * Sub-interface for ChunkSources that support previous value retrieval.
     *
     * @param <ATTR>
     */
    interface WithPrev<ATTR extends Any> extends ChunkSource<ATTR> {
        WithPrev[] ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY = new WithPrev[0];

        // TODO: Deprecate or remove getPrevChunk and fillPrevChunk if/when we do away with getPrev methods

        /**
         * Returns a chunk of previous data corresponding to the keys from the given {@link RowSequence}.
         *
         * @param context A context containing all mutable/state related data used in retrieving the Chunk. In
         *        particular, the Context may be used to provide a Chunk data pool
         * @param rowSequence An {@link RowSequence} representing the keys to be fetched
         * @return A chunk of data corresponding to the keys from the given {@link RowSequence}
         * @apiNote
         *          <p>
         *          The returned chunk belongs to the ColumnSource and may be mutated as result of calling getChunk
         *          again under the same context or as a result of the column source itself being mutated. The callee is
         *          not supposed to keep references to the chunk beyond the scope of the call.
         *          </p>
         *          <p>
         *          Post-condition: The retrieved values start at position 0 in the chunk.
         *          <p>
         *          Post-condition: destination.size() will be equal to rowSequence.size()
         */
        Chunk<? extends ATTR> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence);

        /**
         * Same as {@link #getPrevChunk(GetContext, RowSequence)}, except that you pass in the begin and last keys
         * representing the begin and last (inclusive) keys of a single range rather than an {@link RowSequence}.
         */
        Chunk<? extends ATTR> getPrevChunk(@NotNull GetContext context, long firstKey, long lastKey);

        /**
         * Populates the given destination chunk with data corresponding to the keys from the given {@link RowSequence}.
         *
         * @param context A context containing all mutable/state related data used in retrieving the Chunk.
         * @param destination The chunk to be populated according to {@code rowSequence}. No assumptions shall be made
         *        about the size of the chunk shall be made. The chunk will be populated from position
         *        [0,rowSequence.size()).
         * @param rowSequence An {@link RowSequence} representing the keys to be fetched
         * @apiNote
         *          <p>
         *          Post-condition: The retrieved values start at position 0 in the chunk.
         *          </p>
         *          <p>
         *          Post-condition: destination.size() will be equal to rowSequence.size()
         *          </p>
         */
        void fillPrevChunk(
                @NotNull FillContext context,
                @NotNull WritableChunk<? super ATTR> destination,
                @NotNull RowSequence rowSequence);

        /**
         * @return a chunk source which accesses the previous values.
         */

        ChunkSource<ATTR> getPrevSource();
    }
}

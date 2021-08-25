package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.v2.utils.LongRangeConsumer;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

/**
 *
 * @param <ATTR> the attribute describing what kind of chunks are produced by this source
 */
public interface ChunkSource<ATTR extends Attributes.Any> extends FillContextMaker, GetContextMaker {

    ChunkSource[] ZERO_LENGTH_CHUNK_SOURCE_ARRAY = new ChunkSource[0];

    FillContext DEFAULT_FILL_INSTANCE = new FillContext() {};

    /**
     * Get the most suitable {@link ChunkType} for use with this ChunkSource.
     *
     * @return The ChunkType
     */
    ChunkType getChunkType();

    /**
     * Returns a chunk of data corresponding to the keys from the given {@link OrderedKeys}.
     *
     * @param context A context containing all mutable/state related data used in retrieving the Chunk. In particular,
     *        the Context may be used to provide a Chunk data pool
     * @param orderedKeys An {@link OrderedKeys} representing the keys to be fetched
     * @return A chunk of data corresponding to the keys from the given {@link OrderedKeys}
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
     *          Post-condition: destination.size() will be equal to orderedKeys.size()
     *          </p>
     */
    Chunk<? extends ATTR> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys);

    /**
     * Same as {@link #getChunk(GetContext, OrderedKeys)}, except that you pass in the begin and last keys representing
     * the begin and last (inclusive) keys of a single range rather than an {@link OrderedKeys}. Typically you want to
     * call this only if you don't have an {@link OrderedKeys}, such as during an
     * {@link OrderedKeys#forAllLongRanges(LongRangeConsumer)} call. In this case, it allows you to avoid creating an
     * intermediary {@link OrderedKeys} object.
     *
     * @param context A context containing all mutable/state related data used in retrieving the Chunk. In particular,
     *        the Context may be used to provide a Chunk data pool
     * @param firstKey The beginning key (inclusive) of the range to fetch in the chunk
     * @param lastKey The last key (inclusive) of the range to fetch in the chunk
     *
     * @apiNote
     *          <p>
     *          [beginKey,lastKey] must be a range that exists in this ChunkSource. This is unchecked.
     *          </p>
     *          <p>
     *          Post-condition: The retrieved values start at position 0 in the chunk.
     *          </p>
     *          <p>
     *          Post-condition: destination.size() will be equal to lastKey - beginKey + 1
     *          </p>
     */
    Chunk<? extends ATTR> getChunk(@NotNull GetContext context, long firstKey, long lastKey);

    /**
     * Populates the given destination chunk with data corresponding to the keys from the given {@link OrderedKeys}.
     *
     * @param context A context containing all mutable/state related data used in retrieving the Chunk.
     * @param destination The chunk to be populated according to {@code orderedKeys}. No assumptions shall be made about
     *        the size of the chunk shall be made. The chunk will be populated from position [0,orderedKeys.size()).
     * @param orderedKeys An {@link OrderedKeys} representing the keys to be fetched
     * @apiNote
     *          <p>
     *          Post-condition: The retrieved values start at position 0 in the chunk.
     *          </p>
     *          <p>
     *          Post-condition: destination.size() will be equal to orderedKeys.size()
     *          </p>
     */
    void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination,
            @NotNull OrderedKeys orderedKeys);

    /**
     * Marker interface for {@link Context}s that are used in {@link #getChunk(GetContext, OrderedKeys)}.
     */
    interface GetContext extends Context {
    }

    /**
     * Marker interface for {@link Context}s that are used in
     * {@link #fillChunk(FillContext, WritableChunk, OrderedKeys)}.
     */
    interface FillContext extends Context {
    }

    /**
     * Sub-interface for ChunkSources that support previous value retrieval.
     *
     * @param <ATTR>
     */
    interface WithPrev<ATTR extends Attributes.Any> extends ChunkSource<ATTR> {
        WithPrev[] ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY = new WithPrev[0];

        // TODO: Deprecate or remove getPrevChunk and fillPrevChunk if/when we do away with getPrev methods

        /**
         * Returns a chunk of previous data corresponding to the keys from the given {@link OrderedKeys}.
         *
         * @param context A context containing all mutable/state related data used in retrieving the Chunk. In
         *        particular, the Context may be used to provide a Chunk data pool
         * @param orderedKeys An {@link OrderedKeys} representing the keys to be fetched
         * @return A chunk of data corresponding to the keys from the given {@link OrderedKeys}
         * @apiNote
         *          <p>
         *          The returned chunk belongs to the ColumnSource and may be mutated as result of calling getChunk
         *          again under the same context or as a result of the column source itself being mutated. The callee is
         *          not supposed to keep references to the chunk beyond the scope of the call.
         *          </p>
         *          <p>
         *          Post-condition: The retrieved values start at position 0 in the chunk.
         *          <p>
         *          Post-condition: destination.size() will be equal to orderedKeys.size()
         */
        Chunk<? extends ATTR> getPrevChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys);

        /**
         * Same as {@link #getPrevChunk(GetContext, OrderedKeys)}, except that you pass in the begin and last keys
         * representing the begin and last (inclusive) keys of a single range rather than an {@link OrderedKeys}.
         */
        Chunk<? extends ATTR> getPrevChunk(@NotNull GetContext context, long firstKey, long lastKey);

        /**
         * Populates the given destination chunk with data corresponding to the keys from the given {@link OrderedKeys}.
         *
         * @param context A context containing all mutable/state related data used in retrieving the Chunk.
         * @param destination The chunk to be populated according to {@code orderedKeys}. No assumptions shall be made
         *        about the size of the chunk shall be made. The chunk will be populated from position
         *        [0,orderedKeys.size()).
         * @param orderedKeys An {@link OrderedKeys} representing the keys to be fetched
         * @apiNote
         *          <p>
         *          Post-condition: The retrieved values start at position 0 in the chunk.
         *          </p>
         *          <p>
         *          Post-condition: destination.size() will be equal to orderedKeys.size()
         *          </p>
         */
        void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination,
                @NotNull OrderedKeys orderedKeys);

        /**
         * @return a chunk source which accesses the previous values.
         */

        ChunkSource<ATTR> getPrevSource();
    }
}

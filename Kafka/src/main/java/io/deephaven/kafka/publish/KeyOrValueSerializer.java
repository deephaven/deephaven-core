package io.deephaven.kafka.publish;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.SafeCloseable;

/**
 * Chunk-oriented serializer that supplies keys or values for stream publishing.
 */
public interface KeyOrValueSerializer<SERIALIZED_TYPE> {
    /**
     * Create a chunk of output keys or values that correspond to {@code orderedKeys}. The output {@link ObjectChunk
     * chunks} should be cached in the {@code context} for re-use, but the data returned in them should be functionally
     * immutable and not rely on pooled or re-usable objects.
     *
     * @param context A {@link Context} created by {@link #makeContext(int)}
     * @param orderedKeys The row keys to serialize
     * @param previous If previous row values should be used, as with row key removals
     *
     * @return A chunk of serialized data keys or values, with {@code ObjectChunk.size() == orderedKeys.size()}
     */
    ObjectChunk<SERIALIZED_TYPE, Attributes.Values> handleChunk(Context context, OrderedKeys orderedKeys,
            boolean previous);

    /**
     * Create a context for calling {@link #handleChunk(Context, OrderedKeys, boolean)}.
     *
     * @param size The maximum number of rows that will be serialized for each chunk
     *
     * @return A Context for the KeyOrValueSerializer
     */
    Context makeContext(int size);

    /**
     * Context interface.
     */
    interface Context extends SafeCloseable {
    }
}

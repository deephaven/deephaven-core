package io.deephaven.kafka.publish;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.SafeCloseable;

/**
 * Chunk-oriented serializer that supplies keys or values for stream publishing.
 */
public interface KeyOrValueSerializer<SERIALIZED_TYPE> {
    /**
     * Create a chunk of output keys or values that correspond to {@code rowSequence}. The output {@link ObjectChunk
     * chunks} should be cached in the {@code context} for re-use, but the data returned in them should be functionally
     * immutable and not rely on pooled or re-usable objects.
     *
     * @param context A {@link Context} created by {@link #makeContext(int)}
     * @param rowSequence The row keys to serialize
     * @param previous If previous row values should be used, as with row key removals
     *
     * @return A chunk of serialized data keys or values, with {@code ObjectChunk.size() == rowSequence.size()}
     */
    ObjectChunk<SERIALIZED_TYPE, Values> handleChunk(Context context, RowSequence rowSequence,
            boolean previous);

    /**
     * Create a context for calling {@link #handleChunk(Context, RowSequence, boolean)}.
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

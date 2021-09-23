package io.deephaven.kafka.publish;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.SafeCloseable;

public interface KeyOrValueSerializer<SERIALIZED_TYPE> {
    /**
     * Create a chunk of output values from a table and index.
     *
     * TODO: should we read the chunks and pass them in, or allow the key or value serializer to do that work?
     *
     * @param context a context created by {@link #makeContext(int)}
     * @param orderedKeys the keys to serialize
     * @param previous if previous values should be used, as with key removals
     *
     * @return a chunk of serialized values
     */
    ObjectChunk<SERIALIZED_TYPE, Attributes.Values> handleChunk(Context context, OrderedKeys orderedKeys,
            boolean previous);

    /**
     * Create a context for calling handleChunk.
     *
     * @param size the maximum number of rows that will be serialized for each chunk
     *
     * @return a Context for the KeyOrValueSerializer
     */
    Context makeContext(int size);

    interface Context extends SafeCloseable {
    }
}

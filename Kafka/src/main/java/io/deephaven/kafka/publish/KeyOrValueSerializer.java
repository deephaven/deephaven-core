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
     * @param outputChunk the chunk containing a serialized value
     * @param outputChunk the output chunk
     */
    ObjectChunk<SERIALIZED_TYPE, Attributes.Values> handleChunk(Context context, OrderedKeys orderedKeys, boolean previous);

    Context makeContext(int size);

    interface Context extends SafeCloseable {
    }
}

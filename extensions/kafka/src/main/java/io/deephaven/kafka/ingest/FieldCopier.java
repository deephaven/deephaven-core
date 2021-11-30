package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

/**
 * Copy fields from a chunk of Kafka key or value objects to a chunk that will be published to a stream table.
 */
public interface FieldCopier {
    /**
     * Copy fields from a chunk of Kafka key or value objects to a chunk that will be published to a stream table.
     * @param inputChunk     the chunk containing Kafka keys or values
     * @param publisherChunk the output chunk for the provided field
     * @param sourceOffset   the source chunk offset
     * @param destOffset     the destination chunk offset
     * @param length
     */
    void copyField(ObjectChunk<Object, Values> inputChunk, WritableChunk<Values> publisherChunk,
                   int sourceOffset, int destOffset, int length);

    interface Factory {
        FieldCopier make(String fieldName, ChunkType chunkType, Class<?> dataType);
    }
}

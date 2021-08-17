package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;

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
    void copyField(ObjectChunk<Object, Attributes.Values> inputChunk, WritableChunk<Attributes.Values> publisherChunk,
                   int sourceOffset, int destOffset, int length);

    interface Factory {
        FieldCopier make(String fieldName, ChunkType chunkType, Class<?> dataType);
    }
}

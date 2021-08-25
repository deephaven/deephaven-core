package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;

public interface KeyOrValueProcessor {
    /**
     * After consuming a set of generic records for a batch that are not raw objects, we pass the
     * keys or values to an appropriate handler. The handler must know its data types and offsets
     * within the publisher chunks, and "copy" the data from the inputChunk to the appropriate
     * chunks for the stream publisher.
     *
     * @param inputChunk the chunk containing the keys or values as Kafka deserialized them from the
     *        consumer record
     * @param publisherChunks the output chunks for this table that must be appended to.
     */
    void handleChunk(ObjectChunk<Object, Attributes.Values> inputChunk,
        WritableChunk<Attributes.Values>[] publisherChunks);
}

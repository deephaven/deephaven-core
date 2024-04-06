//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.kafka.ingest.FieldCopier;
import io.deephaven.kafka.ingest.KafkaIngesterException;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;

import java.util.List;
import java.util.Objects;

final class KeyOrValueProcessorImpl implements KeyOrValueProcessor {

    private final int[] chunkOffsets;
    private final List<FieldCopier> copiers;
    private final boolean allowNulls;

    public KeyOrValueProcessorImpl(final int[] chunkOffsets, List<FieldCopier> copiers, boolean allowNulls) {
        this.chunkOffsets = Objects.requireNonNull(chunkOffsets);
        this.copiers = Objects.requireNonNull(copiers);
        this.allowNulls = allowNulls;
    }

    @Override
    public void handleChunk(ObjectChunk<Object, Values> inputChunk, WritableChunk<Values>[] publisherChunks) {
        if (!allowNulls) {
            for (int ii = 0; ii < inputChunk.size(); ++ii) {
                if (inputChunk.get(ii) == null) {
                    throw new KafkaIngesterException("Null records are not permitted");
                }
            }
        }
        for (int i = 0; i < chunkOffsets.length; ++i) {
            final WritableChunk<Values> publisherChunk = publisherChunks[chunkOffsets[i]];
            final int existingSize = publisherChunk.size();
            publisherChunk.setSize(existingSize + inputChunk.size());
            copiers.get(i).copyField(inputChunk, publisherChunk, 0, existingSize, inputChunk.size());
        }
    }
}

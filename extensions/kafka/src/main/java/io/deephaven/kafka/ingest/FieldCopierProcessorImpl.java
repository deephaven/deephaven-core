//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.processor.ObjectProcessor;

import java.util.List;
import java.util.Objects;

final class FieldCopierProcessorImpl implements FieldCopier {

    private final ObjectProcessor<Object> processor;

    FieldCopierProcessorImpl(ObjectProcessor<Object> processor) {
        this.processor = Objects.requireNonNull(processor);
    }

    @Override
    public void copyField(ObjectChunk<Object, Values> inputChunk, WritableChunk<Values> publisherChunk,
            int sourceOffset, int destOffset, int length) {
        final int originalSize = publisherChunk.size();
        publisherChunk.setSize(destOffset);
        try {
            processor.processAll(inputChunk.slice(sourceOffset, length), List.of(publisherChunk));
        } finally {
            publisherChunk.setSize(originalSize);
        }
    }
}

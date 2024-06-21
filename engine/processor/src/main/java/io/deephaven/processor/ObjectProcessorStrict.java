//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.Objects;

final class ObjectProcessorStrict<T> implements ObjectProcessor<T> {

    static <T> ObjectProcessor<T> create(ObjectProcessor<T> delegate) {
        if (delegate instanceof ObjectProcessorStrict) {
            return delegate;
        }
        return new ObjectProcessorStrict<>(delegate);
    }

    private final ObjectProcessor<T> delegate;
    private final List<Type<?>> outputTypes;

    ObjectProcessorStrict(ObjectProcessor<T> delegate) {
        this.delegate = Objects.requireNonNull(delegate);
        this.outputTypes = List.copyOf(delegate.outputTypes());
        if (delegate.outputSize() != outputTypes.size()) {
            throw new IllegalArgumentException(
                    String.format("Inconsistent size. delegate.outputSize()=%d, delegate.outputTypes().size()=%d",
                            delegate.outputSize(), outputTypes.size()));
        }
    }

    @Override
    public int outputSize() {
        return delegate.outputSize();
    }

    @Override
    public List<Type<?>> outputTypes() {
        final List<Type<?>> outputTypes = delegate.outputTypes();
        if (!this.outputTypes.equals(outputTypes)) {
            throw new UncheckedDeephavenException("Implementation is returning a different list of outputTypes");
        }
        return outputTypes;
    }

    @Override
    public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        final int numColumns = delegate.outputSize();
        if (numColumns != out.size()) {
            throw new IllegalArgumentException(String.format(
                    "Improper number of out chunks. Expected delegate.outputSize() == out.size(). delegate.outputSize()=%d, out.size()=%d",
                    numColumns, out.size()));
        }
        final List<Type<?>> delegateOutputTypes = delegate.outputTypes();
        final int[] originalSizes = new int[numColumns];
        for (int chunkIx = 0; chunkIx < numColumns; ++chunkIx) {
            final WritableChunk<?> chunk = out.get(chunkIx);
            if (chunk.capacity() - chunk.size() < in.size()) {
                throw new IllegalArgumentException(String.format(
                        "out chunk does not have enough remaining capacity. chunkIx=%d, in.size()=%d, chunk.size()=%d, chunk.capacity()=%d",
                        chunkIx, in.size(), chunk.size(), chunk.capacity()));
            }
            final Type<?> type = delegateOutputTypes.get(chunkIx);
            final ChunkType expectedChunkType = ObjectProcessor.chunkType(type);
            final ChunkType actualChunkType = chunk.getChunkType();
            if (expectedChunkType != actualChunkType) {
                throw new IllegalArgumentException(String.format(
                        "Improper ChunkType. chunkIx=%d, outputType=%s, expectedChunkType=%s, actualChunkType=%s",
                        chunkIx, type, expectedChunkType, actualChunkType));
            }
            originalSizes[chunkIx] = chunk.size();
        }
        delegate.processAll(in, out);
        for (int chunkIx = 0; chunkIx < numColumns; ++chunkIx) {
            final WritableChunk<?> chunk = out.get(chunkIx);
            final int expectedSize = originalSizes[chunkIx] + in.size();
            if (chunk.size() != expectedSize) {
                throw new UncheckedDeephavenException(String.format(
                        "Implementation did not increment chunk size correctly. chunkIx=%d, (before) chunk.size()=%d, (after) chunk.size()=%d, in.size()=%d",
                        chunkIx, originalSizes[chunkIx], chunk.size(), in.size()));
            }
        }
    }
}

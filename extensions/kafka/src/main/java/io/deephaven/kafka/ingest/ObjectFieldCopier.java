/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.ToObjectFunction;

import java.util.Objects;

class ObjectFieldCopier implements FieldCopier {
    public static ObjectFieldCopier of(ToObjectFunction<Object, ?> f) {
        return new ObjectFieldCopier(f);
    }

    private final ToObjectFunction<Object, ?> f;

    private ObjectFieldCopier(ToObjectFunction<Object, ?> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableObjectChunk(), destOffset, length);
    }
}

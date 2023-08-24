/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.FloatFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class FloatFieldCopier implements FieldCopier {
    public static FloatFieldCopier of(FloatFunction<Object> f) {
        return new FloatFieldCopier(f);
    }

    public static FloatFieldCopier of(ObjectFunction<Object, Float> f) {
        return of(f.mapFloat(TypeUtils::unbox));
    }

    private final FloatFunction<Object> f;

    private FloatFieldCopier(FloatFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableFloatChunk(), destOffset, length);
    }
}

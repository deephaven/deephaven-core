/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.ToFloatFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class FloatFieldCopier implements FieldCopier {
    public static FloatFieldCopier of(ToFloatFunction<Object> f) {
        return new FloatFieldCopier(f);
    }

    public static FloatFieldCopier of(ToObjectFunction<Object, Float> f) {
        return of(f.mapToFloat(TypeUtils::unbox));
    }

    private final ToFloatFunction<Object> f;

    private FloatFieldCopier(ToFloatFunction<Object> f) {
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

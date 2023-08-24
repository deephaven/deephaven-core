/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.IntFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class IntFieldCopier implements FieldCopier {
    public static IntFieldCopier of(IntFunction<Object> f) {
        return new IntFieldCopier(f);
    }

    public static IntFieldCopier of(ObjectFunction<Object, Integer> f) {
        return of(f.mapInt(TypeUtils::unbox));
    }

    private final IntFunction<Object> f;

    private IntFieldCopier(IntFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableIntChunk(), destOffset, length);
    }
}

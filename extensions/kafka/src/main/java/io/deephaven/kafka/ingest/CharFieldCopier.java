/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.ToCharFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class CharFieldCopier implements FieldCopier {
    public static CharFieldCopier of(ToCharFunction<Object> f) {
        return new CharFieldCopier(f);
    }

    public static CharFieldCopier of(ToObjectFunction<Object, Character> f) {
        return of(f.mapToChar(TypeUtils::unbox));
    }

    private final ToCharFunction<Object> f;

    private CharFieldCopier(ToCharFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableCharChunk(), destOffset, length);
    }
}

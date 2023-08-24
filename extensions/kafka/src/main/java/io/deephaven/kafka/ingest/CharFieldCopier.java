/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.CharFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class CharFieldCopier implements FieldCopier {
    public static CharFieldCopier of(CharFunction<Object> f) {
        return new CharFieldCopier(f);
    }

    public static CharFieldCopier of(ObjectFunction<Object, Character> f) {
        return of(f.mapChar(TypeUtils::unbox));
    }

    private final CharFunction<Object> f;

    private CharFieldCopier(CharFunction<Object> f) {
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

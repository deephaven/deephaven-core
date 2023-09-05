/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.functions.ToShortFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class ShortFieldCopier implements FieldCopier {
    public static ShortFieldCopier of(ToShortFunction<Object> f) {
        return new ShortFieldCopier(f);
    }

    public static ShortFieldCopier of(ToObjectFunction<Object, Short> f) {
        return of(f.mapToShort(TypeUtils::unbox));
    }

    private final ToShortFunction<Object> f;

    private ShortFieldCopier(ToShortFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableShortChunk(), destOffset, length);
    }
}

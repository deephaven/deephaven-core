/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.ToLongFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class LongFieldCopier implements FieldCopier {
    public static LongFieldCopier of(ToLongFunction<Object> f) {
        return new LongFieldCopier(f);
    }

    public static LongFieldCopier of(ToObjectFunction<Object, Long> f) {
        return of(f.mapToLong(TypeUtils::unbox));
    }

    private final ToLongFunction<Object> f;

    private LongFieldCopier(ToLongFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableLongChunk(), destOffset, length);
    }
}

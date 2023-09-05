/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.ToDoubleFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class DoubleFieldCopier implements FieldCopier {
    public static DoubleFieldCopier of(ToDoubleFunction<Object> f) {
        return new DoubleFieldCopier(f);
    }

    public static DoubleFieldCopier of(ToObjectFunction<Object, Double> f) {
        return of(f.mapToDouble(TypeUtils::unbox));
    }

    private final ToDoubleFunction<Object> f;

    private DoubleFieldCopier(ToDoubleFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableDoubleChunk(), destOffset, length);
    }
}

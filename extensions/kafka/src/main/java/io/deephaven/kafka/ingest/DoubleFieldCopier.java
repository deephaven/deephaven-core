/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.DoubleFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class DoubleFieldCopier implements FieldCopier {
    public static DoubleFieldCopier of(DoubleFunction<Object> f) {
        return new DoubleFieldCopier(f);
    }

    public static DoubleFieldCopier of(ObjectFunction<Object, Double> f) {
        return of(f.mapDouble(TypeUtils::unbox));
    }

    private final DoubleFunction<Object> f;

    private DoubleFieldCopier(DoubleFunction<Object> f) {
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

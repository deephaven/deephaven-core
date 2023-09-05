/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.functions.ToByteFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;

class ByteFieldCopier implements FieldCopier {
    public static ByteFieldCopier of(ToByteFunction<Object> f) {
        return new ByteFieldCopier(f);
    }

    public static ByteFieldCopier of(ToObjectFunction<Object, Byte> f) {
        return of(f.mapToByte(TypeUtils::unbox));
    }

    public static ByteFieldCopier ofBoolean(ToObjectFunction<Object, Boolean> f) {
        return of(f.mapToByte(BooleanUtils::booleanAsByte));
    }

    private final ToByteFunction<Object> f;

    private ByteFieldCopier(ToByteFunction<Object> f) {
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void copyField(
            ObjectChunk<Object, Values> inputChunk,
            WritableChunk<Values> publisherChunk,
            int sourceOffset,
            int destOffset,
            int length) {
        ChunkUtils.applyInto(f, inputChunk, sourceOffset, publisherChunk.asWritableByteChunk(), destOffset, length);
    }
}

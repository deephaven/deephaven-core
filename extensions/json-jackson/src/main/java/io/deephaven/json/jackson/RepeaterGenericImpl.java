//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.MathUtil;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.sized.SizedObjectChunk;
import io.deephaven.qst.type.NativeArrayType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

final class RepeaterGenericImpl<T> extends RepeaterProcessorBase<T[]> {
    private final ToObject<T> toObject;
    private final SizedObjectChunk<T, ?> chunk;

    public RepeaterGenericImpl(ToObject<T> toObject, T[] onMissing, T[] onNull, NativeArrayType<T[], T> arrayType) {
        super(onMissing, onNull, arrayType);
        this.toObject = Objects.requireNonNull(toObject);
        chunk = new SizedObjectChunk<>(0);
    }

    @Override
    public void processElementImpl(JsonParser parser, int index) throws IOException {
        final int newSize = index + 1;
        final WritableObjectChunk<T, ?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
        chunk.set(index, toObject.parseValue(parser));
        chunk.setSize(newSize);
    }

    @Override
    public void processElementMissingImpl(JsonParser parser, int index) throws IOException {
        final int newSize = index + 1;
        final WritableObjectChunk<T, ?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
        chunk.set(index, toObject.parseMissing(parser));
        chunk.setSize(newSize);
    }

    @Override
    public T[] doneImpl(JsonParser parser, int length) {
        final WritableObjectChunk<T, ?> chunk = this.chunk.get();
        final T[] result = Arrays.copyOfRange(chunk.array(), chunk.arrayOffset(), chunk.arrayOffset() + length);
        chunk.fillWithNullValue(0, length);
        return result;
    }
}

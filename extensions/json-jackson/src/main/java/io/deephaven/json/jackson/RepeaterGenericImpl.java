//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.ArrayUtil;
import io.deephaven.qst.type.NativeArrayType;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Objects;

final class RepeaterGenericImpl<T> extends RepeaterProcessorBase<T[]> {
    private final ToObject<T> toObject;
    private final Class<T[]> bufferClass;
    private final Class<T> componentClass;
    private T[] buffer;

    public RepeaterGenericImpl(ToObject<T> toObject, T[] onMissing, T[] onNull, NativeArrayType<T[], T> arrayType) {
        super(onMissing, onNull, arrayType);
        this.toObject = Objects.requireNonNull(toObject);
        this.bufferClass = arrayType.clazz();
        this.componentClass = arrayType.componentType().clazz();
        // noinspection unchecked
        this.buffer = (T[]) Array.newInstance(componentClass, 0);
    }

    @Override
    public void processElementImpl(JsonParser parser, int index) throws IOException {
        buffer = ArrayUtil.put(buffer, index, toObject.parseValue(parser), componentClass);
    }

    @Override
    public void processElementMissingImpl(JsonParser parser, int index) throws IOException {
        buffer = ArrayUtil.put(buffer, index, toObject.parseMissing(parser), componentClass);
    }

    @Override
    public T[] doneImpl(JsonParser parser, int length) {
        return Arrays.copyOfRange(buffer, 0, length, bufferClass);
    }
}

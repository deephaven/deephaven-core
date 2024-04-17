//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class ObjectValueProcessor<T> implements ValueProcessor {

    private WritableObjectChunk<T, ?> out;
    private final ToObject<? extends T> toObj;
    private final GenericType<T> type;

    ObjectValueProcessor(ToObject<? extends T> toObj, GenericType<T> type) {
        this.toObj = Objects.requireNonNull(toObj);
        this.type = Objects.requireNonNull(type);
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        this.out = out.get(0).asWritableObjectChunk();
    }

    @Override
    public void clearContext() {
        out = null;
    }

    @Override
    public int numColumns() {
        return 1;
    }

    @Override
    public Stream<Type<?>> columnTypes() {
        return Stream.of(type);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(toObj.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toObj.parseMissing(parser));
    }

    interface ToObject<T> {

        T parseValue(JsonParser parser) throws IOException;

        T parseMissing(JsonParser parser) throws IOException;
    }
}

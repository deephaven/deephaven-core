//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class ShortValueProcessor implements ValueProcessor {

    private WritableShortChunk<?> out;
    private final ToShort toShort;

    ShortValueProcessor(ToShort toShort) {
        this.toShort = Objects.requireNonNull(toShort);
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        this.out = out.get(0).asWritableShortChunk();
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
        return Stream.of(Type.shortType());
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(toShort.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toShort.parseMissing(parser));
    }

    interface ToShort {

        short parseValue(JsonParser parser) throws IOException;

        short parseMissing(JsonParser parser) throws IOException;
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class FloatValueProcessor implements ValueProcessor {

    private WritableFloatChunk<?> out;
    private final ToFloat toFloat;

    FloatValueProcessor(ToFloat toFloat) {
        this.toFloat = Objects.requireNonNull(toFloat);
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        this.out = out.get(0).asWritableFloatChunk();
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
        return Stream.of(Type.floatType());
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(toFloat.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toFloat.parseMissing(parser));
    }

    interface ToFloat {

        float parseValue(JsonParser parser) throws IOException;

        float parseMissing(JsonParser parser) throws IOException;
    }
}

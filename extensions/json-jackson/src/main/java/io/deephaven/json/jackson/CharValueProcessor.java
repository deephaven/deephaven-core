//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

final class CharValueProcessor implements ValueProcessor {

    private WritableCharChunk<?> out;
    private final ToChar toChar;

    CharValueProcessor(ToChar toChar) {
        this.toChar = Objects.requireNonNull(toChar);
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        this.out = out.get(0).asWritableCharChunk();
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
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(toChar.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toChar.parseMissing(parser));
    }

    interface ToChar {

        char parseValue(JsonParser parser) throws IOException;

        char parseMissing(JsonParser parser) throws IOException;
    }
}

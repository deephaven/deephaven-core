//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableCharChunk;

import java.io.IOException;
import java.util.Objects;

final class CharValueProcessor implements ValueProcessor {

    private final WritableCharChunk<?> out;
    private final ToChar toChar;

    CharValueProcessor(WritableCharChunk<?> out, ToChar toChar) {
        this.out = Objects.requireNonNull(out);
        this.toChar = Objects.requireNonNull(toChar);
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

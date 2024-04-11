//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableShortChunk;

import java.io.IOException;
import java.util.Objects;

final class ShortValueProcessor implements ValueProcessor {

    private final WritableShortChunk<?> out;
    private final ToShort toShort;

    ShortValueProcessor(WritableShortChunk<?> out, ToShort toShort) {
        this.out = Objects.requireNonNull(out);
        this.toShort = Objects.requireNonNull(toShort);
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

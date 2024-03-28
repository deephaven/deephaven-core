//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableByteChunk;

import java.io.IOException;
import java.util.Objects;

final class BoolValueProcessor implements ValueProcessor {

    private final WritableByteChunk<?> out;
    private final ToByte toChar;

    BoolValueProcessor(WritableByteChunk<?> out, ToByte toByte) {
        this.out = Objects.requireNonNull(out);
        this.toChar = Objects.requireNonNull(toByte);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(toChar.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toChar.parseMissing(parser));
    }

    interface ToByte {

        byte parseValue(JsonParser parser) throws IOException;

        byte parseMissing(JsonParser parser) throws IOException;
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableByteChunk;

import java.io.IOException;
import java.util.Objects;

final class ByteValueProcessor implements ValueProcessor {

    private final WritableByteChunk<?> out;
    private final ToByte toByte;

    ByteValueProcessor(WritableByteChunk<?> out, ToByte toByte) {
        this.out = Objects.requireNonNull(out);
        this.toByte = Objects.requireNonNull(toByte);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(toByte.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toByte.parseMissing(parser));
    }

    interface ToByte {

        byte parseValue(JsonParser parser) throws IOException;

        byte parseMissing(JsonParser parser) throws IOException;
    }
}

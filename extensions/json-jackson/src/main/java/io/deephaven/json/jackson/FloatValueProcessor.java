//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableFloatChunk;

import java.io.IOException;
import java.util.Objects;

final class FloatValueProcessor implements ValueProcessor {

    private final WritableFloatChunk<?> out;
    private final ToFloat toFloat;

    FloatValueProcessor(WritableFloatChunk<?> out, ToFloat toFloat) {
        this.out = Objects.requireNonNull(out);
        this.toFloat = Objects.requireNonNull(toFloat);
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

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

final class DoubleValueProcessor implements ValueProcessor {

    private WritableDoubleChunk<?> out;
    private final ToDouble toDouble;

    DoubleValueProcessor(ToDouble toDouble) {
        this.toDouble = Objects.requireNonNull(toDouble);
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        this.out = out.get(0).asWritableDoubleChunk();
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
        out.add(toDouble.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toDouble.parseMissing(parser));
    }

    interface ToDouble {

        double parseValue(JsonParser parser) throws IOException;

        double parseMissing(JsonParser parser) throws IOException;
    }
}

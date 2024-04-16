//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

final class IntValueProcessor implements ValueProcessor {

    private WritableIntChunk<?> out;
    private final ToInt toInt;

    IntValueProcessor(ToInt toInt) {
        this.toInt = Objects.requireNonNull(toInt);
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        this.out = out.get(0).asWritableIntChunk();
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
        out.add(toInt.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toInt.parseMissing(parser));
    }

    interface ToInt {

        int parseValue(JsonParser parser) throws IOException;

        int parseMissing(JsonParser parser) throws IOException;
    }
}

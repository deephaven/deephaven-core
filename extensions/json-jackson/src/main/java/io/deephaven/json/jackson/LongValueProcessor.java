//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class LongValueProcessor implements ValueProcessor {
    private WritableLongChunk<?> out;
    private final ToLong toLong;

    LongValueProcessor(ToLong toLong) {
        this.toLong = Objects.requireNonNull(toLong);
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        this.out = out.get(0).asWritableLongChunk();
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
        return Stream.of(Type.longType());
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.add(toLong.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.add(toLong.parseMissing(parser));
    }

    interface ToLong {

        long parseValue(JsonParser parser) throws IOException;

        long parseMissing(JsonParser parser) throws IOException;
    }
}

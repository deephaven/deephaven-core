//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class ByteValueProcessor implements ValueProcessor {

    private WritableByteChunk<?> out;
    private final ToByte toByte;

    ByteValueProcessor(ToByte toByte) {
        this.toByte = Objects.requireNonNull(toByte);
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        this.out = out.get(0).asWritableByteChunk();
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
        return Stream.of(Type.byteType());
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

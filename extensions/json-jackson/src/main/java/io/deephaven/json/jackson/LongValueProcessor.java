//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableLongChunk;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongConsumer;

final class LongValueProcessor implements ValueProcessor {

    public static LongValueProcessor of(WritableLongChunk<?> out, ToLong toLong) {
        return new LongValueProcessor(out::add, toLong);
    }

    private final LongConsumer out;
    private final ToLong toLong;

    LongValueProcessor(LongConsumer out, ToLong toLong) {
        this.out = Objects.requireNonNull(out);
        this.toLong = Objects.requireNonNull(toLong);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.accept(toLong.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.accept(toLong.parseMissing(parser));
    }

    interface ToLong {

        long parseValue(JsonParser parser) throws IOException;

        long parseMissing(JsonParser parser) throws IOException;
    }
}

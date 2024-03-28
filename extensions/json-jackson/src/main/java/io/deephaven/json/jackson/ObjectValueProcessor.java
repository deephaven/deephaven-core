//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableObjectChunk;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

final class ObjectValueProcessor<T> implements ValueProcessor {

    public static <T> ObjectValueProcessor<T> of(WritableObjectChunk<T, ?> chunk, ToObject<? extends T> toObj) {
        return new ObjectValueProcessor<>(chunk::add, toObj);
    }

    private final Consumer<? super T> out;
    private final ToObject<? extends T> toObj;

    ObjectValueProcessor(Consumer<? super T> out, ToObject<? extends T> toObj) {
        this.out = Objects.requireNonNull(out);
        this.toObj = Objects.requireNonNull(toObj);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        out.accept(toObj.parseValue(parser));
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        out.accept(toObj.parseMissing(parser));
    }

    interface ToObject<T> {

        T parseValue(JsonParser parser) throws IOException;

        T parseMissing(JsonParser parser) throws IOException;
    }
}

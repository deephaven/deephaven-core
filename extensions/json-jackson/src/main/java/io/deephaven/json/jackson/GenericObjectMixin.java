//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.json.Value;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

abstract class GenericObjectMixin<T extends Value, TT> extends Mixin<T> implements ToObject<TT> {
    private final GenericType<TT> type;

    public GenericObjectMixin(JsonFactory factory, T options, GenericType<TT> type) {
        super(factory, options);
        this.type = Objects.requireNonNull(type);
    }

    @Override
    public final int outputSize() {
        return 1;
    }

    @Override
    final Stream<List<String>> paths() {
        return Stream.of(List.of());
    }

    @Override
    final Stream<Type<?>> outputTypesImpl() {
        return Stream.of(type);
    }

    @Override
    final ValueProcessor processor(String context) {
        return new GenericObjectMixinProcessor();
    }

    @Override
    final RepeaterProcessor repeaterProcessor() {
        return new RepeaterGenericImpl<>(this, null, null, type.arrayType());
    }

    private class GenericObjectMixinProcessor extends ValueProcessorMixinBase {

        private WritableObjectChunk<TT, ?> out;

        @Override
        public final void setContext(List<WritableChunk<?>> out) {
            this.out = out.get(0).asWritableObjectChunk();
        }

        @Override
        public final void clearContext() {
            out = null;
        }

        @Override
        protected void processCurrentValueImpl(JsonParser parser) throws IOException {
            out.add(parseValue(parser));
        }

        @Override
        protected void processMissingImpl(JsonParser parser) throws IOException {
            out.add(parseMissing(parser));
        }
    }
}

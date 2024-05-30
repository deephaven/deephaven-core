//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.MathUtil;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.sized.SizedCharChunk;
import io.deephaven.json.CharValue;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class CharMixin extends Mixin<CharValue> {
    public CharMixin(CharValue options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int outputSize() {
        return 1;
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.of(List.of());
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return Stream.of(Type.charType());
    }

    @Override
    public ValueProcessor processor(String context) {
        return new CharMixinProcessor();
    }

    private char parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw unexpectedToken(parser);
    }

    private char parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        return new CharRepeaterImpl();
    }

    final class CharRepeaterImpl extends RepeaterProcessorBase<char[]> {
        private final SizedCharChunk<?> chunk = new SizedCharChunk<>(0);

        public CharRepeaterImpl() {
            super(null, null, Type.charType().arrayType());
        }

        @Override
        public void processElementImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableCharChunk<?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
            chunk.set(index, CharMixin.this.parseValue(parser));
            chunk.setSize(newSize);
        }

        @Override
        public void processElementMissingImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableCharChunk<?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
            chunk.set(index, CharMixin.this.parseMissing(parser));
            chunk.setSize(newSize);
        }

        @Override
        public char[] doneImpl(JsonParser parser, int length) {
            final WritableCharChunk<?> chunk = this.chunk.get();
            return Arrays.copyOfRange(chunk.array(), chunk.arrayOffset(), chunk.arrayOffset() + length);
        }
    }

    private char parseFromString(JsonParser parser) throws IOException {
        checkStringAllowed(parser);
        return Parsing.parseStringAsChar(parser);
    }

    private char parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(QueryConstants.NULL_CHAR);
    }

    private char parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(QueryConstants.NULL_CHAR);
    }

    private class CharMixinProcessor extends ValueProcessorMixinBase {
        private WritableCharChunk<?> out;

        @Override
        public void setContext(List<WritableChunk<?>> out) {
            this.out = out.get(0).asWritableCharChunk();
        }

        @Override
        public void clearContext() {
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

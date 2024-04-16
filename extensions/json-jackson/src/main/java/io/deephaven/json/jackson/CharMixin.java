//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.sized.SizedCharChunk;
import io.deephaven.json.CharValue;
import io.deephaven.json.jackson.CharValueProcessor.ToChar;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class CharMixin extends Mixin<CharValue> implements ToChar {
    public CharMixin(CharValue options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int numColumns() {
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
        return new CharValueProcessor(this);
    }

    @Override
    public char parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw Parsing.mismatch(parser, int.class);
    }

    @Override
    public char parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new CharRepeaterImpl(allowMissing, allowNull);
    }

    final class CharRepeaterImpl extends RepeaterProcessorBase<char[]> {
        private final SizedCharChunk<?> chunk = new SizedCharChunk<>(0);

        public CharRepeaterImpl(boolean allowMissing, boolean allowNull) {
            super(allowMissing, allowNull, null, null);
        }

        @Override
        public void processElement(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableCharChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
            chunk.set(index, CharMixin.this.parseValue(parser));
            chunk.setSize(newSize);
        }

        @Override
        public void processElementMissing(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableCharChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
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
        if (!allowString()) {
            throw Parsing.mismatch(parser, char.class);
        }
        return Parsing.parseStringAsChar(parser);
    }

    private char parseFromNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Parsing.mismatch(parser, char.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_CHAR);
    }

    private char parseFromMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Parsing.mismatchMissing(parser, char.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_CHAR);
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.sized.SizedIntChunk;
import io.deephaven.json.IntValue;
import io.deephaven.json.jackson.IntValueProcessor.ToInt;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class IntMixin extends Mixin<IntValue> implements ToInt {

    public IntMixin(IntValue options, JsonFactory factory) {
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
        return Stream.of(Type.intType());
    }

    @Override
    public ValueProcessor processor(String context) {
        return new IntValueProcessor(this);
    }

    @Override
    public int parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_NUMBER_INT:
                return parseFromInt(parser);
            case VALUE_NUMBER_FLOAT:
                return parseFromDecimal(parser);
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw Parsing.mismatch(parser, int.class);
    }

    @Override
    public int parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new IntRepeaterImpl(allowMissing, allowNull);
    }

    final class IntRepeaterImpl extends RepeaterProcessorBase<int[]> {
        private final SizedIntChunk<?> chunk = new SizedIntChunk<>(0);

        public IntRepeaterImpl(boolean allowMissing, boolean allowNull) {
            super(allowMissing, allowNull, null, null, Type.intType().arrayType());
        }

        @Override
        public void processElementImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableIntChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
            chunk.set(index, IntMixin.this.parseValue(parser));
            chunk.setSize(newSize);
        }

        @Override
        public void processElementMissingImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableIntChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
            chunk.set(index, IntMixin.this.parseMissing(parser));
            chunk.setSize(newSize);
        }

        @Override
        public int[] doneImpl(JsonParser parser, int length) {
            final WritableIntChunk<?> chunk = this.chunk.get();
            return Arrays.copyOfRange(chunk.array(), chunk.arrayOffset(), chunk.arrayOffset() + length);
        }
    }

    private int parseFromInt(JsonParser parser) throws IOException {
        if (!allowNumberInt()) {
            throw Parsing.mismatch(parser, int.class);
        }
        return Parsing.parseIntAsInt(parser);
    }

    private int parseFromDecimal(JsonParser parser) throws IOException {
        if (!allowDecimal()) {
            throw Parsing.mismatch(parser, int.class);
        }
        return Parsing.parseDecimalAsInt(parser);
    }

    private int parseFromString(JsonParser parser) throws IOException {
        if (!allowString()) {
            throw Parsing.mismatch(parser, int.class);
        }
        return allowDecimal()
                ? Parsing.parseDecimalStringAsInt(parser)
                : Parsing.parseStringAsInt(parser);
    }

    private int parseFromNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Parsing.mismatch(parser, int.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_INT);
    }

    private int parseFromMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Parsing.mismatchMissing(parser, int.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_INT);
    }
}

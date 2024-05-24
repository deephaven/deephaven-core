//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.sized.SizedShortChunk;
import io.deephaven.json.ShortValue;
import io.deephaven.json.jackson.ShortValueProcessor.ToShort;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class ShortMixin extends Mixin<ShortValue> implements ToShort {
    public ShortMixin(ShortValue options, JsonFactory factory) {
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
        return Stream.of(Type.shortType());
    }

    @Override
    public ValueProcessor processor(String context) {
        return new ShortValueProcessor(this);
    }

    @Override
    public short parseValue(JsonParser parser) throws IOException {
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
    public short parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new ShortRepeaterImpl(allowMissing, allowNull);
    }

    final class ShortRepeaterImpl extends RepeaterProcessorBase<short[]> {
        private final SizedShortChunk<?> chunk = new SizedShortChunk<>(0);

        public ShortRepeaterImpl(boolean allowMissing, boolean allowNull) {
            super(allowMissing, allowNull, null, null, Type.shortType().arrayType());
        }

        @Override
        public void processElementImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableShortChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
            chunk.set(index, ShortMixin.this.parseValue(parser));
            chunk.setSize(newSize);
        }

        @Override
        public void processElementMissingImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableShortChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
            chunk.set(index, ShortMixin.this.parseMissing(parser));
            chunk.setSize(newSize);
        }

        @Override
        public short[] doneImpl(JsonParser parser, int length) {
            final WritableShortChunk<?> chunk = this.chunk.get();
            return Arrays.copyOfRange(chunk.array(), chunk.arrayOffset(), chunk.arrayOffset() + length);
        }
    }

    private short parseFromInt(JsonParser parser) throws IOException {
        if (!allowNumberInt()) {
            throw Parsing.mismatch(parser, short.class);
        }
        return Parsing.parseIntAsShort(parser);
    }

    private short parseFromDecimal(JsonParser parser) throws IOException {
        if (!allowDecimal()) {
            throw Parsing.mismatch(parser, short.class);
        }
        return Parsing.parseDecimalAsShort(parser);
    }

    private short parseFromString(JsonParser parser) throws IOException {
        if (!allowString()) {
            throw Parsing.mismatch(parser, short.class);
        }
        return allowDecimal()
                ? Parsing.parseDecimalStringAsShort(parser)
                : Parsing.parseStringAsShort(parser);
    }

    private short parseFromNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Parsing.mismatch(parser, short.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_SHORT);
    }

    private short parseFromMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Parsing.mismatchMissing(parser, short.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_SHORT);
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.MathUtil;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.sized.SizedShortChunk;
import io.deephaven.json.ShortValue;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class ShortMixin extends Mixin<ShortValue> {
    public ShortMixin(ShortValue options, JsonFactory factory) {
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
        return Stream.of(Type.shortType());
    }

    @Override
    public ValueProcessor processor(String context) {
        return new ShortMixinProcessor();
    }

    private short parseValue(JsonParser parser) throws IOException {
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
        throw unexpectedToken(parser);
    }

    private short parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        return new ShortRepeaterImpl();
    }

    final class ShortRepeaterImpl extends RepeaterProcessorBase<short[]> {
        private final SizedShortChunk<?> chunk = new SizedShortChunk<>(0);

        public ShortRepeaterImpl() {
            super(null, null, Type.shortType().arrayType());
        }

        @Override
        public void processElementImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableShortChunk<?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
            chunk.set(index, ShortMixin.this.parseValue(parser));
            chunk.setSize(newSize);
        }

        @Override
        public void processElementMissingImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableShortChunk<?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
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
        checkNumberIntAllowed(parser);
        return Parsing.parseIntAsShort(parser);
    }

    private short parseFromDecimal(JsonParser parser) throws IOException {
        checkDecimalAllowed(parser);
        return Parsing.parseDecimalAsShort(parser);
    }

    private short parseFromString(JsonParser parser) throws IOException {
        checkStringAllowed(parser);
        return allowDecimal()
                ? Parsing.parseDecimalStringAsShort(parser)
                : Parsing.parseStringAsShort(parser);
    }

    private short parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(QueryConstants.NULL_SHORT);
    }

    private short parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(QueryConstants.NULL_SHORT);
    }

    final class ShortMixinProcessor extends ValueProcessorMixinBase {

        private WritableShortChunk<?> out;

        @Override
        public void setContext(List<WritableChunk<?>> out) {
            this.out = out.get(0).asWritableShortChunk();
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

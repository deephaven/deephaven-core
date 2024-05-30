//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.MathUtil;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.sized.SizedIntChunk;
import io.deephaven.json.IntValue;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class IntMixin extends Mixin<IntValue> {

    public IntMixin(IntValue options, JsonFactory factory) {
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
        return Stream.of(Type.intType());
    }

    @Override
    public ValueProcessor processor(String context) {
        return new IntMixinProcessor();
    }

    private int parseValue(JsonParser parser) throws IOException {
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

    private int parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        return new IntRepeaterImpl();
    }

    final class IntRepeaterImpl extends RepeaterProcessorBase<int[]> {
        private final SizedIntChunk<?> chunk = new SizedIntChunk<>(0);

        public IntRepeaterImpl() {
            super(null, null, Type.intType().arrayType());
        }

        @Override
        public void processElementImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableIntChunk<?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
            chunk.set(index, IntMixin.this.parseValue(parser));
            chunk.setSize(newSize);
        }

        @Override
        public void processElementMissingImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableIntChunk<?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
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
        checkNumberIntAllowed(parser);
        return Parsing.parseIntAsInt(parser);
    }

    private int parseFromDecimal(JsonParser parser) throws IOException {
        checkDecimalAllowed(parser);
        return Parsing.parseDecimalAsInt(parser);
    }

    private int parseFromString(JsonParser parser) throws IOException {
        checkStringAllowed(parser);
        return allowDecimal()
                ? Parsing.parseDecimalStringAsInt(parser)
                : Parsing.parseStringAsInt(parser);
    }

    private int parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(QueryConstants.NULL_INT);
    }

    private int parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(QueryConstants.NULL_INT);
    }

    final class IntMixinProcessor extends ValueProcessorMixinBase {

        private WritableIntChunk<?> out;

        @Override
        public void setContext(List<WritableChunk<?>> out) {
            this.out = out.get(0).asWritableIntChunk();
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

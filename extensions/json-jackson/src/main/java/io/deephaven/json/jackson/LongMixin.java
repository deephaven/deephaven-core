//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.MathUtil;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.json.LongValue;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class LongMixin extends Mixin<LongValue> {

    public LongMixin(LongValue options, JsonFactory config) {
        super(config, options);
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
        return Stream.of(Type.longType());
    }

    @Override
    public ValueProcessor processor(String context) {
        return new LongMixinProcessor();
    }

    private long parseValue(JsonParser parser) throws IOException {
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

    private long parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        return new LongRepeaterImpl();
    }

    final class LongRepeaterImpl extends RepeaterProcessorBase<long[]> {
        private final SizedLongChunk<?> chunk = new SizedLongChunk<>(0);

        public LongRepeaterImpl() {
            super(null, null, Type.longType().arrayType());
        }

        @Override
        public void processElementImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableLongChunk<?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
            chunk.set(index, LongMixin.this.parseValue(parser));
            chunk.setSize(newSize);
        }

        @Override
        public void processElementMissingImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableLongChunk<?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
            chunk.set(index, LongMixin.this.parseMissing(parser));
            chunk.setSize(newSize);
        }

        @Override
        public long[] doneImpl(JsonParser parser, int length) {
            final WritableLongChunk<?> chunk = this.chunk.get();
            return Arrays.copyOfRange(chunk.array(), chunk.arrayOffset(), chunk.arrayOffset() + length);
        }
    }

    private long parseFromInt(JsonParser parser) throws IOException {
        checkNumberIntAllowed(parser);
        return Parsing.parseIntAsLong(parser);
    }

    private long parseFromDecimal(JsonParser parser) throws IOException {
        checkDecimalAllowed(parser);
        return Parsing.parseDecimalAsLong(parser);
    }

    private long parseFromString(JsonParser parser) throws IOException {
        checkStringAllowed(parser);
        return allowDecimal()
                ? Parsing.parseDecimalStringAsLong(parser)
                : Parsing.parseStringAsLong(parser);
    }

    private long parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(QueryConstants.NULL_LONG);
    }

    private long parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(QueryConstants.NULL_LONG);
    }

    private class LongMixinProcessor extends ValueProcessorMixinBase {
        private WritableLongChunk<?> out;

        @Override
        public void setContext(List<WritableChunk<?>> out) {
            this.out = out.get(0).asWritableLongChunk();
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

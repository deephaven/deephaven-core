//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.BoolValue;
import io.deephaven.qst.type.Type;
import io.deephaven.util.BooleanUtils;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class BoolMixin extends Mixin<BoolValue> {

    private final Boolean onNull;
    private final Boolean onMissing;
    private final byte onNullByte;
    private final byte onMissingByte;

    public BoolMixin(BoolValue options, JsonFactory factory) {
        super(factory, options);
        onNull = options.onNull().orElse(null);
        onMissing = options.onMissing().orElse(null);
        onNullByte = BooleanUtils.booleanAsByte(onNull);
        onMissingByte = BooleanUtils.booleanAsByte(onMissing);
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
        return Stream.of(Type.booleanType().boxedType());
    }

    @Override
    public ValueProcessor processor(String context) {
        return new BoolMixinProcessor();
    }

    private byte parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_TRUE:
                return BooleanUtils.TRUE_BOOLEAN_AS_BYTE;
            case VALUE_FALSE:
                return BooleanUtils.FALSE_BOOLEAN_AS_BYTE;
            case VALUE_NULL:
                return parseFromNull(parser);
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
        }
        throw unexpectedToken(parser);
    }

    private byte parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        return new RepeaterGenericImpl<>(new ToBoolean(), null, null,
                Type.booleanType().boxedType().arrayType());
    }

    final class ToBoolean implements ToObject<Boolean> {
        @Override
        public Boolean parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_TRUE:
                    return Boolean.TRUE;
                case VALUE_FALSE:
                    return Boolean.FALSE;
                case VALUE_NULL:
                    return parseFromNullBoolean(parser);
                case VALUE_STRING:
                case FIELD_NAME:
                    return parseFromStringBoolean(parser);
            }
            throw unexpectedToken(parser);
        }

        @Override
        public Boolean parseMissing(JsonParser parser) throws IOException {
            return parseFromMissingBoolean(parser);
        }
    }

    private byte parseFromString(JsonParser parser) throws IOException {
        checkStringAllowed(parser);
        if (!allowNull()) {
            final byte res = Parsing.parseStringAsByteBool(parser, BooleanUtils.NULL_BOOLEAN_AS_BYTE);
            if (res == BooleanUtils.NULL_BOOLEAN_AS_BYTE) {
                throw nullNotAllowed(parser);
            }
            return res;
        }
        return Parsing.parseStringAsByteBool(parser, onNullByte);
    }

    private byte parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return onNullByte;
    }

    private byte parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return onMissingByte;
    }

    private Boolean parseFromStringBoolean(JsonParser parser) throws IOException {
        checkStringAllowed(parser);
        if (!allowNull()) {
            final Boolean result = Parsing.parseStringAsBoolean(parser, null);
            if (result == null) {
                throw nullNotAllowed(parser);
            }
            return result;
        }
        return Parsing.parseStringAsBoolean(parser, onNull);
    }

    private Boolean parseFromNullBoolean(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return onNull;
    }

    private Boolean parseFromMissingBoolean(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return onMissing;
    }

    private class BoolMixinProcessor extends ValueProcessorMixinBase {
        private WritableByteChunk<?> out;

        @Override
        public final void setContext(List<WritableChunk<?>> out) {
            this.out = out.get(0).asWritableByteChunk();
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

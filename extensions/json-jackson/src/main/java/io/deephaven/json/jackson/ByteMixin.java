//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.sized.SizedByteChunk;
import io.deephaven.json.ByteValue;
import io.deephaven.json.jackson.ByteValueProcessor.ToByte;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class ByteMixin extends Mixin<ByteValue> implements ToByte {
    public ByteMixin(ByteValue options, JsonFactory factory) {
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
        return Stream.of(Type.byteType());
    }

    @Override
    public ValueProcessor processor(String context) {
        return new ByteValueProcessor(this);
    }

    @Override
    public byte parseValue(JsonParser parser) throws IOException {
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

    @Override
    public byte parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new ByteRepeaterImpl(allowMissing, allowNull);
    }

    final class ByteRepeaterImpl extends RepeaterProcessorBase<byte[]> {
        private final SizedByteChunk<?> chunk = new SizedByteChunk<>(0);

        public ByteRepeaterImpl(boolean allowMissing, boolean allowNull) {
            super(allowMissing, allowNull, null, null, Type.byteType().arrayType());
        }

        @Override
        public void processElementImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableByteChunk<?> chunk = this.chunk.ensureCapacityPreserve(Maths.nextArrayCapacity(newSize));
            chunk.set(index, ByteMixin.this.parseValue(parser));
            chunk.setSize(newSize);
        }

        @Override
        public void processElementMissingImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableByteChunk<?> chunk = this.chunk.ensureCapacityPreserve(Maths.nextArrayCapacity(newSize));
            chunk.set(index, ByteMixin.this.parseMissing(parser));
            chunk.setSize(newSize);
        }

        @Override
        public byte[] doneImpl(JsonParser parser, int length) {
            final WritableByteChunk<?> chunk = this.chunk.get();
            return Arrays.copyOfRange(chunk.array(), chunk.arrayOffset(), chunk.arrayOffset() + length);
        }
    }

    private byte parseFromInt(JsonParser parser) throws IOException {
        checkNumberIntAllowed(parser);
        return Parsing.parseIntAsByte(parser);
    }

    private byte parseFromDecimal(JsonParser parser) throws IOException {
        checkDecimalAllowed(parser);
        return Parsing.parseDecimalAsByte(parser);
    }

    private byte parseFromString(JsonParser parser) throws IOException {
        checkStringAllowed(parser);
        return allowDecimal()
                ? Parsing.parseDecimalStringAsByte(parser)
                : Parsing.parseStringAsByte(parser);
    }

    private byte parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(QueryConstants.NULL_BYTE);
    }

    private byte parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(QueryConstants.NULL_BYTE);
    }
}

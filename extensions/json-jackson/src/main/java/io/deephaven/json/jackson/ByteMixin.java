//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.ArrayUtil;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ByteOptions;
import io.deephaven.json.jackson.ByteValueProcessor.ToByte;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.deephaven.util.type.ArrayTypeUtils.EMPTY_BYTE_ARRAY;

final class ByteMixin extends Mixin<ByteOptions> implements ToByte {
    public ByteMixin(ByteOptions options, JsonFactory factory) {
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
        return Stream.of(Type.byteType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new ByteValueProcessor(out.get(0).asWritableByteChunk(), this);
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
        throw Parsing.mismatch(parser, int.class);
    }

    @Override
    public byte parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new ByteRepeaterProcessorImpl(out.get(0).asWritableObjectChunk()::add, allowMissing, allowNull);
    }

    final class ByteRepeaterProcessorImpl extends RepeaterProcessorBase<byte[]> {

        public ByteRepeaterProcessorImpl(Consumer<? super byte[]> consumer, boolean allowMissing, boolean allowNull) {
            super(consumer, allowMissing, allowNull, null, null);
        }

        @Override
        public ByteArrayContext newContext() {
            return new ByteArrayContext();
        }

        final class ByteArrayContext extends RepeaterContextBase {
            private byte[] arr = EMPTY_BYTE_ARRAY;
            private int len = 0;

            @Override
            public void processElement(JsonParser parser, int index) throws IOException {
                if (index != len) {
                    throw new IllegalStateException();
                }
                arr = ArrayUtil.put(arr, len, ByteMixin.this.parseValue(parser));
                ++len;
            }

            @Override
            public void processElementMissing(JsonParser parser, int index) throws IOException {
                if (index != len) {
                    throw new IllegalStateException();
                }
                arr = ArrayUtil.put(arr, len, ByteMixin.this.parseMissing(parser));
                ++len;
            }

            @Override
            public byte[] onDone(int length) {
                if (length != len) {
                    throw new IllegalStateException();
                }
                return arr.length == len ? arr : Arrays.copyOf(arr, len);
            }
        }
    }

    private byte parseFromInt(JsonParser parser) throws IOException {
        if (!allowNumberInt()) {
            throw Parsing.mismatch(parser, byte.class);
        }
        return Parsing.parseIntAsByte(parser);
    }

    private byte parseFromDecimal(JsonParser parser) throws IOException {
        if (!allowDecimal()) {
            throw Parsing.mismatch(parser, byte.class);
        }
        return Parsing.parseDecimalAsTruncatedByte(parser);
    }

    private byte parseFromString(JsonParser parser) throws IOException {
        if (!allowString()) {
            throw Parsing.mismatch(parser, byte.class);
        }
        return allowDecimal()
                ? Parsing.parseDecimalStringAsTruncatedByte(parser)
                : Parsing.parseStringAsByte(parser);
    }

    private byte parseFromNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Parsing.mismatch(parser, byte.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_BYTE);
    }

    private byte parseFromMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Parsing.mismatchMissing(parser, byte.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_BYTE);
    }
}

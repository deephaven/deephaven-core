//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.ArrayUtil;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.CharOptions;
import io.deephaven.json.jackson.CharValueProcessor.ToChar;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.deephaven.util.type.ArrayTypeUtils.EMPTY_CHAR_ARRAY;

final class CharMixin extends Mixin<CharOptions> implements ToChar {
    public CharMixin(CharOptions options, JsonFactory factory) {
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
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new CharValueProcessor(out.get(0).asWritableCharChunk(), this);
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
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new CharRepeaterImpl(out.get(0).asWritableObjectChunk()::add, allowMissing, allowNull);
    }

    final class CharRepeaterImpl extends RepeaterProcessorBase<char[]> {

        public CharRepeaterImpl(Consumer<? super char[]> consumer, boolean allowMissing, boolean allowNull) {
            super(consumer, allowMissing, allowNull, null, null);
        }

        @Override
        public CharArrayContext newContext() {
            return new CharArrayContext();
        }

        final class CharArrayContext extends RepeaterContextBase {
            private char[] arr = EMPTY_CHAR_ARRAY;
            private int len = 0;

            @Override
            public void processElement(JsonParser parser, int index) throws IOException {
                if (index != len) {
                    throw new IllegalStateException();
                }
                arr = ArrayUtil.put(arr, len, CharMixin.this.parseValue(parser));
                ++len;
            }

            @Override
            public void processElementMissing(JsonParser parser, int index) throws IOException {
                if (index != len) {
                    throw new IllegalStateException();
                }
                arr = ArrayUtil.put(arr, len, CharMixin.this.parseMissing(parser));
                ++len;
            }

            @Override
            public char[] onDone(int length) {
                if (length != len) {
                    throw new IllegalStateException();
                }
                return arr.length == len ? arr : Arrays.copyOf(arr, len);
            }
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

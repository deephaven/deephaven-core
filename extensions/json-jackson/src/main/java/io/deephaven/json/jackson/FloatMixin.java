//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.ArrayUtil;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.FloatOptions;
import io.deephaven.json.jackson.FloatValueProcessor.ToFloat;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.deephaven.util.type.ArrayTypeUtils.EMPTY_FLOAT_ARRAY;

final class FloatMixin extends Mixin<FloatOptions> implements ToFloat {

    public FloatMixin(FloatOptions options, JsonFactory factory) {
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
    public Stream<Type<?>> outputTypes() {
        return Stream.of(Type.floatType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new FloatValueProcessor(out.get(0).asWritableFloatChunk(), this);
    }

    @Override
    public float parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                return parseFromNumber(parser);
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw Parsing.mismatch(parser, float.class);
    }

    @Override
    public float parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new FloatRepeaterImpl(out.get(0).asWritableObjectChunk()::add, allowMissing, allowNull);
    }

    final class FloatRepeaterImpl extends RepeaterProcessorBase<float[]> {

        public FloatRepeaterImpl(Consumer<? super float[]> consumer, boolean allowMissing, boolean allowNull) {
            super(consumer, allowMissing, allowNull, null, null);
        }

        @Override
        public FloatArrayContext newContext() {
            return new FloatArrayContext();
        }

        final class FloatArrayContext extends RepeaterContextBase {
            private float[] arr = EMPTY_FLOAT_ARRAY;
            private int len = 0;

            @Override
            public void processElement(JsonParser parser, int index) throws IOException {
                if (index != len) {
                    throw new IllegalStateException();
                }
                arr = ArrayUtil.put(arr, len, FloatMixin.this.parseValue(parser));
                ++len;
            }

            @Override
            public void processElementMissing(JsonParser parser, int index) throws IOException {
                if (index != len) {
                    throw new IllegalStateException();
                }
                arr = ArrayUtil.put(arr, len, FloatMixin.this.parseMissing(parser));
                ++len;
            }

            @Override
            public float[] onDone(int length) {
                if (length != len) {
                    throw new IllegalStateException();
                }
                return arr.length == len ? arr : Arrays.copyOf(arr, len);
            }
        }
    }

    private float onNullOrDefault() {
        final Float onNull = options.onNull();
        return onNull != null ? onNull : QueryConstants.NULL_FLOAT;
    }

    private float onMissingOrDefault() {
        final Float onMissing = options.onMissing();
        return onMissing != null ? onMissing : QueryConstants.NULL_FLOAT;
    }

    private float parseFromNumber(JsonParser parser) throws IOException {
        if (!options.allowDecimal() && !options.allowNumberInt()) {
            throw Parsing.mismatch(parser, float.class);
        }
        return Parsing.parseNumberAsFloat(parser);
    }

    private float parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Parsing.mismatch(parser, float.class);
        }
        return Parsing.parseStringAsFloat(parser);
    }

    private float parseFromNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Parsing.mismatch(parser, float.class);
        }
        return onNullOrDefault();
    }

    private float parseFromMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Parsing.mismatchMissing(parser, float.class);
        }
        return onMissingOrDefault();
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.ArrayUtil;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.DoubleOptions;
import io.deephaven.json.jackson.DoubleValueProcessor.ToDouble;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.deephaven.util.type.ArrayTypeUtils.EMPTY_DOUBLE_ARRAY;

final class DoubleMixin extends Mixin<DoubleOptions> implements ToDouble {

    public DoubleMixin(DoubleOptions options, JsonFactory factory) {
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
        return Stream.of(Type.doubleType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return new DoubleValueProcessor(out.get(0).asWritableDoubleChunk(), this);
    }

    @Override
    public double parseValue(JsonParser parser) throws IOException {
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
        throw Parsing.mismatch(parser, double.class);
    }

    @Override
    public double parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new DoubleRepeaterImpl(out.get(0).asWritableObjectChunk()::add, allowMissing, allowNull);
    }

    final class DoubleRepeaterImpl extends RepeaterProcessorBase<double[]> {

        public DoubleRepeaterImpl(Consumer<? super double[]> consumer, boolean allowMissing, boolean allowNull) {
            super(consumer, allowMissing, allowNull, null, null);
        }

        @Override
        public DoubleArrayContext newContext() {
            return new DoubleArrayContext();
        }

        final class DoubleArrayContext extends RepeaterContextBase {
            private double[] arr = EMPTY_DOUBLE_ARRAY;
            private int len = 0;

            @Override
            public void processElement(JsonParser parser, int index) throws IOException {
                if (index != len) {
                    throw new IllegalStateException();
                }
                arr = ArrayUtil.put(arr, len, DoubleMixin.this.parseValue(parser));
                ++len;
            }

            @Override
            public void processElementMissing(JsonParser parser, int index) throws IOException {
                if (index != len) {
                    throw new IllegalStateException();
                }
                arr = ArrayUtil.put(arr, len, DoubleMixin.this.parseMissing(parser));
                ++len;
            }

            @Override
            public double[] onDone(int length) {
                if (length != len) {
                    throw new IllegalStateException();
                }
                return arr.length == len ? arr : Arrays.copyOf(arr, len);
            }
        }
    }

    private double parseFromNumber(JsonParser parser) throws IOException {
        if (!allowDecimal() && !allowNumberInt()) {
            throw Parsing.mismatch(parser, double.class);
        }
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return Parsing.parseNumberAsDouble(parser);
    }

    private double parseFromString(JsonParser parser) throws IOException {
        if (!allowString()) {
            throw Parsing.mismatch(parser, double.class);
        }
        return Parsing.parseStringAsDouble(parser);
    }

    private double parseFromNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Parsing.mismatch(parser, double.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_DOUBLE);
    }

    private double parseFromMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Parsing.mismatchMissing(parser, double.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_DOUBLE);
    }
}

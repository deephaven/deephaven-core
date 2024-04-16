//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.sized.SizedDoubleChunk;
import io.deephaven.json.DoubleValue;
import io.deephaven.json.jackson.DoubleValueProcessor.ToDouble;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class DoubleMixin extends Mixin<DoubleValue> implements ToDouble {

    public DoubleMixin(DoubleValue options, JsonFactory factory) {
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
    public ValueProcessor processor(String context) {
        return new DoubleValueProcessor(this);
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
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new DoubleRepeaterImpl(allowMissing, allowNull);
    }

    final class DoubleRepeaterImpl extends RepeaterProcessorBase<double[]> {
        private final SizedDoubleChunk<?> chunk = new SizedDoubleChunk<>(0);

        public DoubleRepeaterImpl(boolean allowMissing, boolean allowNull) {
            super(allowMissing, allowNull, null, null);
        }

        @Override
        public void processElement(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableDoubleChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
            chunk.set(index, DoubleMixin.this.parseValue(parser));
            chunk.setSize(newSize);
        }

        @Override
        public void processElementMissing(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableDoubleChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
            chunk.set(index, DoubleMixin.this.parseMissing(parser));
            chunk.setSize(newSize);
        }

        @Override
        public double[] doneImpl(JsonParser parser, int length) {
            final WritableDoubleChunk<?> chunk = this.chunk.get();
            return Arrays.copyOfRange(chunk.array(), chunk.arrayOffset(), chunk.arrayOffset() + length);
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

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.base.MathUtil;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.sized.SizedDoubleChunk;
import io.deephaven.json.DoubleValue;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class DoubleMixin extends Mixin<DoubleValue> {

    public DoubleMixin(DoubleValue options, JsonFactory factory) {
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
        return Stream.of(Type.doubleType());
    }

    @Override
    public ValueProcessor processor(String context) {
        return new DoubleMixinProcessor();
    }

    private double parseValue(JsonParser parser) throws IOException {
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
        throw unexpectedToken(parser);
    }

    private double parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        return new DoubleRepeaterImpl();
    }

    final class DoubleRepeaterImpl extends RepeaterProcessorBase<double[]> {
        private final SizedDoubleChunk<?> chunk = new SizedDoubleChunk<>(0);

        public DoubleRepeaterImpl() {
            super(null, null, Type.doubleType().arrayType());
        }

        @Override
        public void processElementImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableDoubleChunk<?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
            chunk.set(index, DoubleMixin.this.parseValue(parser));
            chunk.setSize(newSize);
        }

        @Override
        public void processElementMissingImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableDoubleChunk<?> chunk = this.chunk.ensureCapacityPreserve(MathUtil.roundUpArraySize(newSize));
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
        checkNumberAllowed(parser);
        // TODO: improve after https://github.com/FasterXML/jackson-core/issues/1229
        return Parsing.parseNumberAsDouble(parser);
    }

    private double parseFromString(JsonParser parser) throws IOException {
        checkStringAllowed(parser);
        return Parsing.parseStringAsDouble(parser);
    }

    private double parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(QueryConstants.NULL_DOUBLE);
    }

    private double parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(QueryConstants.NULL_DOUBLE);
    }

    final class DoubleMixinProcessor extends ValueProcessorMixinBase {

        private WritableDoubleChunk<?> out;

        @Override
        public void setContext(List<WritableChunk<?>> out) {
            this.out = out.get(0).asWritableDoubleChunk();
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

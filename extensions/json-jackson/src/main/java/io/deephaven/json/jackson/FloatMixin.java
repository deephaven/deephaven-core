//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.sized.SizedFloatChunk;
import io.deephaven.json.FloatValue;
import io.deephaven.json.jackson.FloatValueProcessor.ToFloat;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

final class FloatMixin extends Mixin<FloatValue> implements ToFloat {

    public FloatMixin(FloatValue options, JsonFactory factory) {
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
        return Stream.of(Type.floatType());
    }

    @Override
    public ValueProcessor processor(String context) {
        return new FloatValueProcessor(this);
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
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new FloatRepeaterImpl(allowMissing, allowNull);
    }

    final class FloatRepeaterImpl extends RepeaterProcessorBase<float[]> {
        private final SizedFloatChunk<?> chunk = new SizedFloatChunk<>(0);

        public FloatRepeaterImpl(boolean allowMissing, boolean allowNull) {
            super(allowMissing, allowNull, null, null);
        }

        @Override
        public void processElementImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableFloatChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
            chunk.set(index, FloatMixin.this.parseValue(parser));
            chunk.setSize(newSize);
        }

        @Override
        public void processElementMissingImpl(JsonParser parser, int index) throws IOException {
            final int newSize = index + 1;
            final WritableFloatChunk<?> chunk = this.chunk.ensureCapacityPreserve(newSize);
            chunk.set(index, FloatMixin.this.parseMissing(parser));
            chunk.setSize(newSize);
        }

        @Override
        public float[] doneImpl(JsonParser parser, int length) {
            final WritableFloatChunk<?> chunk = this.chunk.get();
            return Arrays.copyOfRange(chunk.array(), chunk.arrayOffset(), chunk.arrayOffset() + length);
        }
    }

    private float parseFromNumber(JsonParser parser) throws IOException {
        if (!allowDecimal() && !allowNumberInt()) {
            throw Parsing.mismatch(parser, float.class);
        }
        return Parsing.parseNumberAsFloat(parser);
    }

    private float parseFromString(JsonParser parser) throws IOException {
        if (!allowString()) {
            throw Parsing.mismatch(parser, float.class);
        }
        return Parsing.parseStringAsFloat(parser);
    }

    private float parseFromNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Parsing.mismatch(parser, float.class);
        }
        return options.onNull().orElse(QueryConstants.NULL_FLOAT);
    }

    private float parseFromMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Parsing.mismatchMissing(parser, float.class);
        }
        return options.onMissing().orElse(QueryConstants.NULL_FLOAT);
    }
}

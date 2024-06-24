//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.ArrayValue;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class ArrayMixin extends Mixin<ArrayValue> {

    private final Mixin<?> element;

    public ArrayMixin(ArrayValue options, JsonFactory factory) {
        super(factory, options);
        element = Mixin.of(options.element(), factory);
    }

    @Override
    public int outputSize() {
        return element.outputSize();
    }

    @Override
    public Stream<List<String>> paths() {
        return element.paths();
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return elementOutputTypes().map(Type::arrayType);
    }

    @Override
    public ValueProcessor processor(String context) {
        return new ArrayMixinProcessor();
    }

    private Stream<? extends Type<?>> elementOutputTypes() {
        return element.outputTypesImpl();
    }

    private RepeaterProcessor elementRepeater() {
        return element.repeaterProcessor();
    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        // For example:
        // double (element())
        // double[] (processor())
        // double[][] (repeater())
        // return new ArrayOfArrayRepeaterProcessor(allowMissing, allowNull);
        return new ValueInnerRepeaterProcessor(new ArrayMixinProcessor());
    }

    private class ArrayMixinProcessor implements ValueProcessor {

        private final RepeaterProcessor elementProcessor;

        ArrayMixinProcessor() {
            this.elementProcessor = elementRepeater();
        }

        @Override
        public void setContext(List<WritableChunk<?>> out) {
            elementProcessor.setContext(out);
        }

        @Override
        public void clearContext() {
            elementProcessor.clearContext();
        }

        @Override
        public int numColumns() {
            return elementProcessor.numColumns();
        }

        @Override
        public Stream<Type<?>> columnTypes() {
            return elementProcessor.columnTypes();
        }

        @Override
        public void processCurrentValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case START_ARRAY:
                    RepeaterProcessor.processArray(parser, elementProcessor);
                    return;
                case VALUE_NULL:
                    checkNullAllowed(parser);
                    elementProcessor.processNullRepeater(parser);
                    return;
                default:
                    throw unexpectedToken(parser);
            }
        }

        @Override
        public void processMissing(JsonParser parser) throws IOException {
            checkMissingAllowed(parser);
            elementProcessor.processMissingRepeater(parser);
        }
    }
}

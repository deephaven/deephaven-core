//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.SkipValue;
import io.deephaven.json.jackson.RepeaterProcessor.Context;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class SkipMixin extends Mixin<SkipValue> implements ValueProcessor {

    public SkipMixin(SkipValue options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int outputSize() {
        return 0;
    }

    @Override
    public int numColumns() {
        return 0;
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.empty();
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return Stream.empty();
    }

    @Override
    public ValueProcessor processor(String context) {
        return this;
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {

    }

    @Override
    public void clearContext() {

    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        return new SkipArray();
    }

    @Override
    public Stream<Type<?>> columnTypes() {
        return Stream.empty();
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case START_OBJECT:
                checkObjectAllowed(parser);
                parser.skipChildren();
                break;
            case START_ARRAY:
                checkArrayAllowed(parser);
                parser.skipChildren();
                break;
            case VALUE_STRING:
            case FIELD_NAME:
                checkStringAllowed(parser);
                break;
            case VALUE_NUMBER_INT:
                checkNumberIntAllowed(parser);
                break;
            case VALUE_NUMBER_FLOAT:
                checkDecimalAllowed(parser);
                break;
            case VALUE_TRUE:
            case VALUE_FALSE:
                checkBoolAllowed(parser);
                break;
            case VALUE_NULL:
                checkNullAllowed(parser);
                break;
            default:
                throw unexpectedToken(parser);
        }
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
    }

    private final class SkipArray implements RepeaterProcessor, Context {

        @Override
        public Context context() {
            return this;
        }

        @Override
        public void processNullRepeater(JsonParser parser) throws IOException {}

        @Override
        public void processMissingRepeater(JsonParser parser) throws IOException {}

        @Override
        public void setContext(List<WritableChunk<?>> out) {

        }

        @Override
        public void clearContext() {

        }

        @Override
        public int numColumns() {
            return 0;
        }

        @Override
        public Stream<Type<?>> columnTypes() {
            return Stream.empty();
        }

        @Override
        public void start(JsonParser parser) throws IOException {

        }

        @Override
        public void processElement(JsonParser parser) throws IOException {
            processCurrentValue(parser);
        }

        @Override
        public void processElementMissing(JsonParser parser) throws IOException {
            processMissing(parser);
        }

        @Override
        public void done(JsonParser parser) throws IOException {

        }
    }
}

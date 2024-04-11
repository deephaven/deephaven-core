//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.SkipOptions;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class SkipMixin extends Mixin<SkipOptions> implements ValueProcessor, RepeaterProcessor.Context {

    public SkipMixin(SkipOptions options, JsonFactory factory) {
        super(factory, options);
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
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return this;
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new SkipArray(allowMissing, allowNull);
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case START_OBJECT:
                if (!allowObject()) {
                    throw Parsing.mismatch(parser, void.class);
                }
                parser.skipChildren();
                break;
            case START_ARRAY:
                if (!allowArray()) {
                    throw Parsing.mismatch(parser, void.class);
                }
                parser.skipChildren();
                break;
            case VALUE_STRING:
            case FIELD_NAME:
                if (!allowString()) {
                    throw Parsing.mismatch(parser, void.class);
                }
                break;
            case VALUE_NUMBER_INT:
                if (!allowNumberInt()) {
                    throw Parsing.mismatch(parser, void.class);
                }
                break;
            case VALUE_NUMBER_FLOAT:
                if (!allowDecimal()) {
                    throw Parsing.mismatch(parser, void.class);
                }
                break;
            case VALUE_TRUE:
            case VALUE_FALSE:
                if (!allowBool()) {
                    throw Parsing.mismatch(parser, void.class);
                }
                break;
            case VALUE_NULL:
                if (!allowNull()) {
                    throw Parsing.mismatch(parser, void.class);
                }
                break;
            default:
                throw Parsing.mismatch(parser, void.class);
        }
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Parsing.mismatchMissing(parser, void.class);
        }
    }

    private final class SkipArray implements RepeaterProcessor {
        private final boolean allowMissing;
        private final boolean allowNull;

        public SkipArray(boolean allowMissing, boolean allowNull) {
            this.allowMissing = allowMissing;
            this.allowNull = allowNull;
        }

        @Override
        public Context start(JsonParser parser) throws IOException {
            return SkipMixin.this;
        }

        @Override
        public void processNullRepeater(JsonParser parser) throws IOException {
            if (!allowNull) {
                throw Parsing.mismatch(parser, void.class);
            }
        }

        @Override
        public void processMissingRepeater(JsonParser parser) throws IOException {
            if (!allowMissing) {
                throw Parsing.mismatch(parser, void.class);
            }
        }
    }

    @Override
    public void processElement(JsonParser parser, int index) throws IOException {
        processCurrentValue(parser);
    }

    @Override
    public void processElementMissing(JsonParser parser, int index) throws IOException {
        processMissing(parser);
    }

    @Override
    public void done(JsonParser parser, int length) throws IOException {
        // no-op
    }
}

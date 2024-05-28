//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

// generalized for array vs object-kv
interface RepeaterProcessor extends ContextAware {

    static void processArray(
            JsonParser parser,
            RepeaterProcessor processor) throws IOException {
        Parsing.assertCurrentToken(parser, JsonToken.START_ARRAY);
        final Context context = processor.context();
        context.start(parser);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            context.processElement(parser);
        }
        context.done(parser);
    }

    static void processObjectKeyValues(
            JsonParser parser,
            RepeaterProcessor keyProcessor,
            RepeaterProcessor valueProcessor) throws IOException {
        Parsing.assertCurrentToken(parser, JsonToken.START_OBJECT);
        final Context keyContext = keyProcessor.context();
        final Context valueContext = valueProcessor.context();
        keyContext.start(parser);
        valueContext.start(parser);
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            Parsing.assertCurrentToken(parser, JsonToken.FIELD_NAME);
            keyContext.processElement(parser);
            parser.nextToken();
            valueContext.processElement(parser);
        }
        keyContext.done(parser);
        valueContext.done(parser);
    }

    void processNullRepeater(JsonParser parser) throws IOException;

    void processMissingRepeater(JsonParser parser) throws IOException;

    Context context();

    interface Context {

        void start(JsonParser parser) throws IOException;

        void processElement(JsonParser parser) throws IOException;

        // While traditional arrays can't have missing elements, when an object is an array, a field may be missing:
        // [ { "foo": 1, "bar": 2 }, {"bar": 3} ]
        void processElementMissing(JsonParser parser) throws IOException;

        void done(JsonParser parser) throws IOException;
    }
}

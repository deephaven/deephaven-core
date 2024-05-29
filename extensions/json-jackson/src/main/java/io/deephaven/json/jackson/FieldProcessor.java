//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

import static io.deephaven.json.jackson.Parsing.assertCurrentToken;

interface FieldProcessor {

    static void processFields(JsonParser parser, FieldProcessor fieldProcess) throws IOException {
        while (parser.hasToken(JsonToken.FIELD_NAME)) {
            final String fieldName = parser.currentName();
            parser.nextToken();
            fieldProcess.process(fieldName, parser);
            parser.nextToken();
        }
        assertCurrentToken(parser, JsonToken.END_OBJECT);
    }

    static void skipFields(JsonParser parser) throws IOException {
        while (parser.hasToken(JsonToken.FIELD_NAME)) {
            parser.nextToken();
            parser.skipChildren();
            parser.nextToken();
        }
        assertCurrentToken(parser, JsonToken.END_OBJECT);
    }

    void process(String fieldName, JsonParser parser) throws IOException;
}

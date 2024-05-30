//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.StringValue;
import io.deephaven.qst.type.Type;

import java.io.IOException;

final class StringMixin extends GenericObjectMixin<StringValue, String> {

    public StringMixin(StringValue options, JsonFactory factory) {
        super(factory, options, Type.stringType());
    }

    @Override
    public String parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
            case VALUE_NUMBER_INT:
                return parseFromInt(parser);
            case VALUE_NUMBER_FLOAT:
                return parseFromDecimal(parser);
            case VALUE_TRUE:
            case VALUE_FALSE:
                return parseFromBool(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw unexpectedToken(parser);
    }

    @Override
    public String parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    private String parseFromString(JsonParser parser) throws IOException {
        checkStringAllowed(parser);
        return Parsing.parseStringAsString(parser);
    }

    private String parseFromInt(JsonParser parser) throws IOException {
        checkNumberIntAllowed(parser);
        return Parsing.parseIntAsString(parser);
    }

    private String parseFromDecimal(JsonParser parser) throws IOException {
        checkDecimalAllowed(parser);
        return Parsing.parseDecimalAsString(parser);
    }

    private String parseFromBool(JsonParser parser) throws IOException {
        checkBoolAllowed(parser);
        return Parsing.parseBoolAsString(parser);
    }

    private String parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(null);
    }

    private String parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(null);
    }
}

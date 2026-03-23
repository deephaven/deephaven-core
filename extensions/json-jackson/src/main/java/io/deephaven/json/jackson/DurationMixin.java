//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.DurationValue;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.time.Duration;

final class DurationMixin extends GenericObjectMixin<DurationValue, Duration> {

    public DurationMixin(DurationValue options, JsonFactory factory) {
        super(factory, options, Type.durationType());
    }

    @Override
    public Duration parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw unexpectedToken(parser);
    }

    @Override
    public Duration parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    private Duration parseFromString(JsonParser parser) throws IOException {
        return Duration.parse(Parsing.textAsCharSequence(parser));
    }

    private Duration parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(null);
    }

    private Duration parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(null);
    }
}

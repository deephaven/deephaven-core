//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.LocalTimeValue;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;

final class LocalTimeMixin extends GenericObjectMixin<LocalTimeValue, LocalTime> {

    public LocalTimeMixin(LocalTimeValue options, JsonFactory factory) {
        super(factory, options, Type.localTimeType());
    }

    @Override
    public LocalTime parseValue(JsonParser parser) throws IOException {
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
    public LocalTime parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    private LocalTime parseFromString(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = options.dateTimeFormatter().parse(Parsing.textAsCharSequence(parser));
        return LocalTime.from(accessor);
    }

    private LocalTime parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(null);
    }

    private LocalTime parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(null);
    }
}

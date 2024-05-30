//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.LocalDateValue;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;

final class LocalDateMixin extends GenericObjectMixin<LocalDateValue, LocalDate> {

    public LocalDateMixin(LocalDateValue options, JsonFactory factory) {
        super(factory, options, Type.ofCustom(LocalDate.class));
    }

    @Override
    public LocalDate parseValue(JsonParser parser) throws IOException {
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
    public LocalDate parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    private LocalDate parseFromString(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = options.dateTimeFormatter().parse(Parsing.textAsCharSequence(parser));
        return LocalDate.from(accessor);
    }

    private LocalDate parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(null);
    }

    private LocalDate parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(null);
    }
}

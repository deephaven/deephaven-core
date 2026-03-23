//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.PeriodValue;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.time.Period;

final class PeriodMixin extends GenericObjectMixin<PeriodValue, Period> {

    public PeriodMixin(PeriodValue options, JsonFactory factory) {
        super(factory, options, Type.periodType());
    }

    @Override
    public Period parseValue(JsonParser parser) throws IOException {
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
    public Period parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    private Period parseFromString(JsonParser parser) throws IOException {
        return Period.parse(Parsing.textAsCharSequence(parser));
    }

    private Period parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(null);
    }

    private Period parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(null);
    }
}

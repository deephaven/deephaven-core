//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.BigDecimalValue;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.math.BigDecimal;

final class BigDecimalMixin extends GenericObjectMixin<BigDecimalValue, BigDecimal> {

    public BigDecimalMixin(BigDecimalValue options, JsonFactory factory) {
        super(factory, options, Type.ofCustom(BigDecimal.class));
    }

    @Override
    public BigDecimal parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:
                return parseFromNumber(parser);
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw unexpectedToken(parser);
    }

    @Override
    public BigDecimal parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    private BigDecimal parseFromNumber(JsonParser parser) throws IOException {
        checkNumberAllowed(parser);
        return Parsing.parseDecimalAsBigDecimal(parser);
    }

    private BigDecimal parseFromString(JsonParser parser) throws IOException {
        checkStringAllowed(parser);
        return Parsing.parseStringAsBigDecimal(parser);
    }

    private BigDecimal parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(null);
    }

    private BigDecimal parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(null);
    }
}

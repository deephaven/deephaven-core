//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.BigIntegerValue;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.math.BigInteger;

final class BigIntegerMixin extends GenericObjectMixin<BigIntegerValue, BigInteger> {

    public BigIntegerMixin(BigIntegerValue options, JsonFactory factory) {
        super(factory, options, Type.ofCustom(BigInteger.class));
    }

    @Override
    public BigInteger parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_NUMBER_INT:
                return parseFromInt(parser);
            case VALUE_NUMBER_FLOAT:
                return parseFromDecimal(parser);
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw unexpectedToken(parser);
    }

    @Override
    public BigInteger parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    private BigInteger parseFromInt(JsonParser parser) throws IOException {
        checkNumberIntAllowed(parser);
        return Parsing.parseIntAsBigInteger(parser);
    }

    private BigInteger parseFromDecimal(JsonParser parser) throws IOException {
        checkDecimalAllowed(parser);
        return Parsing.parseDecimalAsBigInteger(parser);
    }

    private BigInteger parseFromString(JsonParser parser) throws IOException {
        checkStringAllowed(parser);
        return allowDecimal()
                ? Parsing.parseDecimalStringAsBigInteger(parser)
                : Parsing.parseStringAsBigInteger(parser);
    }

    private BigInteger parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(null);
    }

    private BigInteger parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(null);
    }
}

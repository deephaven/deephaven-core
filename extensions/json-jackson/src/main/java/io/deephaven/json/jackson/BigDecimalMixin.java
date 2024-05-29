//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.BigDecimalValue;
import io.deephaven.json.jackson.ObjectValueProcessor.ToObject;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;

final class BigDecimalMixin extends Mixin<BigDecimalValue> implements ToObject<BigDecimal> {

    public BigDecimalMixin(BigDecimalValue options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int numColumns() {
        return 1;
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.of(List.of());
    }

    @Override
    public Stream<Type<?>> outputTypesImpl() {
        return Stream.of(Type.ofCustom(BigDecimal.class));
    }

    @Override
    public ValueProcessor processor(String context) {
        return new ObjectValueProcessor<>(this, Type.ofCustom(BigDecimal.class));
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

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new RepeaterGenericImpl<>(this, allowMissing, allowNull, null, null,
                Type.ofCustom(BigDecimal.class).arrayType());
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
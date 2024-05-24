//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.InstantNumberValue;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

final class InstantNumberMixin extends Mixin<InstantNumberValue> {

    private final long onNull;
    private final long onMissing;

    public InstantNumberMixin(InstantNumberValue options, JsonFactory factory) {
        super(factory, options);
        onNull = DateTimeUtils.epochNanos(options.onNull().orElse(null));
        onMissing = DateTimeUtils.epochNanos(options.onMissing().orElse(null));
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
        return Stream.of(Type.instantType());
    }

    @Override
    public ValueProcessor processor(String context) {
        return new LongValueProcessor(function());
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new LongRepeaterImpl(function(), allowMissing, allowNull, Type.instantType().arrayType());
    }

    private LongValueProcessor.ToLong function() {
        switch (options.format()) {
            case EPOCH_SECONDS:
                return new Impl(9);
            case EPOCH_MILLIS:
                return new Impl(6);
            case EPOCH_MICROS:
                return new Impl(3);
            case EPOCH_NANOS:
                return new Impl(0);
            default:
                throw new IllegalStateException();
        }
    }

    private class Impl implements LongValueProcessor.ToLong {

        private final int scaled;
        private final int mult;

        Impl(int scaled) {
            this.scaled = scaled;
            this.mult = BigInteger.valueOf(10).pow(scaled).intValueExact();
        }

        private long parseFromInt(JsonParser parser) throws IOException {
            return mult * Parsing.parseIntAsLong(parser);
        }

        private long parseFromDecimal(JsonParser parser) throws IOException {
            // We need to parse w/ BigDecimal in the case of VALUE_NUMBER_FLOAT, otherwise we might lose accuracy
            // jshell> (long)(1703292532.123456789 * 1000000000)
            // $4 ==> 1703292532123456768
            // See InstantNumberOptionsTest
            return Parsing.parseDecimalAsScaledLong(parser, scaled);
        }

        private long parseFromString(JsonParser parser) throws IOException {
            return mult * Parsing.parseStringAsLong(parser);
        }

        private long parseFromDecimalString(JsonParser parser) throws IOException {
            return Parsing.parseDecimalStringAsScaledLong(parser, scaled);
        }

        @Override
        public final long parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_NUMBER_INT:
                    if (!allowNumberInt()) {
                        throw Parsing.mismatch(parser, Instant.class);
                    }
                    return parseFromInt(parser);
                case VALUE_NUMBER_FLOAT:
                    if (!allowDecimal()) {
                        throw Parsing.mismatch(parser, Instant.class);
                    }
                    return parseFromDecimal(parser);
                case VALUE_STRING:
                case FIELD_NAME:
                    if (!allowString()) {
                        throw Parsing.mismatch(parser, Instant.class);
                    }
                    return allowDecimal()
                            ? parseFromDecimalString(parser)
                            : parseFromString(parser);
                case VALUE_NULL:
                    if (!allowNull()) {
                        throw Parsing.mismatch(parser, Instant.class);
                    }
                    return onNull;
            }
            throw Parsing.mismatch(parser, Instant.class);
        }

        @Override
        public final long parseMissing(JsonParser parser) throws IOException {
            if (!allowMissing()) {
                throw Parsing.mismatchMissing(parser, Instant.class);
            }
            return onMissing;
        }
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.InstantNumberOptions;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

final class InstantNumberMixin extends Mixin<InstantNumberOptions> {

    private final long onNull;
    private final long onMissing;

    public InstantNumberMixin(InstantNumberOptions options, JsonFactory factory) {
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
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return LongValueProcessor.of(out.get(0).asWritableLongChunk(), function());
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new LongRepeaterImpl(function(), allowMissing, allowNull, out.get(0).asWritableObjectChunk()::add);
    }

    private LongValueProcessor.ToLong function() {
        switch (options.format()) {
            case EPOCH_SECONDS:
                return new EpochSeconds();
            case EPOCH_MILLIS:
                return new EpochMillis();
            case EPOCH_MICROS:
                return new EpochMicros();
            case EPOCH_NANOS:
                return new EpochNanos();
            default:
                throw new IllegalStateException();
        }
    }

    private abstract class Base implements LongValueProcessor.ToLong {

        abstract long parseFromInt(JsonParser parser) throws IOException;

        abstract long parseFromDecimal(JsonParser parser) throws IOException;

        abstract long parseFromString(JsonParser parser) throws IOException;

        abstract long parseFromDecimalString(JsonParser parser) throws IOException;

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

    // We need to parse w/ BigDecimal in the case of VALUE_NUMBER_FLOAT, otherwise we might lose accuracy
    // jshell> (long)(1703292532.123456789 * 1000000000)
    // $4 ==> 1703292532123456768
    // See InstantNumberOptionsTest

    private class EpochSeconds extends Base {

        private static final int SCALED = 9;
        private static final int MULT = 1_000_000_000;

        private long epochNanos(long epochSeconds) {
            return MULT * epochSeconds;
        }

        @Override
        long parseFromInt(JsonParser parser) throws IOException {
            return epochNanos(Parsing.parseIntAsLong(parser));
        }

        @Override
        long parseFromDecimal(JsonParser parser) throws IOException {
            return Parsing.parseDecimalAsScaledTruncatedLong(parser, SCALED);
        }

        @Override
        long parseFromString(JsonParser parser) throws IOException {
            return epochNanos(Parsing.parseStringAsLong(parser));
        }

        @Override
        long parseFromDecimalString(JsonParser parser) throws IOException {
            return Parsing.parseDecimalStringAsScaledTruncatedLong(parser, SCALED);
        }
    }

    private class EpochMillis extends Base {
        private static final int SCALED = 6;
        private static final int MULT = 1_000_000;

        private long epochNanos(long epochMillis) {
            return MULT * epochMillis;
        }

        @Override
        long parseFromInt(JsonParser parser) throws IOException {
            return epochNanos(Parsing.parseIntAsLong(parser));
        }

        @Override
        long parseFromDecimal(JsonParser parser) throws IOException {
            return Parsing.parseDecimalAsScaledTruncatedLong(parser, SCALED);
        }

        @Override
        long parseFromString(JsonParser parser) throws IOException {
            return epochNanos(Parsing.parseStringAsLong(parser));
        }

        @Override
        long parseFromDecimalString(JsonParser parser) throws IOException {
            return Parsing.parseDecimalStringAsScaledTruncatedLong(parser, SCALED);
        }
    }

    private class EpochMicros extends Base {
        private static final int SCALED = 3;
        private static final int MULT = 1_000;

        private long epochNanos(long epochMicros) {
            return MULT * epochMicros;
        }

        @Override
        long parseFromInt(JsonParser parser) throws IOException {
            return epochNanos(Parsing.parseIntAsLong(parser));
        }

        @Override
        long parseFromDecimal(JsonParser parser) throws IOException {
            return Parsing.parseDecimalAsScaledTruncatedLong(parser, SCALED);
        }

        @Override
        long parseFromString(JsonParser parser) throws IOException {
            return epochNanos(Parsing.parseStringAsLong(parser));
        }

        @Override
        long parseFromDecimalString(JsonParser parser) throws IOException {
            return Parsing.parseDecimalStringAsScaledTruncatedLong(parser, SCALED);
        }
    }

    private class EpochNanos extends Base {

        @Override
        long parseFromInt(JsonParser parser) throws IOException {
            return Parsing.parseIntAsLong(parser);
        }

        @Override
        long parseFromDecimal(JsonParser parser) throws IOException {
            return Parsing.parseDecimalAsTruncatedLong(parser);
        }

        @Override
        long parseFromString(JsonParser parser) throws IOException {
            return Parsing.parseStringAsLong(parser);
        }

        @Override
        long parseFromDecimalString(JsonParser parser) throws IOException {
            return Parsing.parseDecimalStringAsTruncatedLong(parser);
        }
    }
}

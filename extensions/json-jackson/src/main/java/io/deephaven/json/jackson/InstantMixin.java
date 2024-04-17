//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.json.InstantValue;
import io.deephaven.json.jackson.LongValueProcessor.ToLong;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.stream.Stream;

final class InstantMixin extends Mixin<InstantValue> implements ToLong {

    private final long onNull;
    private final long onMissing;

    public InstantMixin(InstantValue options, JsonFactory factory) {
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
        return new LongValueProcessor(this);
    }

    @Override
    public long parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw Parsing.mismatch(parser, Instant.class);
    }

    @Override
    public long parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull) {
        return new LongRepeaterImpl(this, allowMissing, allowNull, Type.instantType().arrayType());
    }

    private long parseFromString(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = options.dateTimeFormatter().parse(Parsing.textAsCharSequence(parser));
        final long epochSeconds = accessor.getLong(ChronoField.INSTANT_SECONDS);
        final int nanoOfSecond = accessor.get(ChronoField.NANO_OF_SECOND);
        return epochSeconds * 1_000_000_000L + nanoOfSecond;
    }

    private long parseFromNull(JsonParser parser) throws IOException {
        if (!allowNull()) {
            throw Parsing.mismatch(parser, Instant.class);
        }
        return onNull;
    }

    private long parseFromMissing(JsonParser parser) throws IOException {
        if (!allowMissing()) {
            throw Parsing.mismatchMissing(parser, Instant.class);
        }
        return onMissing;
    }
}

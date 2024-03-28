//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.InstantOptions;
import io.deephaven.json.jackson.LongValueProcessor.ToLong;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.stream.Stream;

final class InstantMixin extends Mixin<InstantOptions> implements ToLong {

    public InstantMixin(InstantOptions options, JsonFactory factory) {
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
    public Stream<Type<?>> outputTypes() {
        return Stream.of(Type.instantType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return LongValueProcessor.of(out.get(0).asWritableLongChunk(), this);
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
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new LongRepeaterImpl(this, allowMissing, allowNull, out.get(0).asWritableObjectChunk()::add);
    }

    private long parseFromString(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = options.dateTimeFormatter().parse(Parsing.textAsCharSequence(parser));
        final long epochSeconds = accessor.getLong(ChronoField.INSTANT_SECONDS);
        final int nanoOfSecond = accessor.get(ChronoField.NANO_OF_SECOND);
        return epochSeconds * 1_000_000_000L + nanoOfSecond;
    }

    private long parseFromNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Parsing.mismatch(parser, Instant.class);
        }
        return DateTimeUtils.epochNanos(options.onNull().orElse(null));
    }

    private long parseFromMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Parsing.mismatchMissing(parser, Instant.class);
        }
        return DateTimeUtils.epochNanos(options.onMissing().orElse(null));
    }
}

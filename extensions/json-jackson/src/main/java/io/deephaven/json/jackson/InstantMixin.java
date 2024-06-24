//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.json.InstantValue;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.stream.Stream;

final class InstantMixin extends Mixin<InstantValue> {

    private final long onNull;
    private final long onMissing;

    public InstantMixin(InstantValue options, JsonFactory factory) {
        super(factory, options);
        onNull = DateTimeUtils.epochNanos(options.onNull().orElse(null));
        onMissing = DateTimeUtils.epochNanos(options.onMissing().orElse(null));
    }

    @Override
    public int outputSize() {
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
        return new InstantMixinProcessor();
    }

    private long parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw unexpectedToken(parser);
    }

    private long parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor() {
        return new RepeaterGenericImpl<>(new ToObjectImpl(), null, null,
                Type.instantType().arrayType());
    }

    class ToObjectImpl implements ToObject<Instant> {
        @Override
        public Instant parseValue(JsonParser parser) throws IOException {
            switch (parser.currentToken()) {
                case VALUE_STRING:
                case FIELD_NAME:
                    return parseFromStringToInstant(parser);
                case VALUE_NULL:
                    return parseFromNullToInstant(parser);
            }
            throw unexpectedToken(parser);
        }

        @Override
        public Instant parseMissing(JsonParser parser) throws IOException {
            return parseFromMissingToInstant(parser);
        }
    }

    private long parseFromString(JsonParser parser) throws IOException {
        final TemporalAccessor accessor = options.dateTimeFormatter().parse(Parsing.textAsCharSequence(parser));
        final long epochSeconds = accessor.getLong(ChronoField.INSTANT_SECONDS);
        final int nanoOfSecond = accessor.get(ChronoField.NANO_OF_SECOND);
        return epochSeconds * 1_000_000_000L + nanoOfSecond;
    }

    private long parseFromNull(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return onNull;
    }

    private long parseFromMissing(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return onMissing;
    }

    private Instant parseFromStringToInstant(JsonParser parser) throws IOException {
        return Instant.from(options.dateTimeFormatter().parse(Parsing.textAsCharSequence(parser)));
    }

    private Instant parseFromNullToInstant(JsonParser parser) throws IOException {
        checkNullAllowed(parser);
        return options.onNull().orElse(null);
    }

    private Instant parseFromMissingToInstant(JsonParser parser) throws IOException {
        checkMissingAllowed(parser);
        return options.onMissing().orElse(null);
    }

    private class InstantMixinProcessor extends ValueProcessorMixinBase {
        private WritableLongChunk<?> out;

        @Override
        public final void setContext(List<WritableChunk<?>> out) {
            this.out = out.get(0).asWritableLongChunk();
        }

        @Override
        public final void clearContext() {
            out = null;
        }

        @Override
        protected void processCurrentValueImpl(JsonParser parser) throws IOException {
            out.add(parseValue(parser));
        }

        @Override
        protected void processMissingImpl(JsonParser parser) throws IOException {
            out.add(parseMissing(parser));
        }
    }
}

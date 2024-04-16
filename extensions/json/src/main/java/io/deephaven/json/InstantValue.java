//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.lang.Runtime.Version;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Set;

/**
 * Processes a JSON string as an {@link Instant}.
 */
@Immutable
@BuildableStyle
public abstract class InstantValue extends ValueSingleValueBase<Instant> {

    private static final Version VERSION_12 = Version.parse("12");

    public static Builder builder() {
        return ImmutableInstantValue.builder();
    }

    /**
     * The standard {@link Instant} options. Allows missing and accepts {@link JsonValueTypes#stringOrNull()}.
     *
     * @return the standard Instant options
     */
    public static InstantValue standard() {
        return builder().build();
    }

    /**
     * The strict {@link Instant} options. Disallows missing and accepts {@link JsonValueTypes#string()}.
     *
     * @return the strict Instant options
     */
    public static InstantValue strict() {
        return builder()
                .allowMissing(false)
                .allowedTypes(JsonValueTypes.string())
                .build();
    }

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#stringOrNull()}. By default is
     * {@link JsonValueTypes#stringOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.stringOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.stringOrNull();
    }

    /**
     * The date-time formatter to use for {@link DateTimeFormatter#parse(CharSequence) parsing}. The parsed result must
     * support extracting {@link java.time.temporal.ChronoField#INSTANT_SECONDS INSTANT_SECONDS} and
     * {@link java.time.temporal.ChronoField#NANO_OF_SECOND NANO_OF_SECOND} fields. Defaults to
     * {@link DateTimeFormatter#ISO_INSTANT} for java versions 12+, and {@link DateTimeFormatter#ISO_OFFSET_DATE_TIME}
     * otherwise. These defaults will parse offsets, converting to UTC as necessary.
     *
     * @return the date-time formatter
     */
    @Default
    public DateTimeFormatter dateTimeFormatter() {
        // ISO_INSTANT became more versatile in 12+ (handling the parsing of offsets), and is likely more efficient, so
        // we should choose to use it when we can.
        return Runtime.version().compareTo(VERSION_12) >= 0
                ? DateTimeFormatter.ISO_INSTANT
                : DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueSingleValueBase.Builder<Instant, InstantValue, Builder> {

        Builder dateTimeFormatter(DateTimeFormatter formatter);
    }
}

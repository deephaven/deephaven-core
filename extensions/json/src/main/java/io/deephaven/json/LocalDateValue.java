//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Set;

/**
 * Processes a JSON string as an {@link LocalDate}.
 */
@Immutable
@BuildableStyle
public abstract class LocalDateValue extends ValueSingleValueBase<LocalDate> {
    public static Builder builder() {
        return ImmutableLocalDateValue.builder();
    }

    public static LocalDateValue standard() {
        return builder().build();
    }

    public static LocalDateValue strict() {
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
     * support extracting an {@link ChronoField#EPOCH_DAY EPOCH_DAY} field. Defaults to
     * {@link DateTimeFormatter#ISO_LOCAL_DATE}.
     *
     * @return the date-time formatter
     */
    @Default
    public DateTimeFormatter dateTimeFormatter() {
        return DateTimeFormatter.ISO_LOCAL_DATE;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueSingleValueBase.Builder<LocalDate, LocalDateValue, Builder> {

        Builder dateTimeFormatter(DateTimeFormatter formatter);
    }
}

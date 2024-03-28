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
import java.util.EnumSet;
import java.util.Set;

/**
 * Processes a JSON string as an {@link LocalDate}.
 */
@Immutable
@BuildableStyle
public abstract class LocalDateOptions extends BoxedOptions<LocalDate> {
    public static Builder builder() {
        return ImmutableLocalDateOptions.builder();
    }

    public static LocalDateOptions standard() {
        return builder().build();
    }

    public static LocalDateOptions strict() {
        return builder()
                .allowMissing(false)
                .desiredTypes(JsonValueTypes.STRING)
                .build();
    }

    @Default
    @Override
    public Set<JsonValueTypes> desiredTypes() {
        return JsonValueTypes.STRING_OR_NULL;
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

    public interface Builder extends BoxedOptions.Builder<LocalDate, LocalDateOptions, Builder> {

        Builder dateTimeFormatter(DateTimeFormatter formatter);
    }


    @Override
    final EnumSet<JsonValueTypes> allowableTypes() {
        return JsonValueTypes.STRING_OR_NULL;
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Instant;
import java.util.EnumSet;

/**
 * Processes a JSON number as an {@link Instant}.
 */
@Immutable
@BuildableStyle
public abstract class InstantNumberOptions extends ValueOptionsSingleValueBase<Instant> {

    public enum Format {
        EPOCH_SECONDS, EPOCH_MILLIS, EPOCH_MICROS, EPOCH_NANOS;

        public InstantNumberOptions lenient(boolean allowDecimal) {
            return builder()
                    .format(this)
                    .allowedTypes(allowDecimal ? JsonValueTypes.NUMBER_LIKE : JsonValueTypes.INT_LIKE)
                    .build();
        }

        public InstantNumberOptions standard(boolean allowDecimal) {
            return builder()
                    .format(this)
                    .allowedTypes(allowDecimal ? JsonValueTypes.NUMBER_OR_NULL : JsonValueTypes.INT_OR_NULL)
                    .build();
        }

        public InstantNumberOptions strict(boolean allowDecimal) {
            return builder()
                    .format(this)
                    .allowMissing(false)
                    .allowedTypes(allowDecimal ? JsonValueTypes.NUMBER : EnumSet.of(JsonValueTypes.INT))
                    .build();
        }
    }

    public static Builder builder() {
        return ImmutableInstantNumberOptions.builder();
    }

    /**
     * The format to use.
     */
    public abstract Format format();

    /**
     * {@inheritDoc} By default is {@link JsonValueTypes#INT_OR_NULL}.
     */
    @Default
    @Override
    public EnumSet<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.INT_OR_NULL;
    }

    /**
     * The universe, is {@link JsonValueTypes#NUMBER_LIKE}.
     */
    @Override
    public final EnumSet<JsonValueTypes> universe() {
        return JsonValueTypes.NUMBER_LIKE;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueOptionsSingleValueBase.Builder<Instant, InstantNumberOptions, Builder> {
        Builder format(Format format);
    }
}

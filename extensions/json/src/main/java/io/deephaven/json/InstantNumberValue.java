//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.time.Instant;
import java.util.Set;

/**
 * Processes a JSON number as an {@link Instant}.
 */
@Immutable
@BuildableStyle
public abstract class InstantNumberValue extends ValueSingleValueBase<Instant> {

    public enum Format {
        /**
         * Seconds from the epoch of 1970-01-01T00:00:00Z.
         */
        EPOCH_SECONDS,

        /**
         * Milliseconds from the epoch of 1970-01-01T00:00:00Z.
         */
        EPOCH_MILLIS,

        /**
         * Microseconds from the epoch of 1970-01-01T00:00:00Z.
         */
        EPOCH_MICROS,

        /**
         * Nanoseconds from the epoch of 1970-01-01T00:00:00Z.
         */
        EPOCH_NANOS;

        /**
         * The lenient {@link Instant} number options. Allows missing. If {@code allowDecimal}, accepts
         * {@link JsonValueTypes#numberLike()}, otherwise accepts {@link JsonValueTypes#intLike()}.
         *
         * @param allowDecimal if decimals should be allowed
         * @return the lenient Instant number options
         */
        public InstantNumberValue lenient(boolean allowDecimal) {
            return builder()
                    .format(this)
                    .allowedTypes(allowDecimal ? JsonValueTypes.numberLike() : JsonValueTypes.intLike())
                    .build();
        }

        /**
         * The standard {@link Instant} number options. Allows missing. If {@code allowDecimal}, accepts
         * {@link JsonValueTypes#numberOrNull()}, otherwise accepts {@link JsonValueTypes#intOrNull()}.
         *
         * @param allowDecimal if decimals should be allowed
         * @return the standard Instant number options
         */
        public InstantNumberValue standard(boolean allowDecimal) {
            return builder()
                    .format(this)
                    .allowedTypes(allowDecimal ? JsonValueTypes.numberOrNull() : JsonValueTypes.intOrNull())
                    .build();
        }

        /**
         * The strict {@link Instant} number options. Disallows missing. If {@code allowDecimal}, accepts
         * {@link JsonValueTypes#number()}, otherwise accepts {@link JsonValueTypes#int_()}.
         *
         * @param allowDecimal if decimals should be allowed
         * @return the lenient Instant number options
         */
        public InstantNumberValue strict(boolean allowDecimal) {
            return builder()
                    .format(this)
                    .allowMissing(false)
                    .allowedTypes(allowDecimal ? JsonValueTypes.number() : JsonValueTypes.int_())
                    .build();
        }
    }

    public static Builder builder() {
        return ImmutableInstantNumberValue.builder();
    }

    /**
     * The format to use.
     */
    public abstract Format format();

    /**
     * {@inheritDoc} Must be a subset of {@link JsonValueTypes#numberLike()}. By default is
     * {@link JsonValueTypes#intOrNull()}.
     */
    @Override
    @Default
    public Set<JsonValueTypes> allowedTypes() {
        return JsonValueTypes.intOrNull();
    }

    @Override
    final Set<JsonValueTypes> universe() {
        return JsonValueTypes.numberLike();
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ValueSingleValueBase.Builder<Instant, InstantNumberValue, Builder> {
        Builder format(Format format);
    }
}

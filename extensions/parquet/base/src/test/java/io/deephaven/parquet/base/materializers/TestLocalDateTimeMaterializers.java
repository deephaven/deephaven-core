//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.time.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TestLocalDateTimeMaterializers {

    private static final long POST_EPOCH_NANOS = 123456789123456789L;

    /**
     * Pre-Epoch with a non-zero nano-of-second, which is where truncating (rather than flooring) division produces a
     * negative nano-of-second and fails conversion.
     */
    private static final long PRE_EPOCH_NANOS = -123456789123456789L;

    private static LocalDateTime expected(final long nanos) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(0, nanos), ZoneId.of("UTC"));
    }

    private static void checkNanos(final long nanos) {
        assertThat(LocalDateTimeFromNanosMaterializer.convertValue(nanos)).isEqualTo(expected(nanos));
    }

    private static void checkMicros(final long nanos) {
        final long micros = DateTimeUtils.nanosToMicros(nanos);
        assertThat(LocalDateTimeFromMicrosMaterializer.convertValue(micros))
                .isEqualTo(expected(DateTimeUtils.microsToNanos(micros)));
    }

    private static void checkMillis(final long nanos) {
        final long millis = DateTimeUtils.nanosToMillis(nanos);
        assertThat(LocalDateTimeFromMillisMaterializer.convertValue(millis))
                .isEqualTo(expected(DateTimeUtils.millisToNanos(millis)));
    }

    /**
     * Post-Epoch nanoseconds, the case that needs no sign handling at all.
     */
    @Test
    void testEpochNanosTo() {
        checkNanos(POST_EPOCH_NANOS);
    }

    /**
     * Post-Epoch microseconds. The expected value is derived from the truncated microsecond value, not the original
     * nanoseconds, since the write already discarded that precision.
     */
    @Test
    void testEpochMicrosTo() {
        checkMicros(POST_EPOCH_NANOS);
    }

    /**
     * Post-Epoch milliseconds, derived the same way as {@link #testEpochMicrosTo()}.
     */
    @Test
    void testEpochMillisTo() {
        checkMillis(POST_EPOCH_NANOS);
    }

    /**
     * Pre-Epoch nanoseconds, then a reproducer value spelled out literally. Both used to throw
     * {@link java.time.DateTimeException} for a negative nano-of-second.
     */
    @Test
    void testPreEpochNanosTo() {
        checkNanos(PRE_EPOCH_NANOS);
        assertThat(LocalDateTimeFromNanosMaterializer.convertValue(-2194687799876543211L))
                .isEqualTo(LocalDateTime.parse("1900-06-15T12:30:00.123456789"));
    }

    /**
     * Pre-Epoch microseconds. Truncating to microseconds leaves a non-zero sub-second part, so this still exercises the
     * sign handling.
     */
    @Test
    void testPreEpochMicrosTo() {
        checkMicros(PRE_EPOCH_NANOS);
    }

    /**
     * Pre-Epoch milliseconds, which likewise keeps a non-zero sub-second part after truncation.
     */
    @Test
    void testPreEpochMillisTo() {
        checkMillis(PRE_EPOCH_NANOS);
    }

    /**
     * The Epoch itself and one unit either side of it, where flooring rather than truncating changes which second the
     * value lands in.
     */
    @Test
    void testEpochBoundary() {
        assertThat(LocalDateTimeFromNanosMaterializer.convertValue(0L)).isEqualTo(expected(0L));
        assertThat(LocalDateTimeFromNanosMaterializer.convertValue(-1L)).isEqualTo(expected(-1L));
        assertThat(LocalDateTimeFromNanosMaterializer.convertValue(1L)).isEqualTo(expected(1L));
        assertThat(LocalDateTimeFromMicrosMaterializer.convertValue(-1L)).isEqualTo(expected(-1_000L));
        assertThat(LocalDateTimeFromMillisMaterializer.convertValue(-1L)).isEqualTo(expected(-1_000_000L));
    }
}

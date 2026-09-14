//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DH-23557 finding 3: the {@code LocalDateTimeFrom*Materializer} classes split an epoch offset into a
 * second-and-nano-of-second pair with the truncating {@code /} and {@code %} operators. For a pre-epoch value the
 * remainder is negative, and {@link LocalDateTime#ofEpochSecond} rejects a negative nano-of-second:
 *
 * <pre>
 * java.time.DateTimeException: Invalid value for NanoOfSecond (valid values 0 - 999999999): -1
 * </pre>
 *
 * <p>
 * Every pre-epoch value with a non-zero sub-second part was affected, at all three parquet precisions, so such a value
 * could be written to parquet but never read back. {@link TestLocalDateTimeMaterializers} tests one positive value per
 * precision, which is why this survived.
 *
 * <p>
 * {@link Instant#ofEpochSecond(long, long)} normalizes correctly across zero, so it serves as the oracle here.
 *
 * <p>
 * Fuzzer case seed {@code 8750790217018904276L}; 20 of the 89 failures in the run that found it.
 */
class PreEpochLocalDateTimeMaterializerTest {

    private static LocalDateTime expectedFromNanos(final long nanos) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(0L, nanos), ZoneOffset.UTC);
    }

    /** The exact value from the original failure: one nanosecond before the epoch. */
    @Test
    void oneNanoBeforeEpoch() {
        assertThat(LocalDateTimeFromNanosMaterializer.convertValue(-1L))
                .isEqualTo(LocalDateTime.parse("1969-12-31T23:59:59.999999999"));
    }

    @Test
    void preEpochNanos() {
        for (final long nanos : new long[] {
                -1L, -999_999_999L, -1_000_000_000L, -1_000_000_001L, -876_543_211L,
                -123456789123456789L, Long.MIN_VALUE + 1L}) {
            assertThat(LocalDateTimeFromNanosMaterializer.convertValue(nanos))
                    .describedAs("nanos=%d", nanos)
                    .isEqualTo(expectedFromNanos(nanos));
        }
    }

    @Test
    void preEpochMicros() {
        for (final long micros : new long[] {-1L, -999_999L, -1_000_000L, -1_000_001L, -123456789123456L}) {
            assertThat(LocalDateTimeFromMicrosMaterializer.convertValue(micros))
                    .describedAs("micros=%d", micros)
                    .isEqualTo(expectedFromNanos(micros * 1_000L));
        }
    }

    @Test
    void preEpochMillis() {
        for (final long millis : new long[] {-1L, -999L, -1_000L, -1_001L, -123456789123L}) {
            assertThat(LocalDateTimeFromMillisMaterializer.convertValue(millis))
                    .describedAs("millis=%d", millis)
                    .isEqualTo(expectedFromNanos(millis * 1_000_000L));
        }
    }

    /** Whole pre-epoch seconds already worked -- the remainder is zero -- and must keep working. */
    @Test
    void preEpochWholeSeconds() {
        assertThat(LocalDateTimeFromNanosMaterializer.convertValue(-1_000_000_000L))
                .isEqualTo(LocalDateTime.parse("1969-12-31T23:59:59"));
        assertThat(LocalDateTimeFromMillisMaterializer.convertValue(-1_000L))
                .isEqualTo(LocalDateTime.parse("1969-12-31T23:59:59"));
    }

    /** Post-epoch and zero are unaffected by the change. */
    @Test
    void epochAndPostEpochUnchanged() {
        assertThat(LocalDateTimeFromNanosMaterializer.convertValue(0L))
                .isEqualTo(LocalDateTime.parse("1970-01-01T00:00:00"));
        for (final long nanos : new long[] {1L, 999_999_999L, 1_000_000_000L, 123456789123456789L}) {
            assertThat(LocalDateTimeFromNanosMaterializer.convertValue(nanos))
                    .describedAs("nanos=%d", nanos)
                    .isEqualTo(expectedFromNanos(nanos));
        }
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;

import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * DH-23557 finding 2: {@link DateTimeUtils#epochNanos(Instant)} decided representability from the seconds alone, which
 * both rejected representable values and silently corrupted unrepresentable ones.
 *
 * <p>
 * {@code safeComputeNanos} threw when {@code epochSecond >= Long.MAX_VALUE / 1_000_000_000L}, i.e. at
 * {@code 9223372036}. But {@code 9223372036 * 1_000_000_000 + 854775807} is exactly {@link Long#MAX_VALUE}, so the
 * whole final second of the range -- every instant from {@code 2262-04-11T23:47:16Z} through
 * {@code 2262-04-11T23:47:16.854775807Z} -- was rejected despite being representable. Symmetrically, no check applied
 * to large negative seconds, so an instant far below the range multiplied and added without complaint and wrapped to an
 * unrelated value: {@code 1000-01-01T00:00:00Z} came back as a positive nano count.
 *
 * <p>
 * The pushdown fuzzer reached the first half by writing an {@code Instant} column holding a top-of-range value: 25 of
 * the 89 failures in the run that found it. Fuzzer case seed {@code 8750790217018904276L}.
 *
 * @see DateTimeUtils#epochNanos(Instant)
 */
public class EpochNanosBoundaryTest {

    /** The last representable instant: Long.MAX_VALUE nanos from the epoch. */
    private static final Instant MAX_INSTANT = Instant.ofEpochSecond(9223372036L, 854775807L);
    /** The first representable instant above the null sentinel: Long.MIN_VALUE + 1 nanos from the epoch. */
    private static final Instant MIN_INSTANT = Instant.ofEpochSecond(-9223372037L, 145224193L);

    @Test
    public void maxRepresentableInstantConverts() {
        assertEquals(Long.MAX_VALUE, DateTimeUtils.epochNanos(MAX_INSTANT));
    }

    /** The whole top second was rejected, not just its last nanosecond. */
    @Test
    public void topSecondOfRangeConverts() {
        assertEquals(9223372036_000000000L, DateTimeUtils.epochNanos(Instant.ofEpochSecond(9223372036L, 0L)));
        assertEquals(9223372036_000000001L, DateTimeUtils.epochNanos(Instant.ofEpochSecond(9223372036L, 1L)));
        assertEquals(9223372036_854775806L, DateTimeUtils.epochNanos(Instant.ofEpochSecond(9223372036L, 854775806L)));
    }

    /** The bottom of the range worked before the fix, by modular arithmetic, and must keep working. */
    @Test
    public void minRepresentableInstantConverts() {
        assertEquals(Long.MIN_VALUE + 1L, DateTimeUtils.epochNanos(MIN_INSTANT));
        assertEquals(-9223372036_854775806L,
                DateTimeUtils.epochNanos(Instant.ofEpochSecond(-9223372037L, 145224194L)));
        assertEquals(-9223372036_000000000L, DateTimeUtils.epochNanos(Instant.ofEpochSecond(-9223372036L, 0L)));
    }

    /** One nanosecond past either end must be rejected, not wrapped. */
    @Test
    public void justOutOfRangeThrows() {
        assertThrows(DateTimeUtils.DateTimeOverflowException.class,
                () -> DateTimeUtils.epochNanos(Instant.ofEpochSecond(9223372036L, 854775808L)));
        assertThrows(DateTimeUtils.DateTimeOverflowException.class,
                () -> DateTimeUtils.epochNanos(Instant.ofEpochSecond(-9223372037L, 145224191L)));
    }

    /**
     * The silent-corruption half. Before the fix these returned wrapped garbage -- the year 1000 came back as a
     * <em>positive</em> nano count, i.e. a date in the future.
     */
    @Test
    public void farOutOfRangeThrowsRatherThanWrapping() {
        assertThrows(DateTimeUtils.DateTimeOverflowException.class,
                () -> DateTimeUtils.epochNanos(Instant.parse("1000-01-01T00:00:00Z")));
        assertThrows(DateTimeUtils.DateTimeOverflowException.class,
                () -> DateTimeUtils.epochNanos(Instant.ofEpochSecond(-9223372038L, 0L)));
        assertThrows(DateTimeUtils.DateTimeOverflowException.class,
                () -> DateTimeUtils.epochNanos(Instant.ofEpochSecond(9223372037L, 0L)));
        assertThrows(DateTimeUtils.DateTimeOverflowException.class,
                () -> DateTimeUtils.epochNanos(Instant.MAX));
        assertThrows(DateTimeUtils.DateTimeOverflowException.class,
                () -> DateTimeUtils.epochNanos(Instant.MIN));
    }

    /** {@code epochNanos(ZonedDateTime)} shares safeComputeNanos, so it shares both halves of the defect. */
    @Test
    public void zonedDateTimeBoundariesMatch() {
        assertEquals(Long.MAX_VALUE, DateTimeUtils.epochNanos(MAX_INSTANT.atZone(ZoneOffset.UTC)));
        assertEquals(Long.MIN_VALUE + 1L, DateTimeUtils.epochNanos(MIN_INSTANT.atZone(ZoneOffset.UTC)));
        assertThrows(DateTimeUtils.DateTimeOverflowException.class,
                () -> DateTimeUtils.epochNanos(ZonedDateTime.of(1000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)));
    }

    /** Ordinary values are untouched. */
    @Test
    public void ordinaryValuesRoundTrip() {
        for (final String text : new String[] {
                "1970-01-01T00:00:00Z", "1969-12-31T23:59:59.999999999Z", "2024-02-29T12:00:00.123456789Z",
                "1900-06-15T12:30:00.123456789Z", "2262-04-11T00:00:00Z"}) {
            final Instant instant = Instant.parse(text);
            final long nanos = DateTimeUtils.epochNanos(instant);
            assertEquals(text, instant, DateTimeUtils.epochNanosToInstant(nanos));
        }
    }

    /** The whole representable range must convert without throwing; sample it densely at the edges. */
    @Test
    public void everyRepresentableNanoConverts() {
        for (final long nanos : new long[] {
                Long.MIN_VALUE + 1L, Long.MIN_VALUE + 2L, -9223372036_000000000L, -1_000_000_001L, -1_000_000_000L,
                -999_999_999L, -1L, 0L, 1L, 999_999_999L, 1_000_000_000L, 9223372036_000000000L, Long.MAX_VALUE - 1L,
                Long.MAX_VALUE}) {
            final Instant instant = Instant.ofEpochSecond(
                    Math.floorDiv(nanos, 1_000_000_000L), Math.floorMod(nanos, 1_000_000_000L));
            assertEquals("nanos=" + nanos, nanos, DateTimeUtils.epochNanos(instant));
        }
    }
}

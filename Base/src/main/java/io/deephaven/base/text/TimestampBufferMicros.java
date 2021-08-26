/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.text;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.TimeZone;

public class TimestampBufferMicros {

    private final ZoneRules zoneRules;

    private class ThreadLocalState {
        private long currentTimeMicros = Long.MIN_VALUE; // Ensure we enter the calculation logic the first time through
        // Magic values for the previous/next transition times:
        // MAX_VALUE/MIN_VALUE mean they haven't been initialized
        // MIN_VALUE/MAX_VALUE mean they don't have previous/next transitions (e.g. GMT)
        private long previousDSTTransitionMicros = Long.MAX_VALUE;
        private long nextDSTTransitionMicros = Long.MIN_VALUE;
        private long gmtOffsetMicros;
        private byte[] gmtOffsetSuffix;
        private final ByteBuffer buffer = ByteBuffer.allocate(Convert.MAX_ISO8601_MICROS_BYTES);

        public void update(long nowMicros) {
            // See if the change is more than the seconds/millis/micros component - if so then we should check for DST
            // transition and
            // regenerate the whole buffer
            if (nowMicros / 60_000_000 != currentTimeMicros / 60_000_000) {
                if ((nowMicros < previousDSTTransitionMicros) ||
                        (nowMicros >= nextDSTTransitionMicros)) {
                    calculateDSTTransitions(nowMicros);
                }

                buffer.clear();
                Convert.appendISO8601Micros(nowMicros + gmtOffsetMicros, gmtOffsetSuffix, buffer);
                buffer.flip();
            } else {
                long v = (nowMicros + gmtOffsetMicros) % 60_000_000;
                if (v < 0) {
                    v += 60_000_000;
                } // for dates before the epoch
                buffer.put(Convert.ISO8601_SECOND_OFFSET, (byte) ('0' + (v / 10_000_000)));
                buffer.put(Convert.ISO8601_SECOND_OFFSET + 1, (byte) ('0' + (v % 10_000_000) / 1_000_000));
                buffer.put(Convert.ISO8601_MILLIS_OFFSET, (byte) ('0' + (v % 1_000_000) / 100_000));
                buffer.put(Convert.ISO8601_MILLIS_OFFSET + 1, (byte) ('0' + (v % 100_000) / 10_000));
                buffer.put(Convert.ISO8601_MILLIS_OFFSET + 2, (byte) ('0' + (v % 10_000) / 1000));
                buffer.put(Convert.ISO8601_MICROS_OFFSET, (byte) ('0' + (v % 1000) / 100));
                buffer.put(Convert.ISO8601_MICROS_OFFSET + 1, (byte) ('0' + (v % 100) / 10));
                buffer.put(Convert.ISO8601_MICROS_OFFSET + 2, (byte) ('0' + (v % 10)));
            }
            currentTimeMicros = nowMicros;
        }

        private void calculateDSTTransitions(final long nowMicros) {
            final Instant nowInstant = Instant.ofEpochMilli(nowMicros / 1000);
            final ZoneOffsetTransition previousTransitionOffset = zoneRules.previousTransition(nowInstant);
            final ZoneOffsetTransition nextTransitionOffset = zoneRules.nextTransition(nowInstant);

            // It's possible there's no previous or next transition, in that case set the value so we'll never cross it
            previousDSTTransitionMicros =
                    previousTransitionOffset != null ? previousTransitionOffset.toEpochSecond() * 1000L * 1000L
                            : Long.MIN_VALUE;
            nextDSTTransitionMicros =
                    nextTransitionOffset != null ? nextTransitionOffset.toEpochSecond() * 1000L * 1000L
                            : Long.MAX_VALUE;

            gmtOffsetMicros = zoneRules.getOffset(nowInstant).getTotalSeconds() * 1000L * 1000L;

            if (gmtOffsetMicros == 0) {
                gmtOffsetSuffix = new byte[] {'Z'};
            } else {
                gmtOffsetSuffix = new byte[5];
                gmtOffsetSuffix[0] = (byte) (gmtOffsetMicros < 0 ? '-' : '+');
                int hours = (int) Math.abs(gmtOffsetMicros / 3_600_000 / 1000);
                int minutes = (int) ((Math.abs(gmtOffsetMicros / 1000) - hours * 3_600_000) / 60_000);
                gmtOffsetSuffix[1] = (byte) ('0' + hours / 10);
                gmtOffsetSuffix[2] = (byte) ('0' + hours % 10);
                gmtOffsetSuffix[3] = (byte) ('0' + minutes / 10);
                gmtOffsetSuffix[4] = (byte) ('0' + minutes % 10);
            }
        }
    }

    private ThreadLocal<ThreadLocalState> threadLocals = ThreadLocal.withInitial(ThreadLocalState::new);

    public TimestampBufferMicros(TimeZone tz) {
        zoneRules = tz.toZoneId().getRules();
    }

    @Deprecated
    public TimestampBufferMicros(long localNowMicros, TimeZone tz) {
        this(tz);
    }

    /**
     * Return a thread-local byte buffer containing the give time, formatted in ISO 8601. The buffer's position is set
     * to zero in preparation for writing its contents to another buffer or a channel. Since the buffer is thread-local,
     * the caller can safely assume that it won't change until the same thread accesses this TimestampBuffer again.
     */
    public ByteBuffer getTimestamp(long nowMicros) {
        ThreadLocalState state = threadLocals.get();
        state.update(nowMicros);
        state.buffer.position(0);
        return state.buffer;
    }

    /**
     * Format the current time into a ByteBuffer in according to ISO 8601.
     * 
     * @throws java.nio.BufferOverflowException if there is not enough room in the destination buffer.
     */
    public void getTimestamp(long nowMicros, ByteBuffer dest) {
        dest.put(getTimestamp(nowMicros));
    }
}

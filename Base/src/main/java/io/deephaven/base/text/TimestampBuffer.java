/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.text;

import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;
import java.time.Instant;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.TimeZone;

public class TimestampBuffer {

    private final ZoneRules zoneRules;

    private class ThreadLocalState {
        private long currentTimeMillis = Long.MIN_VALUE; // Ensure we enter the calculation logic
                                                         // the first time through
        // Magic values for the previous/next transition times:
        // MAX_VALUE/MIN_VALUE mean they haven't been initialized
        // MIN_VALUE/MAX_VALUE mean they don't have previous/next transitions (e.g. GMT)
        private long previousDSTTransitionMillis = Long.MAX_VALUE;
        private long nextDSTTransitionMillis = Long.MIN_VALUE;
        private long gmtOffsetMillis;
        private byte[] gmtOffsetSuffix;
        private final ByteBuffer buffer = ByteBuffer.allocate(Convert.MAX_ISO8601_BYTES);

        public void update(long nowMillis) {
            // See if the change is more than the seconds/millis component - if so then we should
            // check for DST transition and
            // regenerate the whole buffer
            if (nowMillis / 60_000 != currentTimeMillis / 60_000) {
                if ((nowMillis < previousDSTTransitionMillis) ||
                    (nowMillis >= nextDSTTransitionMillis)) {
                    calculateDSTTransitions(nowMillis);
                }

                buffer.clear();
                Convert.appendISO8601Millis(nowMillis + gmtOffsetMillis, gmtOffsetSuffix, buffer);
                buffer.flip();
            } else {
                long v = (nowMillis + gmtOffsetMillis) % 60_000;
                if (v < 0) {
                    v += 60_000;
                } // for dates before the epoch
                buffer.put(Convert.ISO8601_SECOND_OFFSET, (byte) ('0' + (v / 10_000)));
                buffer.put(Convert.ISO8601_SECOND_OFFSET + 1, (byte) ('0' + (v % 10_000) / 1000));
                buffer.put(Convert.ISO8601_MILLIS_OFFSET, (byte) ('0' + (v % 1000) / 100));
                buffer.put(Convert.ISO8601_MILLIS_OFFSET + 1, (byte) ('0' + (v % 100) / 10));
                buffer.put(Convert.ISO8601_MILLIS_OFFSET + 2, (byte) ('0' + (v % 10)));
            }
            currentTimeMillis = nowMillis;
        }

        private void calculateDSTTransitions(final long nowMillis) {
            final Instant nowInstant = Instant.ofEpochMilli(nowMillis);
            final ZoneOffsetTransition previousTransitionOffset =
                zoneRules.previousTransition(nowInstant);
            final ZoneOffsetTransition nextTransitionOffset = zoneRules.nextTransition(nowInstant);

            // It's possible there's no previous or next transition, in that case set the value so
            // we'll never cross it
            previousDSTTransitionMillis =
                previousTransitionOffset != null ? previousTransitionOffset.toEpochSecond() * 1000
                    : Long.MIN_VALUE;
            nextDSTTransitionMillis =
                nextTransitionOffset != null ? nextTransitionOffset.toEpochSecond() * 1000
                    : Long.MAX_VALUE;

            gmtOffsetMillis = zoneRules.getOffset(nowInstant).getTotalSeconds() * 1000L;

            if (gmtOffsetMillis == 0) {
                gmtOffsetSuffix = new byte[] {'Z'};
            } else {
                gmtOffsetSuffix = new byte[5];
                gmtOffsetSuffix[0] = (byte) (gmtOffsetMillis < 0 ? '-' : '+');
                int hours = (int) Math.abs(gmtOffsetMillis / 3_600_000);
                int minutes = (int) ((Math.abs(gmtOffsetMillis) - hours * 3600_000) / 60_000);
                gmtOffsetSuffix[1] = (byte) ('0' + hours / 10);
                gmtOffsetSuffix[2] = (byte) ('0' + hours % 10);
                gmtOffsetSuffix[3] = (byte) ('0' + minutes / 10);
                gmtOffsetSuffix[4] = (byte) ('0' + minutes % 10);
            }
        }
    }

    private ThreadLocal<TimestampBuffer.ThreadLocalState> threadLocals =
        ThreadLocal.withInitial(TimestampBuffer.ThreadLocalState::new);

    public TimestampBuffer(TimeZone tz) {
        zoneRules = tz.toZoneId().getRules();
    }

    @Deprecated
    public TimestampBuffer(long localNow, TimeZone tz) {
        this(tz);
    }

    /**
     * Return a thread-local byte buffer containing the give time, formatted in ISO 8601. The
     * buffer's position is set to zero in preparation for writing its contents to another buffer or
     * a channel. Since the buffer is thread-local, the caller can safely assume that it won't
     * change until the same thread accesses this TimestampBuffer again.
     */
    public ByteBuffer getTimestamp(long nowMillis) {
        ThreadLocalState state = threadLocals.get();
        state.update(nowMillis);
        state.buffer.position(0);
        return state.buffer;
    }

    /**
     * Format the current time into a ByteBuffer in according to ISO 8601.
     * 
     * @throws BufferOverflowException if there is not enough room in the destination buffer.
     */
    public void getTimestamp(long nowMillis, ByteBuffer dest) {
        dest.put(getTimestamp(nowMillis));
    }
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.clock.impl;

import com.google.auto.service.AutoService;
import io.deephaven.clock.RealTimeClock;
import jdk.internal.misc.VM;

import java.time.Instant;

/**
 * Clock implementation derived from {@link java.time.Clock#systemUTC()}. The {@link #currentTimeNanos()} method returns
 * the same time represented by {@link Instant#now()} without allocation.
 */

@AutoService(RealTimeClock.class)
public final class JdkInternalsRealTimeClock implements RealTimeClock {

    @Override
    public long currentTimeMicros() {
        return currentTimeNanos() / 1000L;
    }

    // We don't actually need a volatile here.
    // We don't care if offset is set or read concurrently by multiple
    // threads - we just need a value which is 'recent enough' - in other
    // words something that has been updated at least once in the last
    // 2^32 secs (~136 years). And even if we by chance see an invalid
    // offset, the worst that can happen is that we will get a -1 value
    // from getNanoTimeAdjustment, forcing us to update the offset
    // once again.
    private transient long offset;

    /**
     * Returns the current time in nanoseconds. This is the same moment in time {@link Instant#now()} would return, but
     * represented (without allocation) as a single {@code long} instead of as an {@link Instant} object.
     * 
     * @return the difference, measured in nanoseconds (with precision determined by the result of
     *         {@link VM#getNanoTimeAdjustment}), between the current time and midnight, January 1, 1970 UTC.
     */
    @Override
    public long currentTimeNanos() {
        // Take a local copy of offset. offset can be updated concurrently
        // by other threads (even if we haven't made it volatile) so we will
        // work with a local copy.
        long localOffset = offset;
        long adjustment = VM.getNanoTimeAdjustment(localOffset);

        if (adjustment == -1) {
            // -1 is a sentinel value returned by VM.getNanoTimeAdjustment
            // when the offset it is given is too far off the current UTC
            // time. In principle, this should not happen unless the
            // JVM has run for more than ~136 years (not likely) or
            // someone is fiddling with the system time, or the offset is
            // by chance at 1ns in the future (very unlikely).
            // We can easily recover from all these conditions by bringing
            // back the offset in range and retry.

            // bring back the offset in range. We use -1024 to make
            // it more unlikely to hit the 1ns in the future condition.
            localOffset = System.currentTimeMillis() / 1000 - 1024;

            // retry
            adjustment = VM.getNanoTimeAdjustment(localOffset);

            if (adjustment == -1) {
                // Should not happen: we just recomputed a new offset.
                // It should have fixed the issue.
                throw new InternalError("Offset " + localOffset + " is not in range");
            } else {
                // OK - recovery succeeded. Update the offset for the
                // next call...
                offset = localOffset;
            }
        }
        return Math.addExact(localOffset * 1_000_000_000L, adjustment);
    }
}

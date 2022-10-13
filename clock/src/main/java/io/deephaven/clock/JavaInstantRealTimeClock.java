package io.deephaven.clock;

import java.time.Instant;

/**
 * Created by rbasralian on 10/13/22
 */
public final class JavaInstantRealTimeClock implements RealTimeClock {

    @Override
    public long currentTimeNanos() {
        final Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }
}

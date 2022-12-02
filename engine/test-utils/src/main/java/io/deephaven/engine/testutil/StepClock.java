package io.deephaven.engine.testutil;

import io.deephaven.base.clock.ClockNanoBase;

public class StepClock extends ClockNanoBase implements Runnable {

    private final long nanoTimes[];

    private int step;

    public StepClock(final long... nanoTimes) {
        this.nanoTimes = nanoTimes;
        reset();
    }

    @Override
    public long currentTimeNanos() {
        return nanoTimes[step];
    }

    @Override
    public void run() {
        step = Math.min(step + 1, nanoTimes.length - 1);
    }

    public void reset() {
        step = 0;
    }
}

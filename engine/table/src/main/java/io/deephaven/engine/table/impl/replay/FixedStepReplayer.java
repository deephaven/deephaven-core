/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.replay;

import io.deephaven.engine.time.DateTime;
import io.deephaven.engine.time.DateTimeUtil;

public class FixedStepReplayer extends Replayer {
    private long incrementNanos;
    private DateTime currentTime;

    public FixedStepReplayer(DateTime startTime, DateTime endTime, long incrementNanos) {
        super(startTime, endTime);
        this.incrementNanos = incrementNanos;
        currentTime = startTime;
    }

    @Override
    public DateTime currentTime() {
        return currentTime;
    }

    @Override
    public void run() {
        currentTime = DateTimeUtil.plus(currentTime, incrementNanos);
        if (currentTime.getNanos() > endTime.getNanos()) {
            currentTime = endTime;
        }
        super.run();
    }

    @Override
    public void setTime(long updatedTime) {
        currentTime = DateTimeUtil.millisToTime(Math.max(updatedTime, currentTime.getMillis()));
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.replay;

import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.tables.utils.DBTimeUtils;

public class FixedStepReplayer extends Replayer {
    private long incrementNanos;
    private DBDateTime currentTime;

    public FixedStepReplayer(DBDateTime startTime, DBDateTime endTime, long incrementNanos) {
        super(startTime, endTime);
        this.incrementNanos = incrementNanos;
        currentTime = startTime;
    }

    @Override
    public DBDateTime currentTime() {
        return currentTime;
    }

    @Override
    public void run() {
        currentTime = DBTimeUtils.plus(currentTime, incrementNanos);
        if (currentTime.getNanos() > endTime.getNanos()) {
            currentTime = endTime;
        }
        super.run();
    }

    @Override
    public void setTime(long updatedTime) {
        currentTime = DBTimeUtils.millisToTime(Math.max(updatedTime, currentTime.getMillis()));
    }
}

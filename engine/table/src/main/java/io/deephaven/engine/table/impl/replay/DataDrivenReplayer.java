//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.replay;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.clock.ClockNanoBase;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.ColumnSource;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.engine.rowset.RowSet;

import java.time.Instant;

public class DataDrivenReplayer extends Replayer {

    private Instant currentTime;
    private int pos;
    private long lastTime = -1;

    public DataDrivenReplayer(Instant startTime, Instant endTime) {
        super(startTime, endTime);
        currentTime = startTime;
    }

    TLongArrayList allTimestamp = new TLongArrayList();

    @Override
    public void registerTimeSource(RowSet rowSet, ColumnSource<Instant> timestampSource) {
        long prevValue = -1;
        if (timestampSource.allowsReinterpret(long.class)) {
            ColumnSource<Long> longColumn = timestampSource.reinterpret(long.class);
            for (RowSet.Iterator iterator = rowSet.iterator(); iterator.hasNext();) {
                long currentValue = longColumn.getLong(iterator.nextLong());
                if (currentValue != prevValue) {
                    allTimestamp.add(prevValue = currentValue);
                }
            }
        } else {
            for (RowSet.Iterator iterator = rowSet.iterator(); iterator.hasNext();) {
                Instant instant = timestampSource.get(iterator.nextLong());
                long currentValue = DateTimeUtils.epochNanos(instant);
                if (currentValue != prevValue) {
                    allTimestamp.add(prevValue = currentValue);
                }
            }

        }
    }

    @Override
    public void start() {
        allTimestamp.sort();
        while (pos < allTimestamp.size() && allTimestamp.get(pos) < DateTimeUtils.epochNanos(startTime)) {
            pos++;
        }
        super.start();
    }

    @Override
    public void run() {
        long currentTimeNanos = -1;
        while (pos < allTimestamp.size()) {
            currentTimeNanos = allTimestamp.get(pos);
            if (currentTimeNanos > lastTime || currentTimeNanos > DateTimeUtils.epochNanos(endTime)) {
                break;
            }
            pos++;
        }
        if (DateTimeUtils.epochNanos(currentTime) > DateTimeUtils.epochNanos(endTime) || pos >= allTimestamp.size()) {
            currentTime = endTime;
        } else {
            currentTime = DateTimeUtils.epochNanosToInstant(currentTimeNanos);
        }
        lastTime = currentTimeNanos;
        super.run();
    }

    @Override
    public void setTime(long updatedTime) {
        currentTime = DateTimeUtils.epochMillisToInstant(Math.max(updatedTime, currentTime.toEpochMilli()));
    }

    @Override
    public Clock clock() {
        return new ClockImpl();
    }

    private class ClockImpl extends ClockNanoBase {

        @Override
        public long currentTimeNanos() {
            return DateTimeUtils.epochNanos(currentTime);
        }
    }
}

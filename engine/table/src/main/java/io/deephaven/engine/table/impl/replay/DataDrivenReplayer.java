/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.replay;

import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.table.ColumnSource;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.engine.rowset.RowSet;

public class DataDrivenReplayer extends Replayer {
    private DateTime currentTime;
    private int pos;
    private long lastTime = -1;

    public DataDrivenReplayer(DateTime startTime, DateTime endTime) {
        super(startTime, endTime);
        currentTime = startTime;
    }

    @Override
    public DateTime currentTime() {
        return currentTime;
    }

    TLongArrayList allTimestamp = new TLongArrayList();

    @Override
    public void registerTimeSource(RowSet rowSet, ColumnSource<DateTime> timestampSource) {
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
                long currentValue = timestampSource.get(iterator.nextLong()).getNanos();
                if (currentValue != prevValue) {
                    allTimestamp.add(prevValue = currentValue);
                }
            }

        }
    }

    @Override
    public void start() {
        allTimestamp.sort();
        while (pos < allTimestamp.size() && allTimestamp.get(pos) < startTime.getNanos()) {
            pos++;
        }
        super.start();
    }

    @Override
    public void run() {
        long currentTimeNanos = -1;
        while (pos < allTimestamp.size()) {
            currentTimeNanos = allTimestamp.get(pos);
            if (currentTimeNanos > lastTime || currentTimeNanos > endTime.getNanos()) {
                break;
            }
            pos++;
        }
        if (currentTime.getNanos() > endTime.getNanos() || pos >= allTimestamp.size()) {
            currentTime = endTime;
        } else {
            currentTime = new DateTime(currentTimeNanos);
        }
        lastTime = currentTimeNanos;
        super.run();
    }

    @Override
    public void setTime(long updatedTime) {
        currentTime = DateTimeUtils.millisToTime(Math.max(updatedTime, currentTime.getMillis()));
    }
}

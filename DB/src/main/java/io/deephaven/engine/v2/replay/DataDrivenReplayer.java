/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.replay;

import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.tables.utils.DBTimeUtils;
import io.deephaven.engine.v2.sources.ColumnSource;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.engine.v2.utils.RowSet;

public class DataDrivenReplayer extends Replayer {
    private DBDateTime currentTime;
    private int pos;
    private long lastTime = -1;

    public DataDrivenReplayer(DBDateTime startTime, DBDateTime endTime) {
        super(startTime, endTime);
        currentTime = startTime;
    }

    @Override
    public DBDateTime currentTime() {
        return currentTime;
    }

    TLongArrayList allTimestamp = new TLongArrayList();

    @Override
    public void registerTimeSource(RowSet rowSet, ColumnSource<DBDateTime> timestampSource) {
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
            currentTime = new DBDateTime(currentTimeNanos);
        }
        lastTime = currentTimeNanos;
        super.run();
    }

    @Override
    public void setTime(long updatedTime) {
        currentTime = DBTimeUtils.millisToTime(Math.max(updatedTime, currentTime.getMillis()));
    }
}

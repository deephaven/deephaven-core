/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.replay;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import gnu.trove.list.array.TLongArrayList;

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
    public void registerTimeSource(Index index, ColumnSource<DBDateTime> timestampSource) {
        long prevValue = -1;
        if (timestampSource.allowsReinterpret(long.class)) {
            ColumnSource<Long> longColumn = timestampSource.reinterpret(long.class);
            for (Index.Iterator iterator = index.iterator(); iterator.hasNext();) {
                long currentValue = longColumn.getLong(iterator.nextLong());
                if (currentValue != prevValue) {
                    allTimestamp.add(prevValue = currentValue);
                }
            }
        } else {
            for (Index.Iterator iterator = index.iterator(); iterator.hasNext();) {
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
    public void refresh() {
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
        super.refresh();
    }

    @Override
    public void setTime(long updatedTime) {
        currentTime = DBTimeUtils.millisToTime(Math.max(updatedTime, currentTime.getMillis()));
    }
}

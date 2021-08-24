/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.libs.primitives.LongNumericPrimitives;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.DateTimeArraySource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.TimeProvider;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;

import io.deephaven.internal.log.LoggerFactory;
import java.util.Collections;
import java.util.Map;

/**
 * A TimeTable adds rows at a fixed interval with a single column named "Timestamp".
 *
 * To create a TimeTable, you should use the
 * {@link io.deephaven.db.tables.utils.TableTools#timeTable} family of methods.
 */
public class TimeTable extends QueryTable implements LiveTable {
    private static final Logger log = LoggerFactory.getLogger(TimeTable.class);

    private static final String TIMESTAMP = "Timestamp";
    private long lastIndex = -1;
    private final DateTimeArraySource dateTimeArraySource;
    private TimeProvider timeProvider;
    private DBDateTime lastTime;
    private long dbPeriod;
    private long binOffset;
    private final UpdatePerformanceTracker.Entry entry;

    public TimeTable(TimeProvider timeProvider, long dbPeriod) {
        this(timeProvider, null, dbPeriod);
    }

    public TimeTable(TimeProvider timeProvider, DBDateTime firstTime, long dbPeriod) {
        super(Index.FACTORY.getIndexByValues(), initColumn());
        if (dbPeriod <= 0) {
            throw new IllegalArgumentException("Invalid time period: " + dbPeriod + " nanoseconds");
        }
        this.entry = UpdatePerformanceTracker.getInstance()
            .getEntry("TimeTable(" + firstTime + "," + dbPeriod + ")");
        this.lastTime = firstTime == null ? null : new DBDateTime(firstTime.getNanos() - dbPeriod);
        binOffset = firstTime == null ? 0 : lastTime.getNanos() % dbPeriod;
        dateTimeArraySource = (DateTimeArraySource) getColumnSourceMap().get(TIMESTAMP);
        this.timeProvider = timeProvider;
        this.dbPeriod = dbPeriod;
        setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        setFlat();
        if (firstTime != null) {
            refresh(false);
        }
    }

    private static Map<String, ColumnSource> initColumn() {
        return Collections.singletonMap(TIMESTAMP, new DateTimeArraySource());
    }

    @Override
    public void refresh() {
        refresh(true);
    }

    private void refresh(final boolean notifyListeners) {
        entry.onUpdateStart();
        try {
            final DBDateTime dateTime = timeProvider.currentTime();
            DBDateTime currentBinnedTime = new DBDateTime(
                LongNumericPrimitives.lowerBin(dateTime.getNanos() - binOffset, dbPeriod)
                    + binOffset);
            long rangeStart = lastIndex + 1;
            if (lastTime == null) {
                lastIndex = 0;
                dateTimeArraySource.ensureCapacity(lastIndex + 1);
                dateTimeArraySource.set(lastIndex, lastTime = currentBinnedTime);
                getIndex().insert(lastIndex);
            } else
                while (currentBinnedTime.compareTo(lastTime) > 0) {
                    lastTime = DBTimeUtils.plus(lastTime, dbPeriod);
                    lastIndex++;
                    dateTimeArraySource.ensureCapacity(lastIndex + 1);
                    dateTimeArraySource.set(lastIndex, lastTime);
                }
            if (rangeStart <= lastIndex) {
                // If we have a period longer than 10s, print out that the timetable has been
                // updated. This can be
                // useful when analyzing what's gone wrong in the logs. It is capped at periods of
                // 5s, so we don't
                // end up with too much log spam for short interval time tables. 5s is not so
                // coincidentally the period
                // of the Jvm Heap: messages.
                if (dbPeriod >= 5_000_000_000L) {
                    log.info().append("TimeTable updated to ").append(lastTime.toString()).endl();
                }
                final Index range = Index.FACTORY.getIndexByRange(rangeStart, lastIndex);
                getIndex().insert(range);
                if (notifyListeners) {
                    notifyListeners(range, Index.FACTORY.getEmptyIndex(),
                        Index.FACTORY.getEmptyIndex());
                }
            }
        } finally {
            entry.onUpdateEnd();
        }
    }

    @Override
    protected void destroy() {
        super.destroy();
        LiveTableMonitor.DEFAULT.removeTable(this);
    }
}

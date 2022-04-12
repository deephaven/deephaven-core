/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.TimeProvider;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.logger.Logger;
import io.deephaven.engine.table.Table;
import io.deephaven.function.LongNumericPrimitives;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.DateTimeArraySource;

import io.deephaven.internal.log.LoggerFactory;
import java.util.Collections;
import java.util.Map;

/**
 * A TimeTable adds rows at a fixed interval with a single column named "Timestamp".
 *
 * To create a TimeTable, you should use the {@link TableTools#timeTable} family of methods.
 */
public class TimeTable extends QueryTable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TimeTable.class);

    private static final String TIMESTAMP = "Timestamp";
    private long lastIndex = -1;
    private final DateTimeArraySource dateTimeArraySource;
    private final TimeProvider timeProvider;
    private DateTime lastTime;
    private final long period;
    private final long binOffset;
    private final PerformanceEntry entry;

    public TimeTable(TimeProvider timeProvider, long period) {
        this(timeProvider, null, period);
    }

    public TimeTable(TimeProvider timeProvider, DateTime firstTime, long period) {
        super(RowSetFactory.fromKeys().toTracking(), initColumn());
        if (period <= 0) {
            throw new IllegalArgumentException("Invalid time period: " + period + " nanoseconds");
        }
        this.entry = UpdatePerformanceTracker.getInstance().getEntry("TimeTable(" + firstTime + "," + period + ")");
        this.lastTime = firstTime == null ? null : new DateTime(firstTime.getNanos() - period);
        binOffset = firstTime == null ? 0 : lastTime.getNanos() % period;
        dateTimeArraySource = (DateTimeArraySource) getColumnSourceMap().get(TIMESTAMP);
        this.timeProvider = timeProvider;
        this.period = period;
        setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        setFlat();
        if (firstTime != null) {
            refresh(false);
        }
    }

    private static Map<String, ColumnSource<?>> initColumn() {
        return Collections.singletonMap(TIMESTAMP, new DateTimeArraySource());
    }

    @Override
    public void run() {
        refresh(true);
    }

    private void refresh(final boolean notifyListeners) {
        entry.onUpdateStart();
        try {
            final DateTime dateTime = timeProvider.currentTime();
            DateTime currentBinnedTime = new DateTime(
                    LongNumericPrimitives.lowerBin(dateTime.getNanos() - binOffset, period) + binOffset);
            long rangeStart = lastIndex + 1;
            if (lastTime == null) {
                lastIndex = 0;
                dateTimeArraySource.ensureCapacity(lastIndex + 1);
                dateTimeArraySource.set(lastIndex, lastTime = currentBinnedTime);
                getRowSet().writableCast().insert(lastIndex);
            } else
                while (currentBinnedTime.compareTo(lastTime) > 0) {
                    lastTime = DateTimeUtils.plus(lastTime, period);
                    lastIndex++;
                    dateTimeArraySource.ensureCapacity(lastIndex + 1);
                    dateTimeArraySource.set(lastIndex, lastTime);
                }
            if (rangeStart <= lastIndex) {
                // If we have a period longer than 10s, print out that the timetable has been updated. This can be
                // useful when analyzing what's gone wrong in the logs. It is capped at periods of 5s, so we don't
                // end up with too much log spam for short interval time tables. 5s is not so coincidentally the period
                // of the Jvm Heap: messages.
                if (period >= 5_000_000_000L) {
                    log.info().append("TimeTable updated to ").append(lastTime.toString()).endl();
                }
                final RowSet range = RowSetFactory.fromRange(rangeStart, lastIndex);
                getRowSet().writableCast().insert(range);
                if (notifyListeners) {
                    notifyListeners(range, RowSetFactory.empty(), RowSetFactory.empty());
                }
            }
        } finally {
            entry.onUpdateEnd();
        }
    }

    @Override
    protected void destroy() {
        super.destroy();
        UpdateGraphProcessor.DEFAULT.removeSource(this);
    }
}

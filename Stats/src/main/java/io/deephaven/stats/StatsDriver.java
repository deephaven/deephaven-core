/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.stats;

import io.deephaven.base.clock.Clock;
import io.deephaven.net.CommBase;
import io.deephaven.util.formatters.ISO8601;
import io.deephaven.base.stats.*;
import io.deephaven.base.text.TimestampBuffer;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.log.*;
import io.deephaven.io.sched.TimedJob;
import io.deephaven.io.log.impl.LogBufferPoolImpl;
import io.deephaven.io.log.impl.LogEntryPoolImpl;
import io.deephaven.io.log.impl.LogSinkImpl;

import java.util.Properties;

/**
 * Drives the collection of statistics on a 1-second timer task.
 */
public class StatsDriver extends TimedJob {
    public interface StatusAdapter {
        void sendAlert(String alertText);

        boolean cmsAlertEnabled();

        class Null implements StatusAdapter {
            public void sendAlert(String unused) {}

            public boolean cmsAlertEnabled() {
                return true;
            }
        }

        StatusAdapter NULL = new Null();
    }

    private final LogEntryPool entryPool;
    private final LogSink sink;
    private final LogEntry[] entries;

    private final LogEntryPool entryPoolHisto;
    private final LogSink sinkHisto;
    private final LogEntry[] entriesHisto;

    private final TimestampBuffer systemTimestamp;
    private final TimestampBuffer appTimestamp;

    public final static String header =
        "Stat,IntervalName,NowSec,NowString,AppNowSec,AppNowString,TypeTag,Name,N,Sum,Last,Min,Max,Avg,Sum2,Stdev";

    private long nextInvocation = System.currentTimeMillis();
    private long nextCpuUpdate = nextInvocation + CPU_INTERVAL;
    private long nextMemUpdate = nextInvocation + MEM_INTERVAL;

    private static final long STEP = 1000;
    private static final long MEM_INTERVAL = 1000;
    private static final long CPU_INTERVAL = 1000;
    private static final long REPORT_INTERVAL = 10000;

    private static final int BUFFER_SIZE = 8192;
    private static final int GUESS_ENTRY_SIZE = 256;

    private final Value statsTiming = Stats.makeItem("Stats", "updateDuration", State.FACTORY,
        "Microseconds required to update the statistics histories each second").getValue();

    private final Clock clock;
    private final StatsIntradayLogger intraday;
    private final Value clockValue;

    private final StatsMemoryCollector memStats;
    private final StatsCPUCollector cpuStats;
    private ObjectAllocationCollector objectAllocation;

    public StatsDriver(Clock clock) {
        this(clock, true, StatusAdapter.NULL);
    }

    public StatsDriver(Clock clock, StatusAdapter statusAdapter) {
        this(clock, true, statusAdapter);
    }

    public StatsDriver(Clock clock, boolean getFdStats, StatusAdapter statusAdapter) {
        this(clock, StatsIntradayLogger.NULL, getFdStats, statusAdapter);
    }

    public StatsDriver(Clock clock, boolean getFdStats) {
        this(clock, StatsIntradayLogger.NULL, getFdStats, StatusAdapter.NULL);
    }

    public StatsDriver(Clock clock, StatsIntradayLogger intraday, boolean getFdStats) {
        this(clock, intraday, getFdStats, StatusAdapter.NULL);
    }

    public StatsDriver(Clock clock, StatsIntradayLogger intraday, boolean getFdStats,
        StatusAdapter statusAdapter) {
        Properties props = Configuration.getInstance().getProperties();
        String path = props.getProperty("stats.log.prefix");
        if (path != null) {
            if (path.isEmpty()) {
                path = null;
            } else {
                path += ".stats";
            }
        }

        String histoPath = props.getProperty("histo.log.prefix");
        if (histoPath != null) {
            if (histoPath.isEmpty()) {
                histoPath = null;
            } else {
                histoPath += ".histo";
            }
        }

        this.systemTimestamp = new TimestampBuffer(ISO8601.serverTimeZone());
        this.appTimestamp = new TimestampBuffer(ISO8601.serverTimeZone());

        if (path == null) {
            this.entryPool = null;
            this.sink = null;
            this.entries = null;
        } else {
            LogBufferPool bufferPool =
                new LogBufferPoolImpl(History.INTERVALS.length * 20, BUFFER_SIZE);
            this.entryPool = new LogEntryPoolImpl(History.INTERVALS.length * 20, bufferPool);
            this.sink = new LogSinkImpl<>(path, 3600 * 1000, entryPool, true);
            this.entries = new LogEntry[History.INTERVALS.length];
        }

        if (histoPath == null) {
            this.entryPoolHisto = null;
            this.sinkHisto = null;
            this.entriesHisto = null;
        } else {
            LogBufferPool bufferPool =
                new LogBufferPoolImpl(History.INTERVALS.length * 20, BUFFER_SIZE);
            this.entryPoolHisto = new LogEntryPoolImpl(History.INTERVALS.length * 20, bufferPool);
            this.sinkHisto = new LogSinkImpl<>(histoPath, 3600 * 1000, entryPoolHisto, true);
            this.entriesHisto = new LogEntry[History.INTERVALS.length];
        }

        this.clock = clock;
        this.intraday = intraday;
        if (this.clock != null) {
            clockValue = Stats.makeItem("Clock", "value", State.FACTORY,
                "The value of the Clock, useful for mapping data from simulation runs").getValue();
        } else {
            clockValue = null;
        }

        long now = System.currentTimeMillis();
        long delay = STEP - (now % STEP);
        nextInvocation = now + delay;
        cpuStats = new StatsCPUCollector(CPU_INTERVAL, getFdStats);
        memStats = new StatsMemoryCollector(MEM_INTERVAL, statusAdapter::sendAlert,
            statusAdapter::cmsAlertEnabled);
        if (Configuration.getInstance().getBoolean("allocation.stats.enabled")) {
            objectAllocation = new ObjectAllocationCollector();
        }
        if (Configuration.getInstance().getBoolean("statsdriver.enabled")) {
            schedule();
        }
    }

    public void timedOut() {
        long t0 = System.nanoTime();
        long now = System.currentTimeMillis();
        long appNow = clock == null ? now : clock.currentTimeMillis();

        if (now >= nextCpuUpdate) {
            nextCpuUpdate += CPU_INTERVAL;
            cpuStats.update();
        }

        if (now >= nextMemUpdate) {
            nextMemUpdate += MEM_INTERVAL;
            memStats.update();
        }

        if (clock != null) {
            clockValue.sample(clock.currentTimeMillis());
        }

        if (this.entries == null) {
            Stats.update(LISTENER, now, appNow, REPORT_INTERVAL);
        } else {
            for (int i = 0; i < History.INTERVALS.length; ++i) {
                entries[i] = entryPool.take().start(sink, LogLevel.INFO, now * 1000);
                if (entriesHisto != null) {
                    entriesHisto[i] =
                        entryPoolHisto.take().start(sinkHisto, LogLevel.INFO, now * 1000);
                }
            }
            Stats.update(LISTENER, now, appNow, REPORT_INTERVAL);
            for (int i = 0; i < History.INTERVALS.length; ++i) {
                entries[i].end();
                if (entriesHisto != null) {
                    entriesHisto[i].end();
                }
            }
        }

        schedule();

        statsTiming.sample((System.nanoTime() - t0 + 500) / 1000);
    }

    private void schedule() {
        CommBase.getScheduler().installJob(this, nextInvocation);
        long steps = Math.max(1L, (((System.currentTimeMillis() - nextInvocation) / STEP) + 1));
        nextInvocation += steps * STEP;
    }

    private final ItemUpdateListener LISTENER = new ItemUpdateListener() {
        @Override
        public void handleItemUpdated(Item item, long now, long appNow, int intervalIndex,
            long intervalMillis, String intervalName) {
            final Value v = item.getValue();
            final History history = v.getHistory();
            final char typeTag = v.getTypeTag();
            LogEntry e;
            switch (typeTag) { // it's either a HistogramPower2 or a Value
                case 'N': {
                    if (entriesHisto == null && intraday == StatsIntradayLogger.NULL) {
                        return;
                    }
                    final long n = history.getN(intervalIndex, 1);
                    final long sum = history.getSum(intervalIndex, 1);
                    final long last = history.getLast(intervalIndex, 1);
                    final long min = history.getMin(intervalIndex, 1);
                    final long max = history.getMax(intervalIndex, 1);
                    final long avg = history.getAvg(intervalIndex, 1);
                    final long sum2 = history.getSum2(intervalIndex, 1);
                    final long stdev = history.getStdev(intervalIndex, 1);
                    final HistogramPower2 nh = (HistogramPower2) v;
                    if (entriesHisto != null) {
                        e = entriesHisto[intervalIndex];
                        if (e.size() > BUFFER_SIZE - GUESS_ENTRY_SIZE) {
                            e.end();
                            e = entriesHisto[intervalIndex] =
                                entryPoolHisto.take().start(sinkHisto, LogLevel.INFO, now * 1000);
                        }
                        e.append("HISTOGRAM")
                            .append(',').append(intervalName)
                            .append(',').append(now / 1000)
                            .append(',').appendTimestamp(now, systemTimestamp)
                            .append(',').append(appNow / 1000)
                            .append(',').appendTimestamp(appNow, appTimestamp)
                            .append(',').append(v.getTypeTag())
                            .append(',').append(item.getGroupName())
                            .append('.').append(item.getName())
                            .append(',').append(n)
                            .append(',').append(sum)
                            .append(',').append(last)
                            .append(',').append(min)
                            .append(',').append(max)
                            .append(',').append(avg)
                            .append(',').append(sum2)
                            .append(',').append(stdev)
                            .append(',').append(nh.getHistogramString())
                            .nl();
                    }
                    intraday.log(
                        intervalName,
                        now,
                        appNow,
                        v.getTypeTag(),
                        item.getCompactName(),
                        n,
                        sum,
                        last,
                        min,
                        max,
                        avg,
                        sum2,
                        stdev,
                        nh.getHistogram());
                    break;
                }
                default: {
                    if (entries == null && intraday == StatsIntradayLogger.NULL) {
                        return;
                    }
                    final long n = history.getN(intervalIndex, 1);
                    final long sum = history.getSum(intervalIndex, 1);
                    final long last = history.getLast(intervalIndex, 1);
                    final long min = history.getMin(intervalIndex, 1);
                    final long max = history.getMax(intervalIndex, 1);
                    final long avg = history.getAvg(intervalIndex, 1);
                    final long sum2 = history.getSum2(intervalIndex, 1);
                    final long stdev = history.getStdev(intervalIndex, 1);
                    if (entries != null) {
                        e = entries[intervalIndex];
                        if (e.size() > BUFFER_SIZE - GUESS_ENTRY_SIZE) {
                            e.end();
                            e = entries[intervalIndex] =
                                entryPool.take().start(sink, LogLevel.INFO, now * 1000);
                        }

                        e.append("STAT")
                            .append(',').append(intervalName)
                            .append(',').append(now / 1000)
                            .append(',').appendTimestamp(now, systemTimestamp)
                            .append(',').append(appNow / 1000)
                            .append(',').appendTimestamp(appNow, appTimestamp)
                            .append(',').append(v.getTypeTag())
                            .append(',').append(item.getGroupName())
                            .append('.').append(item.getName())
                            .append(',').append(n)
                            .append(',').append(sum)
                            .append(',').append(last)
                            .append(',').append(min)
                            .append(',').append(max)
                            .append(',').append(avg)
                            .append(',').append(sum2)
                            .append(',').append(stdev)
                            .nl();
                    }
                    intraday.log(
                        intervalName,
                        now,
                        appNow,
                        v.getTypeTag(),
                        item.getCompactName(),
                        n,
                        sum,
                        last,
                        min,
                        max,
                        avg,
                        sum2,
                        stdev);
                    break;
                }
            }
        }
    };
}

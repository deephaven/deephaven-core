package io.deephaven.engine.table.impl.perf;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.impl.util.RuntimeMemory;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

/**
 * Entry class for tracking the performance characteristics of a single recurring update event.
 */
public class PerformanceEntry extends BasePerformanceEntry implements TableListener.Entry {
    private final int id;
    private final int evaluationNumber;
    private final int operationNumber;
    private final String description;
    private final String callerLine;

    private long intervalInvocationCount;

    private long intervalAdded;
    private long intervalRemoved;
    private long intervalModified;
    private long intervalShifted;

    private long maxTotalMemory;
    private long minFreeMemory;
    private long collections;
    private long collectionTimeMs;

    private final RuntimeMemory.Sample startSample;
    private final RuntimeMemory.Sample endSample;

    PerformanceEntry(final int id, final int evaluationNumber, final int operationNumber,
            final String description, final String callerLine) {
        this.id = id;
        this.evaluationNumber = evaluationNumber;
        this.operationNumber = operationNumber;
        this.description = description;
        this.callerLine = callerLine;
        startSample = new RuntimeMemory.Sample();
        endSample = new RuntimeMemory.Sample();
        maxTotalMemory = 0;
        minFreeMemory = Long.MAX_VALUE;
        collections = 0;
        collectionTimeMs = 0;
    }

    public final void onUpdateStart() {
        ++intervalInvocationCount;
        RuntimeMemory.getInstance().read(startSample);
        super.onBaseEntryStart();
    }

    public final void onUpdateStart(final RowSet added, final RowSet removed, final RowSet modified,
            final RowSetShiftData shifted) {
        intervalAdded += added.size();
        intervalRemoved += removed.size();
        intervalModified += modified.size();
        intervalShifted += shifted.getEffectiveSize();

        onUpdateStart();
    }

    public final void onUpdateStart(long added, long removed, long modified, long shifted) {
        intervalAdded += added;
        intervalRemoved += removed;
        intervalModified += modified;
        intervalShifted += shifted;

        onUpdateStart();
    }

    public final void onUpdateEnd() {
        onBaseEntryEnd();
        RuntimeMemory.getInstance().read(endSample);
        maxTotalMemory = Math.max(maxTotalMemory, Math.max(startSample.totalMemory, endSample.totalMemory));
        minFreeMemory = Math.min(minFreeMemory, Math.min(startSample.freeMemory, endSample.freeMemory));
        collections += endSample.totalCollections - startSample.totalCollections;
        collectionTimeMs += endSample.totalCollectionTimeMs - startSample.totalCollectionTimeMs;
    }

    void reset() {
        baseEntryReset();
        intervalInvocationCount = 0;

        intervalAdded = 0;
        intervalRemoved = 0;
        intervalModified = 0;
        intervalShifted = 0;

        maxTotalMemory = 0;
        minFreeMemory = Long.MAX_VALUE;
        collections = 0;
        collectionTimeMs = 0;
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        final LogOutput beginning = logOutput.append("PerformanceEntry{")
                .append(", id=").append(id)
                .append(", evaluationNumber=").append(evaluationNumber)
                .append(", operationNumber=").append(operationNumber)
                .append(", description='").append(description).append('\'')
                .append(", callerLine='").append(callerLine).append('\'')
                .append(", intervalUsageNanos=").append(getIntervalUsageNanos())
                .append(", intervalCpuNanos=").append(getIntervalCpuNanos())
                .append(", intervalUserCpuNanos=").append(getIntervalUserCpuNanos())
                .append(", intervalInvocationCount=").append(intervalInvocationCount)
                .append(", intervalAdded=").append(intervalAdded)
                .append(", intervalRemoved=").append(intervalRemoved)
                .append(", intervalModified=").append(intervalModified)
                .append(", intervalShifted=").append(intervalShifted)
                .append(", intervalAllocatedBytes=").append(getIntervalAllocatedBytes())
                .append(", intervalPoolAllocatedBytes=").append(getIntervalPoolAllocatedBytes())
                .append(", maxTotalMemory=").append(maxTotalMemory)
                .append(", minFreeMemory=").append(minFreeMemory)
                .append(", collections=").append(collections)
                .append(", collectionTimeNanos=").append(DateTimeUtils.millisToNanos(collectionTimeMs));
        return appendStart(beginning)
                .append('}');
    }

    public int getId() {
        return id;
    }

    public int getEvaluationNumber() {
        return evaluationNumber;
    }

    public int getOperationNumber() {
        return operationNumber;
    }

    public String getDescription() {
        return description;
    }

    public String getCallerLine() {
        return callerLine;
    }

    public long getIntervalAdded() {
        return intervalAdded;
    }

    public long getIntervalRemoved() {
        return intervalRemoved;
    }

    public long getIntervalModified() {
        return intervalModified;
    }

    public long getIntervalShifted() {
        return intervalShifted;
    }

    public long getMinFreeMemory() {
        return (minFreeMemory == Long.MAX_VALUE) ? QueryConstants.NULL_LONG : minFreeMemory;
    }

    public long getMaxTotalMemory() {
        return (maxTotalMemory == 0) ? QueryConstants.NULL_LONG : maxTotalMemory;
    }

    public long getCollections() {
        return collections;
    }

    public long getCollectionTimeNanos() {
        return DateTimeUtils.millisToNanos(collectionTimeMs);
    }

    public long getIntervalInvocationCount() {
        return intervalInvocationCount;
    }

    /**
     * Suppress de minimus update entry intervals using the properties defined in the QueryPerformanceNugget class.
     *
     * @return if this nugget is significant enough to be logged, otherwise it is aggregated into the small update entry
     */
    boolean shouldLogEntryInterval() {
        return intervalInvocationCount > 0 &&
                UpdatePerformanceTracker.LOG_THRESHOLD.shouldLog(getIntervalUsageNanos());
    }

    public void accumulate(PerformanceEntry entry) {
        if (entry.getMaxTotalMemory() > getMaxTotalMemory()) {
            maxTotalMemory = entry.maxTotalMemory;
        }
        if (entry.getMinFreeMemory() < getMinFreeMemory()) {
            minFreeMemory = entry.getMinFreeMemory();
        }

        collections += entry.getCollections();
        collectionTimeMs += entry.collectionTimeMs;
        intervalInvocationCount += entry.getIntervalInvocationCount();

        intervalAdded += entry.getIntervalAdded();
        intervalRemoved += entry.getIntervalRemoved();
        intervalModified += entry.getIntervalModified();
        intervalShifted += entry.getIntervalShifted();

        super.accumulate(entry);
    }
}

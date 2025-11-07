//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.perf;

import io.deephaven.auth.AuthContext;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.impl.util.RuntimeMemory;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * Entry class for tracking the performance characteristics of a single recurring update event.
 */
public class PerformanceEntry extends BasePerformanceEntry implements TableListener.Entry {
    /**
     * If your system requires authentication contexts for performance logging, then set this property to true.
     * Otherwise, errors can be delayed until the entry is logged; which makes them much harder to track down. The
     * Community Core product does not require authentication contexts, so this defaults to false.
     */
    private final static boolean REQUIRE_AUTH_CONTEXT =
            Configuration.getInstance().getBooleanWithDefault("PerformanceEntry.requireAuthContext", false);

    private final long id;
    private final long evaluationNumber;
    private final int operationNumber;
    private final String description;
    private final String callerLine;

    private final AuthContext authContext;
    private final String updateGraphName;

    private long invocationCount;

    private long rowsAdded;
    private long rowsRemoved;
    private long rowsModified;
    private long rowsShifted;

    private long maxTotalMemory;
    private long minFreeMemory;
    private long collections;
    private long collectionTimeMs;

    private boolean loggedOnce;
    private final RuntimeMemory.Sample startSample;
    private final RuntimeMemory.Sample endSample;

    PerformanceEntry(final long id, final long evaluationNumber, final int operationNumber,
            final String description, final String callerLine, final String updateGraphName) {
        this.id = id;
        this.evaluationNumber = evaluationNumber;
        this.operationNumber = operationNumber;
        this.description = description;
        this.callerLine = callerLine;
        authContext = id == QueryConstants.NULL_LONG ? null : getContext();
        this.updateGraphName = updateGraphName;
        startSample = new RuntimeMemory.Sample();
        endSample = new RuntimeMemory.Sample();
        maxTotalMemory = 0;
        minFreeMemory = Long.MAX_VALUE;
        collections = 0;
        collectionTimeMs = 0;
        loggedOnce = false;
    }

    private static AuthContext getContext() {
        final AuthContext currentAuthContext = ExecutionContext.getContext().getAuthContext();
        return REQUIRE_AUTH_CONTEXT ? Require.neqNull(currentAuthContext, "authContext") : currentAuthContext;
    }

    public final void onUpdateStart() {
        RuntimeMemory.getInstance().read(startSample);
        super.onBaseEntryStart();
    }

    public final void onUpdateStart(final RowSet added, final RowSet removed, final RowSet modified,
            final RowSetShiftData shifted) {
        rowsAdded += added.size();
        rowsRemoved += removed.size();
        rowsModified += modified.size();
        rowsShifted += shifted.getEffectiveSize();

        onUpdateStart();
    }

    public final void onUpdateStart(long added, long removed, long modified, long shifted) {
        rowsAdded += added;
        rowsRemoved += removed;
        rowsModified += modified;
        rowsShifted += shifted;

        onUpdateStart();
    }

    public final void onUpdateEnd() {
        onBaseEntryEnd();
        RuntimeMemory.getInstance().read(endSample);
        maxTotalMemory = Math.max(maxTotalMemory, Math.max(startSample.totalMemory, endSample.totalMemory));
        minFreeMemory = Math.min(minFreeMemory, Math.min(startSample.freeMemory, endSample.freeMemory));
        collections += endSample.totalCollections - startSample.totalCollections;
        collectionTimeMs += endSample.totalCollectionTimeMs - startSample.totalCollectionTimeMs;
        ++invocationCount;
    }

    void reset() {
        baseEntryReset();
        invocationCount = 0;

        rowsAdded = 0;
        rowsRemoved = 0;
        rowsModified = 0;
        rowsShifted = 0;

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
    public LogOutput append(@NotNull final LogOutput logOutput) {
        final LogOutput beginning = logOutput.append("PerformanceEntry{")
                .append(", id=").append(id)
                .append(", evaluationNumber=").append(evaluationNumber)
                .append(", operationNumber=").append(operationNumber)
                .append(", description='").append(description).append('\'')
                .append(", callerLine='").append(callerLine).append('\'')
                .append(", authContext=").append(authContext)
                .append(", usageNanos=").append(getUsageNanos())
                .append(", cpuNanos=").append(getCpuNanos())
                .append(", userCpuNanos=").append(getUserCpuNanos())
                .append(", invocationCount=").append(invocationCount)
                .append(", rowsAdded=").append(rowsAdded)
                .append(", rowsRemoved=").append(rowsRemoved)
                .append(", rowsModified=").append(rowsModified)
                .append(", rowsShifted=").append(rowsShifted)
                .append(", allocatedBytes=").append(getAllocatedBytes())
                .append(", poolAllocatedBytes=").append(getPoolAllocatedBytes())
                .append(", maxTotalMemory=").append(maxTotalMemory)
                .append(", minFreeMemory=").append(minFreeMemory)
                .append(", collections=").append(collections)
                .append(", collectionTimeNanos=").append(DateTimeUtils.millisToNanos(collectionTimeMs));
        return appendStart(beginning)
                .append('}');
    }

    public long getId() {
        return id;
    }

    public long getEvaluationNumber() {
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

    /**
     * @return The {@link AuthContext} that was installed when this PerformanceEntry was constructed
     */
    public AuthContext getAuthContext() {
        return authContext;
    }

    /**
     * @return The name of the update graph that this PerformanceEntry is associated with
     */
    public String getUpdateGraphName() {
        return updateGraphName;
    }

    public long getRowsAdded() {
        return rowsAdded;
    }

    public long getRowsRemoved() {
        return rowsRemoved;
    }

    public long getRowsModified() {
        return rowsModified;
    }

    public long getRowsShifted() {
        return rowsShifted;
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

    public long getInvocationCount() {
        return invocationCount;
    }

    /**
     * Suppress de minimus update entry intervals using the properties defined in the QueryPerformanceNugget class.
     *
     * @return if this nugget is significant enough to be logged, otherwise it is aggregated into the small update entry
     */
    boolean shouldLogEntryInterval() {
        if (UpdatePerformanceTracker.LOG_ALL_ENTRIES_ONCE && !loggedOnce) {
            loggedOnce = true;
            return true;
        }
        return invocationCount > 0 && UpdatePerformanceTracker.LOG_THRESHOLD.shouldLog(getUsageNanos());
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
        invocationCount += entry.getInvocationCount();

        rowsAdded += entry.getRowsAdded();
        rowsRemoved += entry.getRowsRemoved();
        rowsModified += entry.getRowsModified();
        rowsShifted += entry.getRowsShifted();

        super.accumulate(entry);
    }
}

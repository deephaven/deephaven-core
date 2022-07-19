package io.deephaven.demo.control;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * DemoStats:
 * <p>
 * <p>Records statistics for a running demo cluster.
 */
public class DemoStats {

    private class UsageSlice {
        private final long timeStart, timeUsed;
        private final int numLeased;

        private UsageSlice(final long timeStart, final long timeUsed, final int numLeased) {
            this.timeStart = timeStart;
            this.timeUsed = timeUsed;
            this.numLeased = numLeased;
        }
    }
    private final Map<String, Integer> errCnts;
    private final Map<String, Integer> visitCnts;

    private final List<UsageSlice> usages;

    private final AtomicInteger leaseCnt;

    public DemoStats() {
        errCnts = new ConcurrentHashMap<>();
        visitCnts = new ConcurrentHashMap<>();
        leaseCnt = new AtomicInteger();
        // we'll synchronize on usages, since only one thread at a time should be reporting this.
        usages = new ArrayList<>();
    }

    public void recordUsedLeases(int usage) {
        synchronized (usages) {
            long curTime = System.currentTimeMillis();
            if (usages.isEmpty()) {
                UsageSlice slice = new UsageSlice(curTime - 100, 100, usage);
                usages.add(slice);
            } else {
                final UsageSlice prev = usages.get(usages.size() - 1);
                final long start = prev.timeStart + prev.timeUsed;
                UsageSlice slice = new UsageSlice(start, System.currentTimeMillis() - start, usage);
                usages.add(slice);
            }
        }
    }

    /**
     * Record an error message and return true if this is the first time we've seen the error.
     * @param msg The message to log
     * @return true if this is the first time this process has seen this error.
     */
    public boolean recordError(String msg) {
        return 1 == errCnts.merge(msg, 1, Integer::sum);
    }

    /**
     * Record a visitor identity and return true if this is the first time we've seen this user agent.
     * @param msg A http-request-based user id that has visited us
     * @return true if this is the first time this user id has been seen
     */
    public boolean recordVisit(String msg) {
        return 1 == visitCnts.merge(msg, 1, Integer::sum);
    }

    public long getStartTime() {
        return usages.isEmpty() ? System.currentTimeMillis() : usages.get(0).timeStart;
    }

    public Collection<String> getUniqueUsers() {
        return visitCnts.entrySet().stream()
                .map(e->e.getKey() + " (" + e.getValue() + " time" +
                        (e.getValue() == 1 ? "" : "s") + ")")
                .collect(Collectors.toList());
    }

    public Collection<String> getErrorUsage() {
        return errCnts.entrySet().stream()
                .map(e->e.getKey() + " (" + e.getValue() + " time" +
                        (e.getValue() == 1 ? "" : "s") + ")")
                .collect(Collectors.toList());
    }

    public int getPeakUsage() {
        if (usages.isEmpty()) {
            return 0;
        }
        return usages.stream().reduce(usages.get(0), (b,a) -> b.numLeased > a.numLeased ? b : a).numLeased;
    }

    public float getAverageUsage() {
        if (usages.isEmpty()) {
            return 0;
        }
        final UsageSlice first = usages.get(0);
        final UsageSlice last = usages.get(usages.size() - 1);
        long start = first.timeStart, end = last.timeStart + last.timeUsed;
        double totalRuntime = end - start;
        return usages.stream()
                .map(slice -> slice.numLeased * slice.timeUsed / totalRuntime)
                .reduce(0., Double::sum).floatValue();
    }

    public int getPeakMachines() {
        return 0;
    }

    public int getAverageMachines() {
        return 0;
    }

}

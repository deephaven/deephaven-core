package io.deephaven.web.shared.data;

import java.io.Serializable;

/**
 * A DTO containing the result data from worker heap utilization
 */
public class WorkerHeapInfo implements Serializable {
    private long maximumHeapSize;
    private long totalHeapSize;
    private long freeMemory;

    public long getMaximumHeapSize() {
        return maximumHeapSize;
    }

    public long getTotalHeapSize() {
        return totalHeapSize;
    }

    public long getFreeMemory() {
        return freeMemory;
    }

    public void setMaximumHeapSize(long maximumHeapSize) {
        this.maximumHeapSize = maximumHeapSize;
    }

    public void setTotalHeapSize(long totalHeapSize) {
        this.totalHeapSize = totalHeapSize;
    }

    public void setFreeMemory(long freeMemory) {
        this.freeMemory = freeMemory;
    }

    @Override
    public String toString() {
        return "WorkerHeapInfo{" +
                "maximumHeapSize=" + maximumHeapSize +
                ", totalHeapSize=" + totalHeapSize +
                ", freeMemory=" + freeMemory +
                '}';
    }
}
package io.deephaven.web.client.api;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.HeapInfo;
import io.deephaven.web.shared.data.WorkerHeapInfo;
import jsinterop.annotations.JsProperty;

public class JsWorkerHeapInfo {
    private long maximumHeapSize;
    private long freeMemory;
    private long totalHeapSize;

    public JsWorkerHeapInfo(HeapInfo heapInfo){
        this.maximumHeapSize = Long.parseLong(heapInfo.getMaximumHeapSize());
        this.freeMemory = Long.parseLong(heapInfo.getFreeMemory());
        this.totalHeapSize = Long.parseLong(heapInfo.getTotalHeapSize());
    }

    @JsProperty
    public long getMaximumHeapSize() {
        return maximumHeapSize;
    }

    @JsProperty
    public long getFreeMemory() {
        return freeMemory;
    }

    @JsProperty
    public long getTotalHeapSize() {
        return totalHeapSize;
    }
}
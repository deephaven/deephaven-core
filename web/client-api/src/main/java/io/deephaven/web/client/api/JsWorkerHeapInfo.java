package io.deephaven.web.client.api;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.GetHeapInfoResponse;
import jsinterop.annotations.JsProperty;

public class JsWorkerHeapInfo {
    private long maximumHeapSize;
    private long freeMemory;
    private long totalHeapSize;

    public JsWorkerHeapInfo(GetHeapInfoResponse heapInfo) {
        this.maximumHeapSize = Long.parseLong(heapInfo.getMaxMemory());
        this.freeMemory = Long.parseLong(heapInfo.getFreeMemory());
        this.totalHeapSize = Long.parseLong(heapInfo.getTotalMemory());
    }

    @JsProperty
    public double getMaximumHeapSize() {
        return maximumHeapSize;
    }

    @JsProperty
    public double getFreeMemory() {
        return freeMemory;
    }

    @JsProperty
    public double getTotalHeapSize() {
        return totalHeapSize;
    }
}

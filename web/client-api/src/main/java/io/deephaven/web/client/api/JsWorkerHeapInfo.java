package io.deephaven.web.client.api;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.HeapInfo;
import io.deephaven.web.shared.data.WorkerHeapInfo;
import jsinterop.annotations.JsProperty;

public class JsWorkerHeapInfo {
    private HeapInfo heapInfo;

    public JsWorkerHeapInfo(HeapInfo heapInfo){
        this.heapInfo = heapInfo;
    }

    @JsProperty
    public long getMaximumHeapSize() {
        return heapInfo.getMaximumHeapSize();
    }

    @JsProperty
    public long getFreeMemory() {
        return heapInfo.getFreeMemory();
    }

    @JsProperty
    public long getTotalHeapSize() {
        return heapInfo.getTotalHeapSize();
    }
}
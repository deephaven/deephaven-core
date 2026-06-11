//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.proto.backplane.script.grpc.GetHeapInfoResponse;
import jsinterop.annotations.JsProperty;

@TsInterface
@TsName(name = "WorkerHeapInfo", namespace = "dh")
public class JsWorkerHeapInfo {
    private final long maximumHeapSize;
    private final long freeMemory;
    private final long totalHeapSize;

    public JsWorkerHeapInfo(GetHeapInfoResponse heapInfo) {
        this.maximumHeapSize = heapInfo.getMaxMemory();
        this.freeMemory = heapInfo.getFreeMemory();
        this.totalHeapSize = heapInfo.getTotalMemory();
    }

    @JsProperty
    public double getMaximumHeapSize() {
        return maximumHeapSize;
    }

    @JsProperty
    public double getFreeMemory() {
        return freeMemory;
    }

    /**
     * Total heap size available for this worker.
     */
    @JsProperty
    public double getTotalHeapSize() {
        return totalHeapSize;
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.GetHeapInfoResponse;
import jsinterop.annotations.JsProperty;

/**
 * Heap memory information for a Deephaven worker.
 */
@TsInterface
@TsName(name = "WorkerHeapInfo", namespace = "dh")
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

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.calendar;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import jsinterop.annotations.JsProperty;

@TsInterface
@TsName(namespace = "dh.calendar", name = "BusinessPeriod")
public class JsBusinessPeriod {
    private final FigureDescriptor.BusinessCalendarDescriptor.BusinessPeriod businessPeriod;

    public JsBusinessPeriod(FigureDescriptor.BusinessCalendarDescriptor.BusinessPeriod businessPeriod) {
        this.businessPeriod = businessPeriod;
    }

    @JsProperty
    public String getOpen() {
        return businessPeriod.getOpen();
    }

    @JsProperty
    public String getClose() {
        return businessPeriod.getClose();
    }
}

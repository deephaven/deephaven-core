//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.calendar;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.businesscalendardescriptor.BusinessPeriod;
import jsinterop.annotations.JsProperty;

@TsInterface
@TsName(namespace = "dh.calendar", name = "BusinessPeriod")
public class JsBusinessPeriod {
    private final BusinessPeriod businessPeriod;

    public JsBusinessPeriod(BusinessPeriod businessPeriod) {
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

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.calendar;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import jsinterop.annotations.JsProperty;

/**
 * A business period within a {@code dh.calendar.BusinessCalendar}.
 * <p>
 * A business period describes the open and close times for a single contiguous range of time on a business day.
 */
@TsInterface
@TsName(namespace = "dh.calendar", name = "BusinessPeriod")
public class JsBusinessPeriod {
    private final FigureDescriptor.BusinessCalendarDescriptor.BusinessPeriod businessPeriod;

    public JsBusinessPeriod(FigureDescriptor.BusinessCalendarDescriptor.BusinessPeriod businessPeriod) {
        this.businessPeriod = businessPeriod;
    }

    /**
     * The open time for this business period.
     *
     * @return The open time
     */
    @JsProperty
    public String getOpen() {
        return businessPeriod.getOpen();
    }

    /**
     * The close time for this business period.
     *
     * @return The close time
     */
    @JsProperty
    public String getClose() {
        return businessPeriod.getClose();
    }
}

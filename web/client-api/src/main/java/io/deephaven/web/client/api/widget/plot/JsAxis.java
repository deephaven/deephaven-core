//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.AxisDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.BusinessCalendarDescriptor;
import io.deephaven.web.client.api.i18n.JsDateTimeFormat;
import io.deephaven.web.client.api.widget.calendar.JsBusinessCalendar;
import io.deephaven.web.client.api.widget.plot.enums.JsAxisFormatType;
import io.deephaven.web.client.api.widget.plot.enums.JsAxisPosition;
import io.deephaven.web.client.api.widget.plot.enums.JsAxisType;
import io.deephaven.web.client.fu.JsLog;
import jsinterop.annotations.*;
import jsinterop.base.Js;

/**
 * Defines one axis used with by series. These instances will be found both on the Chart and the Series instances, and
 * may be shared between Series instances.
 */
@TsInterface
@TsName(namespace = "dh.plot", name = "Axis")
public class JsAxis {
    private final AxisDescriptor axis;
    private final JsFigure jsFigure;
    private final JsBusinessCalendar businessCalendar;

    private Integer pixels;
    private Long min;
    private Long max;

    public JsAxis(AxisDescriptor descriptor, JsFigure jsFigure) {
        this.axis = descriptor;
        this.jsFigure = jsFigure;

        final BusinessCalendarDescriptor businessCalendarDescriptor = descriptor.getBusinessCalendarDescriptor();
        if (businessCalendarDescriptor != null) {
            businessCalendar = new JsBusinessCalendar(businessCalendarDescriptor);
        } else {
            businessCalendar = null;
        }
    }

    /**
     * The calendar with the business hours and holidays to transform plot data against. Defaults to null, or no
     * transform.
     * 
     * @return dh.calendar.BusinessCalendar
     *
     */
    @JsProperty
    public JsBusinessCalendar getBusinessCalendar() {
        return businessCalendar;
    }

    /**
     * The unique id for this axis.
     * 
     * @return String
     */
    @JsProperty
    public String getId() {
        return axis.getId();
    }

    /**
     * The type for this axis. See <b>AxisFormatType</b> enum for more details.
     * 
     * @return int
     */
    @JsProperty
    @TsTypeRef(JsAxisFormatType.class)
    public int getFormatType() {
        return axis.getFormatType();
    }

    /**
     * The type for this axis, indicating how it will be drawn. See <b>AxisType</b> enum for more details.
     * 
     * @return int
     */
    @JsProperty
    @TsTypeRef(JsAxisType.class)
    public int getType() {
        return axis.getType();
    }

    /**
     * The position for this axis. See <b>AxisPosition</b> enum for more details.
     * 
     * @return int
     */
    @JsProperty
    @TsTypeRef(JsAxisPosition.class)
    public int getPosition() {
        return axis.getPosition();
    }

    @JsProperty
    public boolean isLog() {
        return axis.getLog();
    }

    /**
     * The label for this axis.
     * 
     * @return String
     */
    @JsProperty
    public String getLabel() {
        return axis.getLabel();
    }

    @JsProperty
    public String getLabelFont() {
        return axis.getLabelFont();
    }

    @JsProperty
    public String getTicksFont() {
        return axis.getTicksFont();
    }

    // TODO (deephaven-core#774) finish this field or remove it from the DSL
    // @JsProperty
    // public String getFormat() {
    // return axis.getFormat();
    // }

    /**
     * The format pattern to use with this axis. Use the type to determine which type of formatter to use.
     * 
     * @return String
     */
    @JsProperty
    @JsNullable
    public String getFormatPattern() {
        if (axis.hasFormatPattern()) {
            return axis.getFormatPattern();
        }
        return null;
    }

    @JsProperty
    public String getColor() {
        return axis.getColor();
    }

    @JsProperty
    public double getMinRange() {
        return axis.getMinRange();
    }

    @JsProperty
    public double getMaxRange() {
        return axis.getMaxRange();
    }

    @JsProperty
    public boolean isMinorTicksVisible() {
        return axis.getMinorTicksVisible();
    }

    @JsProperty
    public boolean isMajorTicksVisible() {
        return axis.getMajorTicksVisible();
    }

    @JsProperty
    public int getMinorTickCount() {
        return axis.getMinorTickCount();
    }

    @JsProperty
    @JsNullable
    public Double getGapBetweenMajorTicks() {
        if (axis.hasGapBetweenMajorTicks()) {
            return axis.getGapBetweenMajorTicks();
        }
        return null;
    }

    @JsProperty
    public double[] getMajorTickLocations() {
        return Js.uncheckedCast(axis.getMajorTickLocationsList().slice());
    }

    // TODO (deephaven-core#774) finish this field or remove it from the DSL
    // @JsProperty
    // public String getAxisTransform() {
    // return axis.getAxisTransform();
    // }

    @JsProperty
    public double getTickLabelAngle() {
        return axis.getTickLabelAngle();
    }

    @JsProperty
    public boolean isInvert() {
        return axis.getInvert();
    }

    @JsProperty
    public boolean isTimeAxis() {
        return axis.getIsTimeAxis();
    }

    /**
     * Indicates that this axis is only `widthInPixels` wide, so any extra data can be downsampled out, if this can be
     * done losslessly. The second two arguments represent the current zoom range of this axis, and if provided, most of
     * the data outside of this range will be filtered out automatically and the visible width mapped to that range.
     * When the UI zooms, pans, or resizes, this method should be called again to update these three values to ensure
     * that data is correct and current.
     *
     * @param pixelCount
     * @param min
     * @param max
     */
    @JsMethod
    public void range(@JsOptional @JsNullable Double pixelCount, @JsOptional @JsNullable Object min,
            @JsOptional @JsNullable Object max) {
        if (pixelCount == null || !Js.typeof(Js.asAny(pixelCount)).equals("number")) {
            if (this.pixels != null) {
                JsLog.warn("Turning off downsampling on a chart where it is running is not currently supported");
                return;
            }
            JsLog.warn("Ignoring Axis.range() call with non-numeric pixel count");
            return;
        }
        if (pixelCount < 5) {
            JsLog.warn("Ignoring unreasonably small pixel count: ", pixelCount);
            return;
        }
        pixels = (int) (double) pixelCount;

        if (min != null || max != null) {
            if (min == null || max == null) {
                throw new IllegalArgumentException("If min or max are provided, both must be provided");
            }
            if (min instanceof Number && (double) min < 10 || max instanceof Number && (double) max < 10) {
                JsLog.warn("Ignoring max/min, at least one doesn't make sense", max, min);
            } else {
                this.min = JsDateTimeFormat.longFromDate(min)
                        .orElseThrow(() -> new IllegalArgumentException("Cannot interpret min as a date: " + min));
                this.max = JsDateTimeFormat.longFromDate(max)
                        .orElseThrow(() -> new IllegalArgumentException("Cannot interpret max as a date: " + max));
            }
        } else {
            this.min = null;
            this.max = null;
        }

        jsFigure.updateDownsampleRange(axis, this.pixels, this.min, this.max);
    }

    public AxisDescriptor getDescriptor() {
        return this.axis;
    }
}

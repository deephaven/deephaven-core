package io.deephaven.web.client.api.widget.plot;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.AxisDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.BusinessCalendarDescriptor;
import io.deephaven.web.client.api.i18n.JsDateTimeFormat;
import io.deephaven.web.client.api.widget.calendar.JsBusinessCalendar;
import io.deephaven.web.client.fu.JsLog;
import jsinterop.annotations.*;
import jsinterop.base.Js;

@JsType
public class JsAxis {
    private final AxisDescriptor axis;
    private final JsFigure jsFigure;
    private final JsBusinessCalendar businessCalendar;

    private Integer pixels;
    private Long min;
    private Long max;

    @JsIgnore
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

    @JsProperty
    public JsBusinessCalendar getBusinessCalendar() {
        return businessCalendar;
    }

    @JsProperty
    public String getId() {
        return axis.getId();
    }

    @JsProperty
    @SuppressWarnings("unusable-by-js")
    public int getFormatType() {
        return axis.getFormatType();
    }

    @JsProperty
    @SuppressWarnings("unusable-by-js")
    public int getType() {
        return axis.getType();
    }

    @JsProperty
    @SuppressWarnings("unusable-by-js")
    public int getPosition() {
        return axis.getPosition();
    }

    @JsProperty
    public boolean isLog() {
        return axis.getLog();
    }

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

    @JsProperty
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
    public double getGapBetweenMajorTicks() {
        return axis.getGapBetweenMajorTicks();
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

    @JsMethod
    public void range(@JsOptional Double pixelCount, @JsOptional Object min, @JsOptional Object max) {
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

    @JsIgnore
    public AxisDescriptor getDescriptor() {
        return this.axis;
    }
}

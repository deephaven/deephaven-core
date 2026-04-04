//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import io.deephaven.web.client.fu.JsData;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

/**
 * A descriptor used to configure an axis when creating a figure from JS.
 */
@JsType(name = "AxisDescriptor", namespace = "dh.plot")
public class JsAxisDescriptor {
    // TODO (deephaven-core#3442) change to some kind of String+int union type

    /**
     * The axis format type.
     *
     * <p>
     * This should be a value from {@link io.deephaven.web.client.api.widget.plot.enums.JsAxisFormatType}.
     */
    public String formatType;
    // TODO (deephaven-core#3442) change to some kind of String+int union type

    /**
     * The axis type.
     *
     * <p>
     * This should be a value from {@link io.deephaven.web.client.api.widget.plot.enums.JsAxisType}.
     */
    public String type;
    // TODO (deephaven-core#3442) change to some kind of String+int union type

    /**
     * The axis position.
     *
     * <p>
     * This should be a value from {@link io.deephaven.web.client.api.widget.plot.enums.JsAxisPosition}.
     */
    public String position;

    /**
     * Whether to use logarithmic scaling.
     */
    @JsNullable
    public boolean log = false;

    /**
     * The axis label.
     */
    @JsNullable
    public String label;

    /**
     * The font used to render the axis label.
     */
    @JsNullable
    public String labelFont;

    /**
     * The font used to render tick labels.
     */
    @JsNullable
    public String ticksFont;

    /**
     * The format pattern used to render axis values.
     */
    @JsNullable
    public String formatPattern;

    /**
     * The axis color.
     */
    @JsNullable
    public String color;

    /**
     * The minimum value for the axis range, or {@link Double#NaN} to use the default.
     */
    @JsNullable
    public double minRange = Double.NaN;

    /**
     * The maximum value for the axis range, or {@link Double#NaN} to use the default.
     */
    @JsNullable
    public double maxRange = Double.NaN;

    /**
     * Whether minor ticks are visible.
     *
     * <p>
     * Minor ticks are typically small, unlabeled tick marks drawn between major ticks.
     */
    @JsNullable
    public boolean minorTicksVisible = false;

    /**
     * Whether major ticks are visible.
     *
     * <p>
     * Major ticks are the primary tick marks on an axis. They typically determine the main tick spacing and are often
     * the tick marks that are labeled.
     */
    @JsNullable
    public boolean majorTicksVisible = true;

    /**
     * The number of minor ticks between adjacent major ticks.
     */
    @JsNullable
    public int minorTickCount = 0;

    /**
     * The gap between adjacent major ticks.
     */
    @JsNullable
    public double gapBetweenMajorTicks = -1.0;

    /**
     * Explicit locations for major ticks.
     *
     * <p>
     * When specified, these values are used as the primary (major) tick positions instead of automatically choosing
     * major ticks.
     */
    @JsNullable
    public JsArray<Double> majorTickLocations = new JsArray<>();

    /**
     * The angle to render tick labels at.
     */
    @JsNullable
    public double tickLabelAngle = 0.0;

    /**
     * Whether to invert the axis.
     */
    @JsNullable
    public boolean invert = false;

    /**
     * Whether this axis should be treated as a time axis.
     */
    @JsNullable
    public boolean isTimeAxis = false;

    @JsConstructor
    public JsAxisDescriptor() {}

    @JsIgnore
    public JsAxisDescriptor(JsPropertyMap<Object> source) {
        this();

        formatType = JsData.getRequiredStringProperty(source, "formatType");
        type = JsData.getRequiredStringProperty(source, "type");
        position = JsData.getRequiredStringProperty(source, "position");
        log = JsData.getBooleanProperty(source, "log");
        label = JsData.getStringProperty(source, "label");
        labelFont = JsData.getStringProperty(source, "labelFont");
        ticksFont = JsData.getStringProperty(source, "ticksFont");
        formatPattern = JsData.getStringProperty(source, "formatPattern");
        color = JsData.getStringProperty(source, "color");
        minRange = JsData.getDoubleProperty(source, "minRange", Double.NaN);
        maxRange = JsData.getDoubleProperty(source, "maxRange", Double.NaN);
        minorTicksVisible = JsData.getBooleanProperty(source, "minorTicksVisible");
        majorTicksVisible = JsData.getBooleanProperty(source, "majorTicksVisible", true);
        minorTickCount = JsData.getIntProperty(source, "minorTickCount");
        gapBetweenMajorTicks = JsData.getDoubleProperty(source, "gapBetweenMajorTicks", -1);
        tickLabelAngle = JsData.getDoubleProperty(source, "tickLabelAngle");
        invert = JsData.getBooleanProperty(source, "invert");
        isTimeAxis = JsData.getBooleanProperty(source, "isTimeAxis");
        if (source.has("majorTickLocations")) {
            majorTickLocations = source.getAsAny("majorTickLocations").uncheckedCast();
        }
    }
}

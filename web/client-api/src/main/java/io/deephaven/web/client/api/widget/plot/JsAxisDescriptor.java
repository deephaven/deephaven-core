/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import io.deephaven.web.client.fu.JsData;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

@JsType(name = "AxisDescriptor", namespace = "dh.plot")
public class JsAxisDescriptor {
    // TODO (deephaven-core#3442) change to some kind of String+int union type
    public String formatType;
    // TODO (deephaven-core#3442) change to some kind of String+int union type
    public String type;
    // TODO (deephaven-core#3442) change to some kind of String+int union type
    public String position;

    @JsNullable
    public boolean log = false;
    @JsNullable
    public String label;
    @JsNullable
    public String labelFont;
    @JsNullable
    public String ticksFont;
    @JsNullable
    public String formatPattern;
    @JsNullable
    public String color;
    @JsNullable
    public double minRange = Double.NaN;
    @JsNullable
    public double maxRange = Double.NaN;
    @JsNullable
    public boolean minorTicksVisible = false;
    @JsNullable
    public boolean majorTicksVisible = true;
    @JsNullable
    public int minorTickCount = 0;
    @JsNullable
    public double gapBetweenMajorTicks = -1.0;
    @JsNullable
    public JsArray<Double> majorTickLocations = new JsArray<>();
    @JsNullable
    public double tickLabelAngle = 0.0;
    @JsNullable
    public boolean invert = false;
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

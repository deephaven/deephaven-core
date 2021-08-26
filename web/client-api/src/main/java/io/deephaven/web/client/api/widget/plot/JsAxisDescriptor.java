package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import io.deephaven.web.client.fu.JsData;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

@JsType(name = "AxisDescriptor", namespace = "dh.plot")
public class JsAxisDescriptor {
    public String formatType;
    public String type;
    public String position;

    public boolean log = false;
    public String label;
    public String labelFont;
    public String ticksFont;
    public String formatPattern;
    public String color;
    public double minRange = Double.NaN;
    public double maxRange = Double.NaN;
    public boolean minorTicksVisible = false;
    public boolean majorTicksVisible = true;
    public int minorTickCount = 0;
    public double gapBetweenMajorTicks = -1.0;
    public JsArray<Double> majorTickLocations = new JsArray<>();
    public double tickLabelAngle = 0.0;
    public boolean invert = false;
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
        majorTicksVisible = JsData.getBooleanProperty(source, "majorTicksVisible");
        minorTickCount = JsData.getIntProperty(source, "minorTickCount");
        gapBetweenMajorTicks = JsData.getDoubleProperty(source, "gapBetweenMajorTicks", -1);
        tickLabelAngle = JsData.getDoubleProperty(source, "tickLabelAngle");
        invert = JsData.getBooleanProperty(source, "invert");
        isTimeAxis = JsData.getBooleanProperty(source, "isTimeAxis");
        if (source.has("majorTickLocations")) {
            majorTickLocations = source.getAny("majorTickLocations").uncheckedCast();
        }
    }
}

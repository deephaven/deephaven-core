//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import io.deephaven.web.client.fu.JsData;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.util.Map;

@JsType(name = "SeriesDescriptor", namespace = "dh.plot")
public class JsSeriesDescriptor {
    // TODO (deephaven-core#3442) change to some kind of String+int union type
    public String plotStyle;
    @JsNullable
    public String name;

    @JsNullable
    public Boolean linesVisible;
    @JsNullable
    public Boolean shapesVisible;
    @JsNullable
    public Boolean gradientVisible;

    @JsNullable
    public String lineColor;
    @JsNullable
    public String pointLabelFormat;
    @JsNullable
    public String xToolTipPattern;
    @JsNullable
    public String yToolTipPattern;

    @JsNullable
    public String shapeLabel;
    @JsNullable
    public Double shapeSize;
    @JsNullable
    public String shapeColor;
    @JsNullable
    public String shape;

    public JsArray<JsSourceDescriptor> dataSources;

    @JsConstructor
    public JsSeriesDescriptor() {}

    @JsIgnore
    public JsSeriesDescriptor(JsPropertyMap<Object> source, Map<Object, JsAxisDescriptor> axisMap) {
        this();

        plotStyle = JsData.getRequiredStringProperty(source, "plotStyle");
        name = JsData.getStringProperty(source, "name");
        JsArray<JsPropertyMap<Object>> dataSources = JsData.getRequiredProperty(source, "dataSources").cast();
        this.dataSources = dataSources.map((sourceSource, index) -> new JsSourceDescriptor(sourceSource, axisMap));
        linesVisible = JsData.getNullableBooleanProperty(source, "linesVisible");
        shapesVisible = JsData.getNullableBooleanProperty(source, "shapesVisible");
        gradientVisible = JsData.getNullableBooleanProperty(source, "gradientVisible");
        lineColor = JsData.getStringProperty(source, "lineColor");
        pointLabelFormat = JsData.getStringProperty(source, "pointLabelFormat");
        xToolTipPattern = JsData.getStringProperty(source, "xToolTipPattern");
        yToolTipPattern = JsData.getStringProperty(source, "yToolTipPattern");
        shapeLabel = JsData.getStringProperty(source, "shapeLabel");
        shapeSize = JsData.getNullableDoubleProperty(source, "shapeSize");
        shapeColor = JsData.getStringProperty(source, "shapeColor");
        shape = JsData.getStringProperty(source, "shape");
    }
}

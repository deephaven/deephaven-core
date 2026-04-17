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

import java.util.Map;

/**
 * A descriptor used to define a chart series when creating a figure from JS.
 */
@JsType(name = "SeriesDescriptor", namespace = "dh.plot")
public class JsSeriesDescriptor {
    // TODO (deephaven-core#3442) change to some kind of String+int union type

    /**
     * The series plot style.
     */
    public String plotStyle;

    /**
     * The series name.
     */
    @JsNullable
    public String name;

    /**
     * Whether lines are visible.
     */
    @JsNullable
    public Boolean linesVisible;

    /**
     * Whether shapes are visible.
     */
    @JsNullable
    public Boolean shapesVisible;

    /**
     * Whether the series uses gradient rendering.
     */
    @JsNullable
    public Boolean gradientVisible;

    /**
     * The line color.
     */
    @JsNullable
    public String lineColor;

    /**
     * A format string used to render point labels.
     *
     * This value is passed through to the underlying plotting implementation. The interpretation of the format string
     * is defined by the plotting implementation.
     */
    @JsNullable
    public String pointLabelFormat;

    /**
     * A format string used to render X values in tooltips.
     *
     * This value is passed through to the underlying plotting implementation. The interpretation of the format string
     * is defined by the plotting implementation.
     */
    @JsNullable
    public String xToolTipPattern;

    /**
     * A format string used to render Y values in tooltips.
     *
     * This value is passed through to the underlying plotting implementation. The interpretation of the format string
     * is defined by the plotting implementation.
     */
    @JsNullable
    public String yToolTipPattern;

    /**
     * The shape label.
     */
    @JsNullable
    public String shapeLabel;

    /**
     * The shape size.
     */
    @JsNullable
    public Double shapeSize;

    /**
     * The shape color.
     */
    @JsNullable
    public String shapeColor;

    /**
     * The shape name.
     */
    @JsNullable
    public String shape;

    /**
     * The data sources for this series.
     */
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

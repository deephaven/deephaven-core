package io.deephaven.web.client.api.widget.plot;

import elemental2.core.JsArray;
import io.deephaven.web.client.fu.JsData;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.Map;

@JsType(name = "SeriesDescriptor", namespace = "dh.plot")
public class JsSeriesDescriptor {
    public String plotStyle;
    public String name;

    public Boolean linesVisible;
    public Boolean shapesVisible;
    public Boolean gradientVisible;

    public String lineColor;
    public String pointLabelFormat;
    public String xToolTipPattern;
    public String yToolTipPattern;

    public String shapeLabel;
    public Double shapeSize;
    public String shapeColor;
    public String shape;

    public JsArray<JsSourceDescriptor> dataSources;

    @JsConstructor
    public JsSeriesDescriptor() {}

    @JsIgnore
    public JsSeriesDescriptor(JsPropertyMap<Object> source, Map<Object, JsAxisDescriptor> axisMap) {
        this();

        plotStyle = JsData.getRequiredStringProperty(source, "plotStyle");
        name = JsData.getStringProperty(source, "name");
        JsArray<Object> dataSources = JsData.getRequiredProperty(source, "dataSources").cast();
        this.dataSources = Js.uncheckedCast(dataSources.map(
                (sourceSource, index, all) -> new JsSourceDescriptor((JsPropertyMap<Object>) sourceSource, axisMap)));
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

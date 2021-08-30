package io.deephaven.web.client.api.widget.plot;

import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.fu.JsData;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.util.Map;

@JsType(name = "SourceDescriptor", namespace = "dh.plot")
public class JsSourceDescriptor {
    public JsAxisDescriptor axis;
    public JsTable table;

    public String columnName;
    public String type;

    @JsConstructor
    public JsSourceDescriptor() {}

    @JsIgnore
    public JsSourceDescriptor(JsPropertyMap<Object> source, Map<Object, JsAxisDescriptor> axisMap) {
        this();

        Object axisSource = JsData.getRequiredProperty(source, "axis");
        if (axisMap.containsKey(axisSource)) {
            axis = axisMap.get(axisSource);
        } else if (axisSource instanceof JsAxisDescriptor) {
            axis = (JsAxisDescriptor) axisSource;
        } else {
            axis = new JsAxisDescriptor((JsPropertyMap<Object>) axisSource);
        }
        table = JsData.getRequiredProperty(source, "table").cast();
        columnName = JsData.getRequiredStringProperty(source, "columnName");
        type = JsData.getRequiredStringProperty(source, "type");
    }
}

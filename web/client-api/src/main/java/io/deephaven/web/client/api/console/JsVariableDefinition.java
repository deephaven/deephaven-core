package io.deephaven.web.client.api.console;

import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

@JsType(namespace = "dh")
public class JsVariableDefinition implements Serializable {
    private String name;
    private String type;

    @JsIgnore
    public static JsVariableDefinition from(Object definitionObject) {
        if (definitionObject instanceof JsVariableDefinition) {
            return (JsVariableDefinition) definitionObject;
        } else {
            return new JsVariableDefinition(Js.cast(definitionObject));
        }
    }

    @JsConstructor
    public JsVariableDefinition() {}

    @JsIgnore
    public JsVariableDefinition(String name, String type) {
        this();
        this.name = name;
        this.type = type;
    }

    @JsIgnore
    public JsVariableDefinition(JsPropertyMap<Object> source) {
        this();
        if (source.has("name")) {
            this.name = source.getAny("name").asString();
        }

        if (source.has("type")) {
            this.type = source.getAny("type").asString();
        }
    }

    @JsProperty
    public String getName() {
        return name;
    }

    @JsProperty
    public String getType() {
        return type;
    }
}

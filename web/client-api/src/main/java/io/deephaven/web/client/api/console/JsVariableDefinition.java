package io.deephaven.web.client.api.console;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb.FieldInfo;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

public class JsVariableDefinition {
    private final String type;
    private final String title;
    private final String id;
    private final String description;
    private final String applicationId;
    private final String applicationName;

    public static JsVariableDefinition from(Object definitionObject) {
        if (definitionObject instanceof JsVariableDefinition) {
            return (JsVariableDefinition) definitionObject;
        } else {
            return new JsVariableDefinition(Js.asPropertyMap(definitionObject));
        }
    }

    public JsVariableDefinition(String type, String title, String id, String description) {
        this.type = type;
        this.title = title;
        this.id = id;
        this.description = description;
        this.applicationId = "scope";
        this.applicationName = "";
    }

    public JsVariableDefinition(FieldInfo field) {
        this.type = JsVariableChanges.getVariableTypeFromFieldCase(field.getFieldType().getFieldCase());
        this.id = field.getTicket().getTicket_asB64();
        this.title = field.getFieldName();
        this.description = field.getFieldDescription();
        this.applicationId = field.getApplicationId();
        this.applicationName = field.getApplicationName();
    }

    public JsVariableDefinition(JsPropertyMap<Object> source) {
        if (source.has("type")) {
            this.type = source.getAny("type").asString();
        } else {
            throw new IllegalArgumentException("no type field; could not construct JsVariableDefinition");
        }

        boolean hasName = source.has("name");
        boolean hasId = source.has("id");
        if (hasName && hasId) {
            throw new IllegalArgumentException("has both name and id field; could not construct JsVariableDefinition");
        } else if (hasName) {
            this.id = source.getAny("name").asString();
        } else if (hasId) {
            this.id = source.getAny("id").asString();
        } else {
            throw new IllegalArgumentException("no name/id field; could not construct JsVariableDefinition");
        }

        this.title = "js-constructed-not-available";
        this.description = "js-constructed-not-available";
        this.applicationId = "js-constructed-not-available";
        this.applicationName = "js-constructed-not-available";
    }

    @JsProperty
    public String getType() {
        return type;
    }

    @JsProperty
    @Deprecated
    public String getName() {
        return title;
    }

    @JsProperty
    public String getTitle() {
        return title;
    }

    @JsProperty
    public String getId() {
        return id;
    }

    @JsProperty
    public String getDescription() {
        return description;
    }

    @JsProperty
    public String getApplicationId() {
        return applicationId;
    }

    @JsProperty
    public String getApplicationName() {
        return applicationName;
    }
}

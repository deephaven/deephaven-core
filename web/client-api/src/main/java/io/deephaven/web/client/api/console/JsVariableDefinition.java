package io.deephaven.web.client.api.console;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb.FieldInfo;
import jsinterop.annotations.JsProperty;

public class JsVariableDefinition {
    private static final String JS_UNAVAILABLE = "js-constructed-not-available";

    private final String type;
    private final String title;
    private final String id;
    private final String description;
    private final String applicationId;
    private final String applicationName;

    public JsVariableDefinition(String type, String title, String id, String description) {
        this.type = type;
        this.title = title == null ? JS_UNAVAILABLE : title;
        this.id = id;
        this.description = description == null ? JS_UNAVAILABLE : description;
        this.applicationId = "scope";
        this.applicationName = "";
    }

    public JsVariableDefinition(FieldInfo field) {
        this.type = field.getTypedTicket().getType();
        this.id = field.getTypedTicket().getTicket().getTicket_asB64();
        this.title = field.getFieldName();
        this.description = field.getFieldDescription();
        this.applicationId = field.getApplicationId();
        this.applicationName = field.getApplicationName();
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

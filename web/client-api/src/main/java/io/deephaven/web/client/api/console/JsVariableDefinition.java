//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.console;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.application_pb.FieldInfo;
import jsinterop.annotations.JsProperty;

/**
 * A format to describe a variable available to be read from the server. Application fields are optional, and only
 * populated when a variable is provided by application mode.
 * <p>
 * APIs which take a VariableDefinition must at least be provided an object with a <b>type</b> and <b>id</b> field.
 */
@TsInterface
@TsName(namespace = "dh.ide", name = "VariableDefinition")
public class JsVariableDefinition {
    private static final String JS_UNAVAILABLE = "js-constructed-not-available";

    private final String type;
    private final String title;
    private final String id;
    private final String description;
    private final String applicationId;
    private final String applicationName;

    public JsVariableDefinition(String type, String title, String id, String description) {
        // base64('s/' + str) starts with 'cy8' or 'cy9'
        // base64('a/' + str) starts with 'YS8' or 'YS9'
        if (!id.startsWith("cy") && !id.startsWith("YS")) {
            throw new IllegalArgumentException("Cannot create a VariableDefinition from a non-scope ticket");
        }
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

    /**
     * The type of the variable, one of <b>dh.VariableType</b>
     * 
     * @return dh.VariableType.
     */
    @JsProperty
    @TsTypeRef(JsVariableType.class)
    public String getType() {
        return type;
    }

    @JsProperty
    @Deprecated
    public String getName() {
        return title;
    }

    /**
     * The name of the variable, to be used when rendering it to a user
     * 
     * @return String
     */
    @JsProperty
    public String getTitle() {
        return title;
    }

    /**
     * An opaque identifier for this variable
     * 
     * @return String
     */
    @JsProperty
    public String getId() {
        return id;
    }

    /**
     * Optional description for the variable's contents, typically used to provide more detail that wouldn't be
     * reasonable to put in the title
     * 
     * @return String
     */
    @JsProperty
    public String getDescription() {
        return description;
    }

    /**
     * Optional description for the variable's contents, typically used to provide more detail that wouldn't be
     * reasonable to put in the title
     * 
     * @return String
     */
    @JsProperty
    public String getApplicationId() {
        return applicationId;
    }

    /**
     * The name of the application which provided this variable
     * 
     * @return String
     */
    @JsProperty
    public String getApplicationName() {
        return applicationName;
    }
}

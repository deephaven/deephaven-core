package io.deephaven.web.client.api.console;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb.FieldInfo;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb.FieldsChangeUpdate;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;

public class JsVariableChanges {
    @JsProperty(namespace = "dh.VariableType")
    public static final String TABLE = "Table",
            TREETABLE = "TreeTable",
            TABLEMAP = "TableMap",
            FIGURE = "Figure",
            OTHERWIDGET = "OtherWidget",
            PANDAS = "Pandas";

    private final JsVariableDefinition[] created;
    private final JsVariableDefinition[] updated;
    private final JsVariableDefinition[] removed;

    public static JsVariableChanges from(FieldsChangeUpdate update) {
        final JsVariableDefinition[] created = toVariableDefinitions(update.getCreatedList());
        final JsVariableDefinition[] updated = toVariableDefinitions(update.getUpdatedList());
        final JsVariableDefinition[] removed = toVariableDefinitions(update.getRemovedList());
        return new JsVariableChanges(created, updated, removed);
    }

    private static JsVariableDefinition[] toVariableDefinitions(JsArray<FieldInfo> createdList) {
        final JsVariableDefinition[] definitions = new JsVariableDefinition[createdList.length];
        for (int i = 0; i < createdList.length; i++) {
            definitions[i] = new JsVariableDefinition(createdList.getAt(i));
        }
        return definitions;
    }

    public JsVariableChanges(JsVariableDefinition[] created, JsVariableDefinition[] updated,
            JsVariableDefinition[] removed) {
        this.created = JsObject.freeze(created);
        this.updated = JsObject.freeze(updated);
        this.removed = JsObject.freeze(removed);
    }

    @JsProperty
    public JsArray<JsVariableDefinition> getCreated() {
        return Js.uncheckedCast(created);
    }

    @JsProperty
    public JsArray<JsVariableDefinition> getUpdated() {
        return Js.uncheckedCast(updated);
    }

    @JsProperty
    public JsArray<JsVariableDefinition> getRemoved() {
        return Js.uncheckedCast(removed);
    }
}

package io.deephaven.web.client.api.console;

import elemental2.core.JsArray;
import io.deephaven.web.shared.ide.VariableChanges;
import io.deephaven.web.shared.ide.VariableDefinition;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;

import java.util.Arrays;

public class JsVariableChanges {
    @JsProperty(namespace = "dh.VariableType")
    public static final String TABLE = "Table",
        TREETABLE = "TreeTable",
        TABLEMAP = "TableMap",
        FIGURE = "Figure",
        OTHERWIDGET = "OtherWidget",
        PANDAS = "Pandas";

    private JsVariableDefinition[] created;
    private JsVariableDefinition[] updated;
    private JsVariableDefinition[] removed;

    private static JsVariableDefinition[] convertDefinitions(VariableDefinition[] definitions) {
        return Arrays.stream(definitions)
            .map(def -> new JsVariableDefinition(def.getName(), def.getType()))
            .toArray(JsVariableDefinition[]::new);
    }

    public JsVariableChanges(VariableChanges changes) {
        created = convertDefinitions(changes.created);
        updated = convertDefinitions(changes.updated);
        removed = convertDefinitions(changes.removed);
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

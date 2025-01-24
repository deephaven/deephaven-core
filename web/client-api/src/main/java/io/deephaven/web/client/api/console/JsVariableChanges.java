//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.console;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.application_pb.FieldInfo;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.application_pb.FieldsChangeUpdate;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;


/**
 * Describes changes in the current set of variables in the script session. Note that variables that changed value
 * without changing type will be included as <b>updated</b>, but if a new value with one type replaces an old value with
 * a different type, this will be included as an entry in both <b>removed</b> and <b>created</b> to indicate the old and
 * new types.
 */
@TsInterface
@TsName(namespace = "dh.ide", name = "VariableChanges")
public class JsVariableChanges {

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

    /**
     * @return The variables that were created by this operation, or have a new type.
     */
    @JsProperty
    public JsArray<JsVariableDefinition> getCreated() {
        return Js.uncheckedCast(created);
    }

    /**
     * @return The variables that changed value during this operation.
     */
    @JsProperty
    public JsArray<JsVariableDefinition> getUpdated() {
        return Js.uncheckedCast(updated);
    }

    /**
     * @return The variables that no longer exist after this operation, or were replaced by some variable with a
     *         different type.
     */
    @JsProperty
    public JsArray<JsVariableDefinition> getRemoved() {
        return Js.uncheckedCast(removed);
    }
}

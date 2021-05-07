package io.deephaven.web.client.api.console;

import io.deephaven.web.shared.ide.CommandResult;
import jsinterop.annotations.JsProperty;

public class JsCommandResult {
    private JsVariableChanges changes;
    private String error;

    public JsCommandResult(CommandResult result) {
        changes = new JsVariableChanges(result.getChanges());
        error = result.getError();
    }

    @JsProperty
    public JsVariableChanges getChanges() {
        return changes;
    }

    @JsProperty
    public String getError() {
        return error;
    }
}

package io.deephaven.web.client.api.console;

import jsinterop.annotations.JsProperty;

public class JsCommandResult {
    private JsVariableChanges changes;
    private String error;

    public JsCommandResult(JsVariableChanges changes, String error) {
        this.changes = changes;
        this.error = error;
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

package io.deephaven.web.shared.ide;

import java.io.Serializable;

/**
 * Represents the results from running a block of code from a RemoteScriptCommandQuery
 */
public class CommandResult implements Serializable {
    private VariableChanges changes = new VariableChanges();
    private String error; // TODO a nicely formatted object

    public VariableChanges getChanges() {
        return changes;
    }

    public String getError() {
        return error;
    }

    public void setChanges(VariableChanges changes) {
        this.changes = changes;
    }

    public void setError(String error) {
        this.error = error;
    }
}

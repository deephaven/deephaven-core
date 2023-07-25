/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.console;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsProperty;

/**
 * Indicates the result of code run on the server.
 */
@TsInterface
@TsName(namespace = "dh.ide", name = "CommandResult")
public class JsCommandResult {
    private final JsVariableChanges changes;
    private final String error;

    public JsCommandResult(JsVariableChanges changes, String error) {
        this.changes = changes;
        this.error = error;
    }

    /**
     * Describes changes made in the course of this command.
     * 
     * @return {@link JsVariableChanges}.
     */
    @JsProperty
    public JsVariableChanges getChanges() {
        return changes;
    }

    /**
     * If the command failed, the error message will be provided here.
     * 
     * @return String
     */
    @JsProperty
    public String getError() {
        return error;
    }

    @Override
    public String toString() {
        if (error != null && !error.isEmpty()) {
            return error;
        }
        return super.toString();
    }
}

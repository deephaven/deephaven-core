//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.console;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.web.client.api.DateWrapper;
import jsinterop.annotations.JsProperty;

/**
 * Indicates the result of code run on the server.
 */
@TsInterface
@TsName(namespace = "dh.ide", name = "CommandResult")
public class JsCommandResult {
    private final JsVariableChanges changes;
    private final String error;
    private final long startTimestamp;
    private final long endTimestamp;

    public JsCommandResult(JsVariableChanges changes, String error, long startTimestamp, long endTimestamp) {
        this.changes = changes;
        this.error = error;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
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

    /**
     * The timestamp when the command started running.
     *
     * @return DateWrapper
     */
    @JsProperty
    public DateWrapper getStartTimestamp() {
        return DateWrapper.of(startTimestamp);
    }

    /**
     * The timestamp when the command finished running.
     *
     * @return DateWrapper
     */
    @JsProperty
    public DateWrapper getEndTimestamp() {
        return DateWrapper.of(endTimestamp);
    }

    @Override
    public String toString() {
        if (error != null && !error.isEmpty()) {
            return error;
        }
        return super.toString();
    }
}

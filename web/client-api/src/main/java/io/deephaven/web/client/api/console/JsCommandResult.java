//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.console;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.web.client.api.LongWrapper;
import jsinterop.annotations.JsProperty;

/**
 * Indicates the result of code run on the server.
 */
@TsInterface
@TsName(namespace = "dh.ide", name = "CommandResult")
public class JsCommandResult {
    private final JsVariableChanges changes;
    private final String error;
    private final String startTimestamp;
    private final String endTimestamp;

    public JsCommandResult(JsVariableChanges changes, String error, String startTimestamp, String endTimestamp) {
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
     * @return long
     */
    @JsProperty
    public LongWrapper getStartTimestamp() {
        return LongWrapper.of(Long.parseLong(startTimestamp));
    }

    /**
     * The timestamp when the command finished running.
     *
     * @return long
     */
    @JsProperty
    public LongWrapper getEndTimestamp() {
        return LongWrapper.of(Long.parseLong(endTimestamp));
    }

    @Override
    public String toString() {
        if (error != null && !error.isEmpty()) {
            return error;
        }
        return super.toString();
    }
}

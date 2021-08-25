package io.deephaven.web.shared.ide;

import io.deephaven.web.shared.cmd.ServerReplyHandle;

/**
 * A client-generated handle for each execution.
 *
 * This allows the client to issue cancellation requests using an id it knows before the script is even requested.
 */
public class ExecutionHandle extends ServerReplyHandle {

    private int clientId;
    private int scriptId;
    private int connectionId;

    public ExecutionHandle() {
        this(DESERIALIZATION_IN_PROGRESS);
    }

    public ExecutionHandle(int clientId) {
        this.clientId = clientId;
    }

    @Override
    public int getClientId() {
        return clientId;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    public int getScriptId() {
        return scriptId;
    }

    public void setScriptId(int scriptId) {
        this.scriptId = scriptId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final ExecutionHandle that = (ExecutionHandle) o;

        return clientId == that.clientId;
    }

    @Override
    public int hashCode() {
        return clientId;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }
}

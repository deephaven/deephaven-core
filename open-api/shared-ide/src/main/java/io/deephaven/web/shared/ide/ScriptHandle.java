package io.deephaven.web.shared.ide;

import io.deephaven.web.shared.cmd.ServerReplyHandle;

/**
 * A handle object to represent a console session.
 */
public class ScriptHandle extends ServerReplyHandle {

    /**
     * The client-originated handle for this script session. Used by each client to identify which handle to use in
     * operations.
     *
     * These will not be unique; most cases this will =1
     *
     * Identity semantics will use clientId + connectionId.
     *
     * When we support joining other user's sessions, we will do so by providing both the clientId and the connectionId
     * as integers, and a new ScriptHandle to represent joining the other session (so when the joining user cancels that
     * handle, it does not affect the joined session). Internally, we will record in each "real session" how many
     * ScriptHandles are open to it.
     */
    private int clientId;
    /**
     * The server-originated handle for this script session. This is used to key into the static ScriptSession object;
     * when we support recovering / restarting past sessions, this value _might_ change, but it effectively stable.
     *
     * This field does not participate in identity semantics.
     */
    private int scriptId;
    /**
     * The per-socket connection id.
     *
     * This is a bit of a "magic id" that is passed through all handles, so we can effectively store session state keyed
     * off any given handle.
     *
     * The custom field serializers treat this specially, where client and server each have their own internal
     * connectionId for each socket, and they write the connectionId their recipient expects to receive (so all objects
     * in memory have local ids, and all objects on the wire have remote ids).
     *
     * If a socket dies and has to reconnect, this value will be updated.
     *
     * This field participates in identity semantics.
     */
    private int connectionId;

    public ScriptHandle() {
        this(DESERIALIZATION_IN_PROGRESS);
    }

    public ScriptHandle(int clientId) {
        this.clientId = clientId;
        connectionId = scriptId = UNINITIALIZED;
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

        final ScriptHandle that = (ScriptHandle) o;

        return clientId == that.clientId && connectionId == that.connectionId;
    }

    @Override
    public int hashCode() {
        return clientId ^ 31 + connectionId;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public ScriptHandle setConnectionId(int connectionId) {
        this.connectionId = connectionId;
        return this;
    }
}

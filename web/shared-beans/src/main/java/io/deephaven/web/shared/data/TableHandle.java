package io.deephaven.web.shared.data;

import io.deephaven.web.shared.cmd.ServerReplyHandle;

import javax.annotation.Nonnull;

/**
 * In order to track tables sanely, we will force the client to choose an id to be used for tables
 * _before_ they are created, and then map server table handles back to those ids.
 *
 * While this forces the server to do a little bending over to figure out where a table is expected
 * by the client before sending messages, this allows the client to cancel an in-flight request,
 * before it finishes and the server id is known.
 *
 * Note that this object uses object identity semantics in its equals method; we only consider the
 * clientId in hashCode/equals, so we can safely use these as map keys before the serverId is known.
 */
public class TableHandle extends ServerReplyHandle implements Comparable<TableHandle> {

    public static final int UNASSIGNED = -1;
    public static final int DISCONNECTED = -2;
    public static final int RELEASED = -3;
    private int clientId;
    private volatile int serverId;
    private int connectionId;

    /**
     * All TableHandle's should come from a factory which pools instances
     * ({@link ClientTableIdFactory})
     *
     * This is necessary for nice "object identity semantics" when putting handles into maps like
     * JsMap, or IdentityHashMap.
     *
     * This constructor is deprecated so that you will come here and read this comment, and then
     * either a) use the factory from {@link TableHandle_CustomFieldSerializer}, b) rethink whatever
     * you are doing to pass a TableHandle in from the client.
     *
     * These should never be created on the server.
     */
    @Deprecated
    TableHandle() {}

    /**
     * All TableHandle's should come from a factory which pools instances
     * ({@link ClientTableIdFactory})
     *
     * This is necessary for nice "object identity semantics" when putting handles into maps like
     * JsMap, or IdentityHashMap.
     *
     * This constructor is deprecated so that you will come here and read this comment, and then
     * either a) use the factory from {@link TableHandle_CustomFieldSerializer}, b) rethink whatever
     * you are doing to pass a TableHandle in from the client.
     *
     * These should never be created on the server.
     *
     * @param clientId - the client-generated int id.
     */
    @Deprecated
    public TableHandle(int clientId) {
        this.clientId = clientId;
        this.serverId = UNASSIGNED;
        assert clientId != 0 : "clientId must be non-zero!";
    }

    public int getClientId() {
        return clientId;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    public int getServerId() {
        return serverId;
    }

    public boolean setServerId(int serverId) {
        boolean changed = this.serverId != serverId;
        this.serverId = serverId;
        return changed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final TableHandle that = (TableHandle) o;

        // purposely only considering clientId and connectionId; serverId can be 0 for a while, then
        // change,
        // so we don't want items in maps to "get lost" when we fill in the serverId.
        return clientId == that.clientId && connectionId == that.connectionId;
    }

    @Override
    public int hashCode() {
        // purposely only considering clientId; serverId can be 0 for a while, then change,
        // so we don't want items in maps to "get lost" when we fill in the serverId.
        return clientId * 31 + connectionId;
    }

    @Override
    public String toString() {
        return "TableHandle{" +
            "clientId=" + clientId +
            ", serverId=" + serverId +
            ", connectionId=" + connectionId +
            '}';
    }

    public boolean isResolved() {
        return serverId >= 0;
    }

    public boolean isUnresolved() {
        return serverId < 0;
    }

    @Override
    public int compareTo(@Nonnull TableHandle o) {
        return clientId - o.clientId;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    public boolean isDisconnected() {
        return serverId == DISCONNECTED;
    }

    public boolean isReleased() {
        return serverId == RELEASED;
    }
}

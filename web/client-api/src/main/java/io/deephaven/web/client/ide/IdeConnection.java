//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.ide;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.TerminationNotificationResponse;
import io.deephaven.web.client.api.ConnectOptions;
import io.deephaven.web.client.api.JsPendingObject;
import io.deephaven.web.client.api.JsPendingTable;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.JsTableReference;
import io.deephaven.web.client.api.MicrotaskBatchImpl;
import io.deephaven.web.client.api.QueryConnectable;
import io.deephaven.web.client.api.ServerObject;
import io.deephaven.web.client.api.TableTicket;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.console.JsVariableChanges;
import io.deephaven.web.client.api.console.JsVariableDescriptor;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.grpc.FetchTransport;
import io.deephaven.web.client.api.grpc.MultiplexedWebsocketTransport;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.util.List;

import static io.deephaven.web.client.api.QueryInfoConstants.EVENT_TABLE_OPENED;

/**
 * Presently, this is the entrypoint into the Deephaven JS API. By creating an instance of this with the server URL and
 * some options, JS applications can run code on the server, and interact with available exportable objects.
 */
@JsType(namespace = "dh")
public class IdeConnection extends QueryConnectable<IdeConnection> {
    @Deprecated
    public static final String HACK_CONNECTION_FAILURE = "hack-connection-failure";
    public static final String EVENT_DISCONNECT = "disconnect";
    public static final String EVENT_RECONNECT = "reconnect";

    public static final String EVENT_SHUTDOWN = "shutdown";

    private final String serverUrl;

    private final ConnectToken token = new ConnectToken();
    private final ConnectOptions options;

    /**
     * Creates a new instance, from which console sessions can be made.
     * 
     * @param serverUrl The url used when connecting to the server. Read-only.
     * @param connectOptions Optional Object
     */
    @JsIgnore
    public IdeConnection(String serverUrl, Object connectOptions) {
        // Remove trailing slashes from the url
        this.serverUrl = serverUrl.replaceAll("/+$", "");

        if (connectOptions != null) {
            options = new ConnectOptions(connectOptions);
        } else {
            options = new ConnectOptions();
        }
        if (options.transportFactory == null) {
            // assign a default transport factory
            if (options.useWebsockets == Boolean.TRUE || !serverUrl.startsWith("https:")) {
                options.transportFactory = new MultiplexedWebsocketTransport.Factory();
            } else {
                options.transportFactory = new FetchTransport.Factory();
            }
        }
    }

    @Override
    protected String logPrefix() {
        return "IdeConnection on " + getServerUrl() + ": ";
    }

    @JsIgnore
    @Override
    public ConnectToken getToken() {
        return token;
    }

    @JsIgnore
    @Override
    public ConnectOptions getOptions() {
        return options;
    }

    /**
     * Closes the current connection, releasing any resources on the server or client.
     */
    // Made public to expose to JS
    public void close() {
        super.close();
    }

    /**
     * The url used when connecting to the server.
     * 
     * @return String.
     */
    @Override
    @JsIgnore
    public String getServerUrl() {
        return serverUrl;
    }

    @Override
    public Promise<IdeConnection> running() {
        // This assumes that once the connection has been initialized and left a usable state, it cannot be used again
        if (!connection.isAvailable() || connection.get().isUsable()) {
            return Promise.resolve(this);
        } else {
            return Promise.reject("Cannot connect, session is dead.");
        }
    }

    /**
     * Load the named table, with columns and size information already fully populated.
     *
     * @param name the name of the table to fetch.
     * @param applyPreviewColumns {@code false} to disable previews. Defaults to {@code true}.
     * @return the table, as a pending table that rejects with an error if it cannot be loaded
     * @deprecated Added to resolve a specific issue, in the future preview will be applied as part of the subscription.
     */
    @Deprecated
    public JsPendingTable<JsTable> getTable(String name, @JsOptional @JsNullable Boolean applyPreviewColumns) {
        if (applyPreviewColumns == Boolean.FALSE) {
            JsLog.warn(
                    "getTable is deprecated, please use getObject instead. The applyPreviewColumns parameter no longer applies, the new APIs to access data from the resulting Table should be used instead.");
        }
        // The ticket and state exist now, so operations can be chained on the result before the fetch has been sent;
        // the table itself, and the opened event, come with the first await
        final WorkerConnection workerConnection = connection.get();
        final TableTicket handle = workerConnection.getTickets().newTableTicket();
        return workerConnection.pendingTable(
                workerConnection.fetchTableState(
                        workerConnection.getVariableDefinition(name, JsVariableType.TABLE), applyPreviewColumns,
                        handle),
                table -> fireEvent(EVENT_TABLE_OPENED, table));
    }

    /**
     * Fetches the object a variable descriptor names.
     *
     * @param definitionObject the variable descriptor
     * @return a pending table if the object is a table, otherwise a promise of the object
     */
    public JsPendingObject getObject(@TsTypeRef(JsVariableDescriptor.class) JsPropertyMap<Object> definitionObject) {
        WorkerConnection conn = connection.get();
        String type;
        try {
            type = WorkerConnection.definitionType(definitionObject);
        } catch (IllegalArgumentException e) {
            // As before this returned a pending object, a bad definition rejects rather than throws
            return JsPendingObject.of(Promise.reject(e));
        }
        if (JsVariableType.TABLE.equalsIgnoreCase(type)) {
            // The ticket and state exist now, so operations can be chained on the table before we have even connected
            TableTicket handle = conn.getTickets().newTableTicket();
            return JsPendingObject.of(conn.pendingTable(conn.fetchTableState(
                    onConnected().then(e -> conn.lookupDefinition(definitionObject)), null, handle)));
        }
        return JsPendingObject.of(onConnected().then(e -> conn.getJsObject(definitionObject).asPromise()));
    }

    public JsRunnable subscribeToFieldUpdates(JsConsumer<JsVariableChanges> callback) {
        // Need to make sure the connection is initialized and connected
        WorkerConnection conn = connection.get();
        Promise<JsRunnable> cleanupPromise =
                onConnected().then(e -> Promise.resolve(conn.subscribeToFieldUpdates(callback)));
        return () -> {
            cleanupPromise.then(c -> {
                c.run();
                return null;
            });
        };
    }

    /**
     * Makes an {@code object} available to another user or another client on this same server which knows the value of
     * the {@code sharedTicketBytes}. Use that {@code sharedTicketBytes} value like a one-time use password - any other
     * client which knows this value can read the same object.
     * <p>
     * Shared objects will remain available using the {@code sharedTicketBytes} until the client that first shared them
     * releases/closes their copy of the object. Whatever side-channel is used to share the bytes, be sure to wait until
     * the remote end has signaled that it has successfully fetched the object before releasing it from this client. A
     * pending table that nothing awaits is released once its result arrives and the share has been made, so keep it by
     * awaiting it, or share the table that awaiting gives.
     * <p>
     * Be sure to use an unpredictable value for the shared ticket bytes, like a UUID or other large, random value to
     * prevent access by unauthorized clients.
     *
     * @param object The object to share with another client/user.
     * @param sharedTicketBytes The value which another client/user must know to obtain the object. It may be a unicode
     *        string (will be encoded as utf8 bytes), or a {@link elemental2.core.Uint8Array} value.
     * @return A promise that will resolve to the value passed as {@code sharedTicketBytes} when the object is ready to
     *         be read by another client, or will reject if an error occurs.
     */
    public Promise<SharedExportBytesUnion> shareObject(ServerObject.Union object,
            SharedExportBytesUnion sharedTicketBytes) {
        return connection.get().shareObject(object.asServerObject(), sharedTicketBytes);
    }

    /**
     * Reads an object shared by another client to this server with the {@code sharedTicketBytes}. Until the other
     * client releases this object (or their session ends), the object will be available on the server.
     * <p>
     * The type of the object must be passed so that the object can be read from the server correct - the other client
     * should provide this information.
     *
     * @param sharedTicketBytes The value provided by another client/user to obtain the object. It may be a unicode
     *        string (will be encoded as utf8 bytes), or a {@link elemental2.core.Uint8Array} value.
     * @param type The type of the object, so it can be correctly read from the server.
     * @return the shared object, as a pending object that rejects with an error if it cannot be read
     */
    public JsPendingObject getSharedObject(SharedExportBytesUnion sharedTicketBytes, String type) {
        return connection.get().getSharedObject(sharedTicketBytes, type);
    }

    @JsIgnore
    @Override
    public void notifyServerShutdown(TerminationNotificationResponse success) {
        final String details;
        if (!success.getAbnormalTermination()) {
            details = "Server exited normally.";
        } else {
            StringBuilder retval;
            if (!success.getReason().isEmpty()) {
                retval = new StringBuilder(success.getReason());
            } else {
                retval = new StringBuilder("Server exited abnormally.");
            }

            final List<TerminationNotificationResponse.StackTrace> traces = success.getStackTracesList();
            for (int ii = 0; ii < traces.size(); ++ii) {
                final TerminationNotificationResponse.StackTrace trace = traces.get(ii);
                retval.append("\n\n");
                if (ii != 0) {
                    retval.append("Caused By: ").append(trace.getType()).append(": ").append(trace.getMessage());
                } else {
                    retval.append(trace.getType()).append(": ").append(trace.getMessage());
                }

                final List<String> elements = trace.getElementsList();
                for (int jj = 0; jj < elements.size(); ++jj) {
                    retval.append("\n").append(elements.get(jj));
                }
            }

            details = retval.toString();
        }

        // fire shutdown advice event
        fireEvent(EVENT_SHUTDOWN, details);

        // fire deprecated event
        notifyConnectionError(new StatusRuntimeException(Status.UNAVAILABLE.withDescription(details)));
    }

    /**
     * Creates a table on the server from the given column data.
     *
     * @param columnNames the name of each column
     * @param types the type of each column
     * @param data the values of each column, as strings
     * @param userTimeZone the time zone to read date-time values in
     * @return the new table, as a pending table
     */
    public JsPendingTable<JsTable> newTable(String[] columnNames, String[] types, String[][] data,
            String userTimeZone) {
        return connection.get().newTable(columnNames, types, data, userTimeZone);
    }

    /**
     * Merges the given tables into a single table. Assumes all tables have the same structure.
     * 
     * @param tables
     * @return the merged table, as a pending table
     */
    public JsPendingTable<JsTable> mergeTables(JsTableReference[] tables) {
        return connection.get().mergeTables(tables, this);
    }
}

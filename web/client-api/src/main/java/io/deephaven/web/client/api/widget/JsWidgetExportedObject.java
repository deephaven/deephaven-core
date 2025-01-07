//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.session_pb.ExportRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.JsLazy;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.ServerObject;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

/**
 * Represents a server-side object that may not yet have been fetched by the client. When this object will no longer be
 * used, if {@link #fetch()} is not called on this object, then {@link #close()} must be to ensure server-side resources
 * are correctly freed.
 */
@TsInterface
@TsName(namespace = "dh", name = "WidgetExportedObject")
public class JsWidgetExportedObject implements ServerObject {
    private final WorkerConnection connection;

    private final TypedTicket ticket;

    private final JsLazy<Promise<?>> fetched;

    public JsWidgetExportedObject(WorkerConnection connection, TypedTicket ticket) {
        this.connection = connection;
        this.ticket = ticket;
        this.fetched = JsLazy.of(() -> {
            if (getType() == null) {
                return Promise.reject("Exported object has no type, can't be fetched");
            }
            if (getType().equals(JsVariableType.TABLE)) {
                return Callbacks.<ExportedTableCreationResponse, Object>grpcUnaryPromise(c -> {
                    connection.tableServiceClient().getExportedTableCreationResponse(ticket.getTicket(),
                            connection.metadata(),
                            c::apply);
                }).then(etcr -> {
                    ClientTableState cts = connection.newStateFromUnsolicitedTable(etcr, "table for widget");
                    JsTable table = new JsTable(connection, cts);
                    // never attempt a reconnect, since we might have a different widget schema entirely
                    table.addEventListener(JsTable.EVENT_DISCONNECT, ignore -> table.close());
                    return Promise.resolve(table);
                });
            } else {
                return this.connection.getObject(ticket);
            }
        });
    }

    /**
     * Returns the type of this export, typically one of {@link JsVariableType}, but may also include plugin types. If
     * null, this object cannot be fetched, but can be passed to the server, such as via
     * {@link JsWidget#sendMessage(JsWidget.MessageUnion, JsArray)}.
     *
     * @return the string type of this server-side object, or null.
     */
    @JsNullable
    @JsProperty
    public String getType() {
        if (ticket.getType().isEmpty()) {
            return null;
        }
        return ticket.getType();
    }

    @Override
    public TypedTicket typedTicket() {
        TypedTicket typedTicket = new TypedTicket();
        typedTicket.setTicket(ticket.getTicket());
        typedTicket.setType(getType());
        return typedTicket;
    }

    /**
     * Exports another copy of this reference, allowing it to be fetched separately. Results in rejection if the ticket
     * was already closed (either by calling {@link #close()} or closing the object returned from {@link #fetch()}).
     *
     * @return a promise returning a reexported copy of this object, still referencing the same server-side object.
     */
    @JsMethod
    public Promise<JsWidgetExportedObject> reexport() {
        Ticket reexportedTicket = connection.getConfig().newTicket();

        // Future optimization - we could "race" these by running the export in the background, to avoid
        // an extra round trip.
        return Callbacks.grpcUnaryPromise(c -> {
            ExportRequest req = new ExportRequest();
            req.setSourceId(ticket.getTicket());
            req.setResultId(reexportedTicket);
            connection.sessionServiceClient().exportFromTicket(req, connection.metadata(), c::apply);
        }).then(success -> {
            TypedTicket typedTicket = new TypedTicket();
            typedTicket.setTicket(reexportedTicket);
            typedTicket.setType(ticket.getType());
            return Promise.resolve(new JsWidgetExportedObject(connection, typedTicket));
        });
    }

    /**
     * Returns a promise that will fetch the object represented by this reference. Multiple calls to this will return
     * the same instance.
     * 
     * @return a promise that will resolve to a client side object that represents the reference on the server.
     */
    @JsMethod
    public Promise<?> fetch() {
        if (getType() != null) {
            return fetched.get();
        }
        return Promise.reject("Can't fetch an object with no type (i.e. no server plugin implementation)");
    }

    /**
     * Releases the server-side resources associated with this object, regardless of whether other client-side objects
     * exist that also use that object. Should not be called after fetch() has been invoked.
     */
    @JsMethod
    public void close() {
        if (!fetched.isAvailable()) {
            connection.releaseTicket(ticket.getTicket());
        } else {
            JsLog.warn("Cannot close, already fetched. Instead, close the fetched object.");
        }
    }
}

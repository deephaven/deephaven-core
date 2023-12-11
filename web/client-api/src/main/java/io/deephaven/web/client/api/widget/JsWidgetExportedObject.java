/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.widget;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.ExportRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.JsLazy;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.ServerObject;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

/**
 * Represents a server-side object that may not yet have been fetched by the client.
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
                return this.connection.getObject(
                        new JsVariableDefinition(ticket.getType(), null, ticket.getTicket().getTicket_asB64(), null));
            }
        });
    }

    @JsProperty
    public String getType() {
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
     * @return a promise returning a reexported copy of this object, still referencing the same server-side object.
     */
    @JsMethod
    public Promise<JsWidgetExportedObject> reexport() {
        Ticket reexportedTicket = connection.getConfig().newTicket();
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


    // Could also return Promise<JsWidgetExportedObject> - perhaps if we make it have a `boolean refetch` arg?
    // /**
    // * Returns a copy of this widget, so that any later {@link #fetch()} will always return a fresh instance.
    // * @return
    // */
    // @JsMethod
    // public JsWidgetExportedObject copy() {
    // return new JsWidgetExportedObject(connection, ticket);
    // }

    /**
     * Returns a promise that will fetch the object represented by this reference. Multiple calls to this will return
     * the same instance.
     * 
     * @return a promise that will resolve to a client side object that represents the reference on the server.
     */
    @JsMethod
    public Promise<?> fetch() {
        return fetched.get();
    }

    /**
     * Releases the server-side resources associated with this object, regardless of whether other client-side objects
     * exist that also use that object. Should not be called after fetch() has been invoked.
     */
    @JsMethod
    public void close() {
        if (!fetched.isAvailable()) {
            connection.releaseTicket(ticket.getTicket());
        }
    }
}

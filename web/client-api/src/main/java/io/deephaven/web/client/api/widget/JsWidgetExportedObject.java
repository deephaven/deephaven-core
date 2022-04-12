package io.deephaven.web.client.api.widget;

import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.console.JsVariableChanges;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

public class JsWidgetExportedObject {
    private final WorkerConnection connection;

    private final TypedTicket ticket;

    public JsWidgetExportedObject(WorkerConnection connection, TypedTicket ticket) {
        this.connection = connection;
        this.ticket = ticket;
    }

    @JsProperty
    public String getType() {
        return ticket.getType();
    }

    @JsMethod
    public Promise<Object> fetch() {
        if (getType().equals(JsVariableChanges.TABLE)) {
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
    }
}

package io.deephaven.web.client.api.widget;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb.FetchObjectResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

public class JsWidget {

    private final WorkerConnection connection;

    private final FetchObjectResponse response;

    public JsWidget(WorkerConnection connection, FetchObjectResponse response) {
        this.connection = connection;
        this.response = response;
    }

    @JsProperty
    public String getType() {
        return response.getType();
    }

    @JsProperty
    public String getData() {
        return response.getData_asB64();
    }

    @JsProperty
    public Uint8Array getDataAsU8() {
        return response.getData_asU8();
    }

    @JsProperty
    public String[] getExportedObjectTypes() {
        return response.getTypedExportIdList().asList().stream().map(TypedTicket::getType).toArray(String[]::new);
    }

    @JsMethod
    public Promise<JsTable> getTable(int index) {
        TypedTicket ticket = this.response.getTypedExportIdList().getAt(index);
        if (!ticket.getType().equals("Table")) {
            // TODO (deephaven-core#62) implement fetch for tablemaps
            assert false : ticket.getType() + " found in widget, not yet supported";
            return null;
        }

        return Callbacks.<ExportedTableCreationResponse, Object>grpcUnaryPromise(c -> {
            connection.tableServiceClient().getExportedTableCreationResponse(ticket.getTicket(), connection.metadata(),
                    c::apply);
        }).then(etcr -> {
            ClientTableState cts = connection.newStateFromUnsolicitedTable(etcr, "table for widget");
            JsTable table = new JsTable(connection, cts);
            // never attempt a reconnect, since we might have a different widget schema entirely
            table.addEventListener(JsTable.EVENT_DISCONNECT, ignore -> table.close());
            return Promise.resolve(table);
        });
    }
}

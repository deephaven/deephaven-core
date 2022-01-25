package io.deephaven.web.client.api.widget;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb.FetchObjectResponse;
import io.deephaven.web.client.api.WorkerConnection;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

public class JsWidget {
    private final WorkerConnection connection;

    private final FetchObjectResponse response;

    private JsWidgetExportedObject[] exportedObjects;

    public JsWidget(WorkerConnection connection, FetchObjectResponse response) {
        this.connection = connection;
        this.response = response;
    }

    @JsProperty
    public String getType() {
        return response.getType();
    }

    @JsMethod
    public String getDataAsBase64() {
        return response.getData_asB64();
    }

    @JsMethod
    public Uint8Array getDataAsU8() {
        return response.getData_asU8();
    }

    @JsProperty
    public JsWidgetExportedObject[] getExportedObjects() {
        if (this.exportedObjects == null) {
            this.exportedObjects = response.getTypedExportIdList().asList().stream()
                    .map(ticket -> new JsWidgetExportedObject(connection, ticket))
                    .toArray(JsWidgetExportedObject[]::new);
        }

        return this.exportedObjects;
    }
}

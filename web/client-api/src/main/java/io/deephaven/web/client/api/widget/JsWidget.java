/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.widget;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.Uint8Array;
import elemental2.dom.CustomEventInit;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.AutoCompleteRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.AutoCompleteResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb.*;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

import java.util.function.Supplier;

@TsInterface
@TsName(namespace = "dh", name = "Widget")
public class JsWidget {
    private final WorkerConnection connection;
    private final WidgetFetch fetch;
    private Ticket ticket;

    private final Supplier<BiDiStream<MessageRequest, MessageResponse>> streamFactory;
    private BiDiStream<MessageRequest, MessageResponse> messageStream;

    @JsFunction
    public interface WidgetFetchCallback {
        void handleResponse(Object error, FetchObjectResponse response, Ticket requestedTicket);
    }
    @JsFunction
    public interface WidgetFetch {
        void fetch(WidgetFetchCallback callback);
    }

    private FetchObjectResponse response;

    private JsWidgetExportedObject[] exportedObjects;

    public JsWidget(WorkerConnection connection, WidgetFetch fetch) {
        this.connection = connection;
        this.fetch = fetch;
        BiDiStream.Factory<MessageRequest, MessageResponse> factory = connection.streamFactory();
        streamFactory = () -> factory.create(
                connection.objectServiceClient()::messageStream,
                (first, headers) -> connection.objectServiceClient().openMessageStream(first, headers),
                (next, headers, c) -> connection.objectServiceClient().nextMessageStream(next, headers, c::apply),
                new MessageRequest());
    }

    public Promise<JsWidget> refetch() {
        return new Promise<>((resolve, reject) -> {
            fetch.fetch((err, response, ticket) -> {
                if (err != null) {
                    reject.onInvoke(err);
                } else {
                    this.response = response;
                    this.ticket = ticket;

                    messageStream = streamFactory.get();
                    messageStream.onData(res -> {
                        // Pass the data to any attached message handler
                        // res.getData();
                    });
                    messageStream.onStatus(status -> {
                        if (!status.isOk()) {
                            // Do something if request failed
                        }
                    });
                    messageStream.onEnd(status -> {
                        messageStream = null;
                    });

                    // First message establishes a connection w/ the plugin object instance we're talking to
                    MessageRequest req = new MessageRequest();
                    ConnectRequest data = new ConnectRequest();
                    data.setTypedTicket(ticket);
                    req.setSourceId(data);
                    messageStream.send(req);

                    resolve.onInvoke(this);
                }
            });
        });
    }

    public Ticket getTicket() {
        return ticket;
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

    @JsMethod
    public void sendMessage(String msg) {
        if (messageStream == null) {
            // Should we warn the user they tried to send a message to an unfetched widget?
            return;
        }
        MessageRequest req = new MessageRequest();
        DataRequest data = new DataRequest();
        data.setData(msg);
        req.setData(data);
        messageStream.send(req);
    }
}

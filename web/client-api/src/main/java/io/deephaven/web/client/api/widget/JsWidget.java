/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.widget;

import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.ArrayBuffer;
import elemental2.core.ArrayBufferView;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import elemental2.dom.CustomEventInit;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb.*;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.HasEventHandling;
import io.deephaven.web.client.api.ServerObject;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * A Widget represents a server side object that sends one or more responses to the client. The client can then
 * interpret these responses to see what to render, or how to respond.
 *
 * Most custom object types result in a single response being sent to the client, often with other exported objects, but
 * some will have streamed responses, and allow the client to send follow-up requests of its own. This class's API is
 * backwards compatible, but as such does not offer a way to tell the difference between a streaming or non-streaming
 * object type, the client code that handles the payloads is expected to know what to expect. See
 * dh.WidgetMessageDetails for more information.
 *
 * When the promise that returns this object resolves, it will have the first response assigned to its fields. Later
 * responses from the server will be emitted as "message" events. When the connection with the server ends
 */
// TODO consider reconnect support? This is somewhat tricky without understanding the semantics of the widget
@TsName(namespace = "dh", name = "Widget")
public class JsWidget extends HasEventHandling implements ServerObject, WidgetMessageDetails {
    @JsProperty(namespace = "dh.Widget")
    public static final String EVENT_MESSAGE = "message";
    @JsProperty(namespace = "dh.Widget")
    public static final String EVENT_CLOSE = "close";

    private final WorkerConnection connection;
    private final TypedTicket typedTicket;

    private boolean hasFetched;

    private final Supplier<BiDiStream<StreamRequest, StreamResponse>> streamFactory;
    private BiDiStream<StreamRequest, StreamResponse> messageStream;

    private StreamResponse response;

    private JsArray<JsWidgetExportedObject> exportedObjects;

    public JsWidget(WorkerConnection connection, TypedTicket typedTicket) {
        this.connection = connection;
        this.typedTicket = typedTicket;
        hasFetched = false;
        BiDiStream.Factory<StreamRequest, StreamResponse> factory = connection.streamFactory();
        streamFactory = () -> factory.create(
                connection.objectServiceClient()::messageStream,
                (first, headers) -> connection.objectServiceClient().openMessageStream(first, headers),
                (next, headers, c) -> connection.objectServiceClient().nextMessageStream(next, headers, c::apply),
                new StreamRequest());
        this.exportedObjects = new JsArray<>();
    }

    private void closeStream() {
        if (messageStream != null) {
            messageStream.end();
            messageStream = null;
        }
        hasFetched = false;
    }

    /**
     * Ends the client connection to the server.
     */
    @JsMethod
    public void close() {
        suppressEvents();
        closeStream();
    }

    public Promise<JsWidget> refetch() {
        closeStream();
        return new Promise<>((resolve, reject) -> {
            exportedObjects = new JsArray<>();

            messageStream = streamFactory.get();
            messageStream.onData(res -> {

                JsArray<JsWidgetExportedObject> responseObjects = res.getData().getExportedReferencesList()
                        .map((p0, p1, p2) -> new JsWidgetExportedObject(connection, p0));
                if (!hasFetched) {
                    response = res;
                    exportedObjects = responseObjects;

                    hasFetched = true;
                    resolve.onInvoke(this);
                } else {
                    CustomEventInit<EventDetails> messageEvent = CustomEventInit.create();
                    messageEvent.setDetail(new EventDetails(res.getData(), responseObjects));
                    fireEvent(EVENT_MESSAGE, messageEvent);
                }
            });
            messageStream.onStatus(status -> {
                if (!status.isOk()) {
                    reject.onInvoke(status.getDetails());
                    fireEvent(EVENT_CLOSE);
                    closeStream();
                }
            });
            messageStream.onEnd(status -> {
                closeStream();
            });

            // First message establishes a connection w/ the plugin object instance we're talking to
            StreamRequest req = new StreamRequest();
            ConnectRequest data = new ConnectRequest();
            data.setSourceId(typedTicket);
            req.setConnect(data);
            messageStream.send(req);
        });
    }

    public Ticket getTicket() {
        return typedTicket.getTicket();
    }

    /**
     * @return the type of this widget
     */
    @JsProperty
    public String getType() {
        return typedTicket.getType();
    }

    @Override
    public TypedTicket typedTicket() {
        TypedTicket typedTicket = new TypedTicket();
        typedTicket.setTicket(getTicket());
        typedTicket.setType(getType());
        return typedTicket;
    }

    @Override
    @JsMethod
    public String getDataAsBase64() {
        return response.getData().getPayload_asB64();
    }

    @Override
    @JsMethod
    public Uint8Array getDataAsU8() {
        return response.getData().getPayload_asU8();
    }

    @Override
    @JsMethod
    public String getDataAsString() {
        return new String(Js.uncheckedCast(response.getData().getPayload_asU8()), StandardCharsets.UTF_8);
    }

    @Override
    @JsProperty
    public JsWidgetExportedObject[] getExportedObjects() {
        return Js.uncheckedCast(exportedObjects);
    }

    @TsUnion
    @JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
    public interface MessageUnion {
        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isArrayBuffer() {
            return this instanceof ArrayBuffer;
        }

        @JsOverlay
        default boolean isView() {
            return ArrayBuffer.isView(this);
        }

        @JsOverlay
        @TsUnionMember
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        @TsUnionMember
        default ArrayBuffer asArrayBuffer() {
            return Js.cast(this);
        }

        @JsOverlay
        @TsUnionMember
        default ArrayBufferView asView() {
            // This must be unchecked because there is no type in JS with this name
            return Js.uncheckedCast(this);
        }
    }

    /**
     * Sends a string/bytes payload to the server, along with references to objects that exist on the server.
     *
     * @param msg string/buffer/view instance that represents data to send
     * @param references an array of objects that can be safely sent to the server
     */
    @JsMethod
    public void sendMessage(MessageUnion msg,
            @JsOptional JsArray<@TsTypeRef(ServerObject.Union.class) ServerObject> references) {
        if (messageStream == null) {
            return;
        }
        StreamRequest req = new StreamRequest();
        Data data = new Data();
        if (msg.isString()) {
            byte[] bytes = msg.asString().getBytes(StandardCharsets.UTF_8);
            Uint8Array payload = new Uint8Array(bytes.length);
            payload.set(Js.<double[]>uncheckedCast(bytes));
            data.setPayload(payload);
        } else if (msg.isArrayBuffer()) {
            data.setPayload(new Uint8Array(msg.asArrayBuffer()));
        } else if (msg.isView()) {
            // can cast (unsafely) to any typed array or to DataView to read offset/length/buffer to make a new view
            ArrayBufferView view = msg.asView();
            data.setPayload(new Uint8Array(view.buffer, view.byteOffset, view.byteLength));
        } else {
            throw new IllegalArgumentException("Expected message to be a String or ArrayBuffer");
        }

        for (int i = 0; references != null && i < references.length; i++) {
            ServerObject reference = references.getAt(i);
            data.addExportedReferences(reference.typedTicket());
        }

        req.setData(data);
        messageStream.send(req);
    }

    /**
     * Event details to convey a response from the server.
     */
    @TsName(namespace = "dh", name = "WidgetMessageDetails")
    private static class EventDetails implements WidgetMessageDetails {
        private final Data data;
        private final JsArray<JsWidgetExportedObject> exportedObjects;

        public EventDetails(Data data, JsArray<JsWidgetExportedObject> exports) {
            this.data = data;
            this.exportedObjects = exports;
        }

        @Override
        public String getDataAsBase64() {
            return data.getPayload_asB64();
        }

        @Override
        public Uint8Array getDataAsU8() {
            return data.getPayload_asU8();
        }

        @Override
        public String getDataAsString() {
            return new String(Js.uncheckedCast(data.getPayload_asU8()), StandardCharsets.UTF_8);
        }

        @Override
        public JsWidgetExportedObject[] getExportedObjects() {
            return Js.uncheckedCast(exportedObjects);
        }
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget;

import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.ArrayBuffer;
import elemental2.core.ArrayBufferView;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb.ClientData;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb.ConnectRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb.ServerData;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb.StreamRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb.StreamResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.ServerObject;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.event.HasEventHandling;
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
 * <p>
 * Most custom object types result in a single response being sent to the client, often with other exported objects, but
 * some will have streamed responses, and allow the client to send follow-up requests of its own. This class's API is
 * backwards compatible, but as such does not offer a way to tell the difference between a streaming or non-streaming
 * object type, the client code that handles the payloads is expected to know what to expect. See
 * {@link WidgetMessageDetails} for more information.
 * <p>
 * When the promise that returns this object resolves, it will have the first response assigned to its fields. Later
 * responses from the server will be emitted as "message" events. When the connection with the server ends, the "close"
 * event will be emitted. In this way, the connection will behave roughly in the same way as a WebSocket - either side
 * can close, and after close no more messages will be processed. There can be some latency in closing locally while
 * remote messages are still pending - it is up to implementations of plugins to handle this case.
 * <p>
 * Also like WebSockets, the plugin API doesn't define how to serialize messages, and just handles any binary payloads.
 * What it does handle however, is allowing those messages to include references to server-side objects with those
 * payloads. Those server side objects might be tables or other built-in types in the Deephaven JS API, or could be
 * objects usable through their own plugins. They also might have no plugin at all, allowing the client to hold a
 * reference to them and pass them back to the server, either to the current plugin instance, or through another API.
 * The {@code Widget} type does not specify how those objects should be used or their lifecycle, but leaves that
 * entirely to the plugin. Messages will arrive in the order they were sent.
 * <p>
 * This can suggest several patterns for how plugins operate:
 * <ul>
 * <li>The plugin merely exists to transport some other object to the client. This can be useful for objects which can
 * easily be translated to some other type (like a Table) when the user clicks on it. An example of this is
 * {@code pandas.DataFrame} will result in a widget that only contains a static
 * {@link io.deephaven.web.client.api.JsTable}. Presently, the widget is immediately closed, and only the Table is
 * provided to the JS API consumer.</li>
 * <li>The plugin provides references to Tables and other objects, and those objects can live longer than the object
 * which provided them. One concrete example of this could have been
 * {@link io.deephaven.web.client.api.JsPartitionedTable} when fetching constituent tables, but it was implemented
 * before bidirectional plugins were implemented. Another example of this is plugins that serve as a "factory", giving
 * the user access to table manipulation/creation methods not supported by gRPC or the JS API.</li>
 * <li>The plugin provides reference to Tables and other objects that only make sense within the context of the widget
 * instance, so when the widget goes away, those objects should be released as well. This is also an example of
 * {@link io.deephaven.web.client.api.JsPartitionedTable}, as the partitioned table tracks creation of new keys through
 * an internal table instance.</li>
 * </ul>
 *
 * Handling server objects in messages also has more than one potential pattern that can be used:
 * <ul>
 * <li>One object per message - the message clearly is about that object, no other details required.</li>
 * <li>Objects indexed within their message - as each message comes with a list of objects, those objects can be
 * referenced within the payload by index. This is roughly how {@link io.deephaven.web.client.api.widget.plot.JsFigure}
 * behaves, where the figure descriptor schema includes an index for each created series, describing which table should
 * be used, which columns should be mapped to each axis.</li>
 * <li>Objects indexed since widget creation - each message would append its objects to a list created when the widget
 * was first made, and any new exports that arrive in a new message would be appended to that list. Then, subsequent
 * messages can reference objects already sent. This imposes a limitation where the client cannot release any exports
 * without the server somehow signaling that it will never reference that export again.</li>
 * </ul>
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
        connection.releaseTicket(getTicket());
    }

    public Promise<JsWidget> refetch() {
        closeStream();
        return new Promise<>((resolve, reject) -> {
            exportedObjects = new JsArray<>();

            messageStream = streamFactory.get();
            messageStream.onData(res -> {

                JsArray<JsWidgetExportedObject> responseObjects = res.getData().getExportedReferencesList()
                        .map((p0, p1) -> new JsWidgetExportedObject(connection, p0));
                if (!hasFetched) {
                    response = res;
                    exportedObjects = responseObjects;

                    hasFetched = true;
                    resolve.onInvoke(this);
                } else {
                    fireEvent(EVENT_MESSAGE, new EventDetails(res.getData(), responseObjects));
                }
            });
            messageStream.onStatus(status -> {
                if (!status.isOk()) {
                    reject.onInvoke(status.getDetails());
                }
                fireEvent(EVENT_CLOSE);
                closeStream();
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

    /**
     * @return the exported objects sent in the initial message from the server. The client is responsible for closing
     *         them when finished using them.
     */
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
    public void sendMessage(MessageUnion msg, @JsOptional JsArray<ServerObject.Union> references) {
        if (messageStream == null) {
            return;
        }
        StreamRequest req = new StreamRequest();
        ClientData data = new ClientData();
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
            ServerObject reference = references.getAt(i).asServerObject();
            data.addReferences(reference.typedTicket());
        }

        req.setData(data);
        messageStream.send(req);
    }

    /**
     * Event details to convey a response from the server.
     */
    @TsName(namespace = "dh", name = "WidgetMessageDetails")
    private static class EventDetails implements WidgetMessageDetails {
        private final ServerData data;
        private final JsArray<JsWidgetExportedObject> exportedObjects;

        public EventDetails(ServerData data, JsArray<JsWidgetExportedObject> exports) {
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

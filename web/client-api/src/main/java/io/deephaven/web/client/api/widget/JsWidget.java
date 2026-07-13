//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.ArrayBuffer;
import elemental2.core.ArrayBufferView;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.BrowserNextResponse;
import io.deephaven.proto.backplane.grpc.ClientData;
import io.deephaven.proto.backplane.grpc.ConnectRequest;
import io.deephaven.proto.backplane.grpc.ExportRequest;
import io.deephaven.proto.backplane.grpc.ServerData;
import io.deephaven.proto.backplane.grpc.StreamRequest;
import io.deephaven.proto.backplane.grpc.StreamResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.ServerObject;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.event.HasEventHandling;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import org.gwtproject.nio.TypedArrayHelper;

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
        streamFactory = () -> factory.<BrowserNextResponse>create(
                connection.objectServiceClient()::messageStream,
                (first, headers) -> connection.objectServiceClient().openMessageStream(first, headers),
                (next, c) -> connection.objectServiceClient().nextMessageStream(next, c));
        this.exportedObjects = new JsArray<>();
    }

    @Override
    public WorkerConnection getConnection() {
        return connection;
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

                JsArray<JsWidgetExportedObject> responseObjects = Js.uncheckedCast(res.getData()
                        .getExportedReferencesList()
                        .stream()
                        .map((p0) -> new JsWidgetExportedObject(connection, p0))
                        .toArray());
                if (!hasFetched) {
                    response = res;
                    exportedObjects = responseObjects;

                    hasFetched = true;
                    resolve.onInvoke(this);
                } else {
                    DomGlobal.setTimeout(ignore -> {
                        fireEvent(EVENT_MESSAGE, new EventDetails(res.getData(), responseObjects));
                    }, 0);
                }
            });
            messageStream.onStatus(status -> {
                if (!status.isOk()) {
                    reject.onInvoke(status.getDescription());
                }
                DomGlobal.setTimeout(ignore -> {
                    fireEvent(EVENT_CLOSE);
                }, 0);
                closeStream();
            });
            messageStream.onEnd(status -> {
                closeStream();
            });

            // First message establishes a connection w/ the plugin object instance we're talking to
            StreamRequest.Builder req = StreamRequest.newBuilder();
            ConnectRequest.Builder data = ConnectRequest.newBuilder();
            data.setSourceId(typedTicket);
            req.setConnect(data);
            messageStream.send(req.build());
        });
    }

    /**
     * Exports another copy of the widget, allowing it to be fetched separately.
     * 
     * @return Promise returning a reexported copy of this widget, still referencing the same server-side widget.
     */
    public Promise<JsWidget> reexport() {
        Ticket reexportedTicket = connection.getTickets().newExportTicket();

        // Race these calls so we avoid a round trip, server will do the synchronization for us
        ExportRequest req = ExportRequest.newBuilder()
                .setSourceId(getTicket())
                .setResultId(reexportedTicket)
                .build();
        connection.sessionServiceClient().exportFromTicket(req, Callbacks.ignore());

        TypedTicket typedTicket = TypedTicket.newBuilder()
                .setTicket(reexportedTicket)
                .setType(getType())
                .build();
        return new JsWidget(connection, typedTicket).refetch();
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
        return TypedTicket.newBuilder()
                .setTicket(getTicket())
                .setType(getType())
                .build();
    }

    @Override
    @JsMethod
    public String getDataAsBase64() {
        return BaseEncoding.base64().encode(response.getData().getPayload().toByteArray());
    }

    @Override
    @JsMethod
    public Uint8Array getDataAsU8() {
        ByteString bytes = response.getData().getPayload();
        Uint8Array payload = new Uint8Array(bytes.size());
        for (int i = 0; i < bytes.size(); i++) {
            payload.setAt(i, (double) bytes.byteAt(i));
        }
        return payload;
    }

    @Override
    @JsMethod
    public String getDataAsString() {
        return response.getData().getPayload().toString(StandardCharsets.UTF_8);
    }

    public ByteString getData() {
        return response.getData().getPayload();
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
    public void sendMessage(MessageUnion msg, @JsOptional @JsNullable JsArray<ServerObject.Union> references) {
        if (messageStream == null) {
            return;
        }
        StreamRequest.Builder req = StreamRequest.newBuilder();
        ClientData.Builder data = ClientData.newBuilder();
        if (msg.isString()) {
            data.setPayload(ByteString.copyFrom(msg.asString(), StandardCharsets.UTF_8));
        } else if (msg.isArrayBuffer()) {
            data.setPayload(ByteStringAccess.wrap(TypedArrayHelper.wrap(msg.asArrayBuffer())));
        } else if (msg.isView()) {
            // can cast (unsafely) to any typed array or to DataView to read offset/length/buffer to make a new view
            ArrayBufferView view = msg.asView();
            data.setPayload(ByteStringAccess.wrap(TypedArrayHelper.wrap(view)));
        } else {
            throw new IllegalArgumentException("Expected message to be a String or ArrayBuffer");
        }

        for (int i = 0; references != null && i < references.length; i++) {
            ServerObject reference = references.getAt(i).asServerObject();
            data.addReferences(reference.typedTicket());
        }

        req.setData(data);
        messageStream.send(req.build());
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
            return BaseEncoding.base64().encode(data.getPayload().toByteArray());
        }

        @Override
        public Uint8Array getDataAsU8() {
            Uint8Array payload = new Uint8Array(data.getPayload().size());
            for (int i = 0; i < data.getPayload().size(); i++) {
                payload.setAt(i, (double) data.getPayload().byteAt(i));
            }
            return payload;
        }

        @Override
        public String getDataAsString() {
            return data.getPayload().toString(StandardCharsets.UTF_8);
        }

        @Override
        public JsWidgetExportedObject[] getExportedObjects() {
            return Js.uncheckedCast(exportedObjects);
        }
    }
}

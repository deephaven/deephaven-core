//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.remotefilesource;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.protobuf.InvalidProtocolBufferException;
import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.Uint8Array;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceClientMessage;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceMetaRequest;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceMetaResponse;
import io.deephaven.proto.backplane.grpc.RemoteFileSourcePluginFetchRequest;
import io.deephaven.proto.backplane.grpc.RemoteFileSourceServerMessage;
import io.deephaven.proto.backplane.grpc.SetExecutionContextRequest;
import io.deephaven.proto.backplane.grpc.SetExecutionContextResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.event.Event;
import io.deephaven.web.client.api.event.EventFn;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.deephaven.web.client.api.widget.JsWidget;
import io.deephaven.web.shared.fu.RemoverFn;
import io.deephaven.web.client.api.widget.WidgetMessageDetails;
import io.deephaven.web.client.fu.LazyPromise;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import org.apache.arrow.flight.impl.Flight;
import org.gwtproject.nio.TypedArrayHelper;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * JavaScript client for the RemoteFileSource service. Provides bidirectional communication with the server-side
 * RemoteFileSourcePlugin via a message stream.
 * <p>
 * Events:
 * <ul>
 * <li>{@link #EVENT_REQUEST_SOURCE}: Fired when the server requests a resource from the client. This event MUST have
 * exactly one listener registered. Attempting to register more than one listener will throw an IllegalStateException.
 * Receiving a resource request without a registered listener will also throw an IllegalStateException.</li>
 * </ul>
 */
@JsType(namespace = "dh.remotefilesource", name = "RemoteFileSourceService")
public class JsRemoteFileSourceService extends HasEventHandling {
    /** Event name for resource request events from the server */
    @JsProperty(namespace = "dh.remotefilesource.RemoteFileSourceService")
    public static final String EVENT_REQUEST_SOURCE = "requestsource";

    // Plugin name must match RemoteFileSourcePlugin.name() on the server
    private static final String PLUGIN_NAME = "DeephavenRemoteFileSourcePlugin";

    // Type URL for the Any-wrapped plugin fetch command. Built explicitly rather than with Any.pack(), which needs
    // protobuf descriptors that aren't available once this code is compiled to JS.
    private static final String PLUGIN_FETCH_REQUEST_TYPE_URL =
            "type.googleapis.com/io.deephaven.proto.backplane.grpc.RemoteFileSourcePluginFetchRequest";

    // Timeout for setExecutionContext requests (in milliseconds)
    private static final int SET_EXECUTION_CONTEXT_TIMEOUT_MS = 30000; // 30 seconds

    private final JsWidget widget;

    // Track pending setExecutionContext requests
    private final Map<String, LazyPromise<Boolean>> pendingSetExecutionContextRequests = new HashMap<>();
    private int requestIdCounter = 0;

    private JsRemoteFileSourceService(JsWidget widget) {
        this.widget = widget;
    }

    /**
     * Overrides addEventListener to enforce that EVENT_REQUEST_SOURCE can only have one listener.
     *
     * @param name the name of the event to listen for
     * @param callback a function to call when the event occurs
     * @return Returns a cleanup function.
     * @param <T> the type of the data that the event will provide
     */
    @Override
    public <T> RemoverFn addEventListener(String name, EventFn<T> callback) {
        if (EVENT_REQUEST_SOURCE.equals(name) && hasListeners(EVENT_REQUEST_SOURCE)) {
            throw new IllegalStateException(
                    "EVENT_REQUEST_SOURCE already has a listener. Only one listener is allowed for this event.");
        }
        return super.addEventListener(name, callback);
    }

    /**
     * Fetches the FlightInfo for the plugin fetch command.
     *
     * @param connection the worker connection to use
     * @return a promise that resolves to the FlightInfo for the plugin fetch
     */
    private static Promise<Flight.FlightInfo> fetchPluginFlightInfo(WorkerConnection connection) {
        // Create the fetch request, with a new export ticket for the result
        RemoteFileSourcePluginFetchRequest fetchRequest = RemoteFileSourcePluginFetchRequest.newBuilder()
                .setResultId(connection.getTickets().newExportTicket())
                .setPluginName(PLUGIN_NAME)
                .build();

        // Wrap in google.protobuf.Any with the proper typeUrl
        Any anyWrappedRequest = Any.newBuilder()
                .setTypeUrl(PLUGIN_FETCH_REQUEST_TYPE_URL)
                .setValue(fetchRequest.toByteString())
                .build();

        // Create a FlightDescriptor with the command
        Flight.FlightDescriptor descriptor = Flight.FlightDescriptor.newBuilder()
                .setType(Flight.FlightDescriptor.DescriptorType.CMD)
                .setCmd(anyWrappedRequest.toByteString())
                .build();

        // Send the getFlightInfo request
        return Callbacks.grpcUnaryPromise(
                c -> connection.flightServiceClient().getFlightInfo(descriptor, c));
    }

    /**
     * Fetches a RemoteFileSource plugin instance from the server and establishes a message stream connection.
     *
     * @param connection the worker connection to use for communication
     * @return a promise that resolves to a RemoteFileSourceService instance with an active message stream
     */
    @JsIgnore
    public static Promise<JsRemoteFileSourceService> fetchPlugin(WorkerConnection connection) {
        return fetchPluginFlightInfo(connection)
                .then(flightInfo -> {
                    // The first endpoint contains the ticket for the plugin instance.
                    // This is the standard Flight pattern: we passed resultTicket in the request,
                    // the server exported the service to that ticket, and returned a FlightInfo
                    // with an endpoint containing that same ticket for us to use.
                    if (flightInfo.getEndpointCount() > 0) {
                        // Convert the Arrow Flight ticket from the endpoint to a Deephaven ticket
                        Ticket dhTicket = Ticket.newBuilder()
                                .setTicket(flightInfo.getEndpoint(0).getTicket().getTicket())
                                .build();

                        // Create a TypedTicket for the plugin instance
                        // The type must match RemoteFileSourcePlugin.name()
                        TypedTicket typedTicket = TypedTicket.newBuilder()
                                .setTicket(dhTicket)
                                .setType(PLUGIN_NAME)
                                .build();

                        JsWidget widget = new JsWidget(connection, typedTicket);

                        JsRemoteFileSourceService service = new JsRemoteFileSourceService(widget);
                        return service.connect();
                    } else {
                        return Promise.reject("No endpoints returned from " + PLUGIN_NAME + " plugin fetch");
                    }
                });
    }

    /**
     * Establishes the message stream connection to the server-side plugin instance.
     *
     * @return a promise that resolves to this service instance when the connection is established
     */
    private Promise<JsRemoteFileSourceService> connect() {
        widget.addEventListener(JsWidget.EVENT_MESSAGE, this::handleMessage);
        return widget.refetch().then(w -> Promise.resolve(this));
    }

    /**
     * Handles incoming messages from the server.
     *
     * @param event the message event from the server
     */
    private void handleMessage(Event<WidgetMessageDetails> event) {
        Uint8Array payload = event.getDetail().getDataAsU8();

        RemoteFileSourceServerMessage message;
        try {
            message = RemoteFileSourceServerMessage
                    .parseFrom(ByteStringAccess.wrap(TypedArrayHelper.wrap(payload)));
        } catch (InvalidProtocolBufferException e) {
            // Failed to parse as proto
            throw new IllegalStateException("Received unparseable message from server", e);
        }

        // Route the parsed message to the appropriate handler
        if (message.hasMetaRequest()) {
            handleMetaRequest(message);
        } else if (message.hasSetExecutionContextResponse()) {
            handleSetExecutionContextResponse(message);
        } else {
            throw new IllegalStateException("Received unknown message type from server");
        }
    }

    /**
     * Handles a meta request (resource request) from the server.
     *
     * @param message the server request message
     */
    private void handleMetaRequest(RemoteFileSourceServerMessage message) {
        if (!hasListeners(EVENT_REQUEST_SOURCE)) {
            throw new IllegalStateException(
                    "Received resource request from server but no listener is registered for EVENT_REQUEST_SOURCE. "
                            + "A listener must be registered to handle resource requests.");
        }
        RemoteFileSourceMetaRequest request = message.getMetaRequest();
        fireEvent(EVENT_REQUEST_SOURCE, new ResourceRequestEvent(message.getRequestId(), request));
    }

    /**
     * Handles a set execution context response from the server.
     *
     * @param message the server request message
     */
    private void handleSetExecutionContextResponse(RemoteFileSourceServerMessage message) {
        String requestId = message.getRequestId();
        LazyPromise<Boolean> promise = pendingSetExecutionContextRequests.remove(requestId);
        if (promise != null) {
            SetExecutionContextResponse response = message.getSetExecutionContextResponse();
            promise.succeed(response.getSuccess());
        }
    }


    /**
     * Sets the execution context on the server to identify this message stream as active for script execution.
     *
     * @param isDirty whether the execution context is dirty (has pending changes)
     * @param resourcePaths array of resource paths to resolve from remote source (e.g., ["com/example/Test.groovy",
     *        "org/mycompany/Utils.groovy"]), or null/empty for no specific resources
     * @return a promise that resolves to true if the server successfully set the execution context, false otherwise
     */
    @JsMethod
    public Promise<Boolean> setExecutionContext(boolean isDirty, @JsOptional String[] resourcePaths) {
        // Generate a unique request ID
        String requestId = "setExecutionContext-" + (requestIdCounter++);

        // Create a lazy promise that will be resolved when we get the response
        LazyPromise<Boolean> promise = new LazyPromise<>();
        pendingSetExecutionContextRequests.put(requestId, promise);

        // Send the request
        RemoteFileSourceClientMessage clientRequest = getSetExecutionContextRequest(isDirty, resourcePaths, requestId);
        sendClientRequest(clientRequest);

        // Return a promise with built-in timeout
        return promise.asPromise(SET_EXECUTION_CONTEXT_TIMEOUT_MS);
    }

    /**
     * Helper method to build a RemoteFileSourceClientMessage for setting execution context.
     *
     * @param isDirty whether the execution context is dirty (has pending changes)
     * @param resourcePaths array of resource paths to resolve
     * @param requestId unique request ID
     * @return the constructed RemoteFileSourceClientMessage
     */
    private static @NotNull RemoteFileSourceClientMessage getSetExecutionContextRequest(boolean isDirty,
            String[] resourcePaths, String requestId) {
        SetExecutionContextRequest.Builder setContextRequest = SetExecutionContextRequest.newBuilder()
                .setIsDirty(isDirty);

        if (resourcePaths != null) {
            setContextRequest.addAllResourcePaths(Arrays.asList(resourcePaths));
        }

        return RemoteFileSourceClientMessage.newBuilder()
                .setRequestId(requestId)
                .setSetExecutionContext(setContextRequest)
                .build();
    }

    /**
     * Helper method to send a RemoteFileSourceClientMessage to the server.
     *
     * @param clientRequest the client request to send
     */
    private void sendClientRequest(RemoteFileSourceClientMessage clientRequest) {
        // Serialize the protobuf message to bytes
        Uint8Array messageBytes = toUint8Array(clientRequest.toByteString());

        // Uint8Array is an ArrayBufferView, which is one of the MessageUnion types
        // The unchecked cast is safe because MessageUnion accepts String | ArrayBuffer | ArrayBufferView
        widget.sendMessage(Js.uncheckedCast(messageBytes), null);
    }

    /**
     * Copies a ByteString into a Uint8Array so that it can be handed to JS APIs.
     *
     * @param bytes the bytes to copy
     * @return a new Uint8Array holding the same bytes
     */
    private static Uint8Array toUint8Array(ByteString bytes) {
        Uint8Array result = new Uint8Array(bytes.size());
        for (int i = 0; i < bytes.size(); i++) {
            result.setAt(i, (double) bytes.byteAt(i));
        }
        return result;
    }

    /**
     * Closes the message stream connection to the server. Any in-flight requests are failed rather than left to time
     * out, since their responses can no longer arrive once the stream is gone.
     */
    public void close() {
        // Take the in-flight promises before clearing, so the map is empty before the failures are delivered
        final List<LazyPromise<Boolean>> pending = new ArrayList<>(pendingSetExecutionContextRequests.values());
        pendingSetExecutionContextRequests.clear();
        pending.forEach(promise -> promise.fail("RemoteFileSourceService closed"));

        widget.close();
    }

    /**
     * Event details for a resource request from the server. Wraps the proto RemoteFileSourceMetaRequest and provides a
     * respond() method.
     */
    @TsInterface
    @TsName(namespace = "dh.remotefilesource")
    public class ResourceRequestEvent {
        private final String requestId;
        private final RemoteFileSourceMetaRequest protoRequest;

        public ResourceRequestEvent(String requestId, RemoteFileSourceMetaRequest protoRequest) {
            this.requestId = requestId;
            this.protoRequest = protoRequest;
        }

        /**
         * @return the name/path of the requested resource
         */
        @JsProperty
        public String getResourceName() {
            return protoRequest.getResourceName();
        }

        /**
         * Responds to this resource request with the given content.
         *
         * @param content the resource content (String | Uint8Array | null):
         *        <ul>
         *        <li>String - will be UTF-8 encoded before sending to server</li>
         *        <li>Uint8Array - sent as-is to server</li>
         *        <li>null - indicates resource was not found</li>
         *        </ul>
         */
        @JsMethod
        public void respond(@JsNullable ResourceContentUnion content) {
            // Build RemoteFileSourceMetaResponse proto
            RemoteFileSourceMetaResponse.Builder response = RemoteFileSourceMetaResponse.newBuilder();

            if (content == null) {
                // Resource not found
                response.setFound(false);
                response.setContent(ByteString.EMPTY);
            } else {
                response.setFound(true);

                // Convert content to bytes using union type methods
                if (content.isString()) {
                    response.setContent(ByteString.copyFrom(content.asString(), StandardCharsets.UTF_8));
                } else if (content.isUint8Array()) {
                    response.setContent(ByteStringAccess.wrap(TypedArrayHelper.wrap(content.asUint8Array())));
                } else {
                    throw new IllegalArgumentException("Content must be a String, Uint8Array, or null");
                }
            }

            // Wrap in RemoteFileSourceClientMessage (client→server)
            RemoteFileSourceClientMessage clientRequest = RemoteFileSourceClientMessage.newBuilder()
                    .setRequestId(requestId)
                    .setMetaResponse(response)
                    .build();

            sendClientRequest(clientRequest);
        }
    }
}

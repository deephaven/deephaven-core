//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.remotefilesource;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.Uint8Array;
import elemental2.dom.DomGlobal;
import elemental2.dom.TextEncoder;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightDescriptor;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightInfo;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceClientRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceMetaRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceMetaResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourcePluginFetchRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceServerRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.SetExecutionContextRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.SetExecutionContextResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.JsProtobufUtils;
import io.deephaven.web.client.api.event.Event;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.deephaven.web.client.api.widget.JsWidget;
import io.deephaven.web.client.api.widget.WidgetMessageDetails;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;


/**
 * JavaScript client for the RemoteFileSource service. Provides bidirectional communication with the server-side
 * RemoteFileSourcePlugin via a message stream.
 * <p>
 * Events:
 * <ul>
 *   <li>{@link #EVENT_MESSAGE}: Fired for unrecognized messages from the server</li>
 *   <li>{@link #EVENT_REQUEST_SOURCE}: Fired when the server requests a resource from the client</li>
 * </ul>
 */
@JsType(namespace = "dh.remotefilesource", name = "RemoteFileSourceService")
public class JsRemoteFileSourceService extends HasEventHandling {
    /** Event name for generic messages from the server */
    public static final String EVENT_MESSAGE = "message";

    /** Event name for resource request events from the server */
    public static final String EVENT_REQUEST_SOURCE = "requestsource";

    // Plugin name must match RemoteFileSourcePlugin.name() on the server
    private static final String PLUGIN_NAME = "DeephavenRemoteFileSourcePlugin";

    // Timeout for setExecutionContext requests (in milliseconds)
    private static final int SET_EXECUTION_CONTEXT_TIMEOUT_MS = 30000; // 30 seconds

    private final JsWidget widget;

    // Track pending setExecutionContext requests
    private final Map<String, Promise.PromiseExecutorCallbackFn.ResolveCallbackFn<Boolean>> pendingSetExecutionContextRequests = new HashMap<>();
    private int requestIdCounter = 0;

    @JsIgnore
    private JsRemoteFileSourceService(JsWidget widget) {
        this.widget = widget;
    }

    /**
     * Fetches the FlightInfo for the plugin fetch command.
     *
     * @param connection the worker connection to use
     * @return a promise that resolves to the FlightInfo for the plugin fetch
     */
    @JsIgnore
    private static Promise<FlightInfo> fetchPluginFlightInfo(WorkerConnection connection) {
        // Create a new export ticket for the result
        Ticket resultTicket = connection.getTickets().newExportTicket();

        // Create the fetch request
        RemoteFileSourcePluginFetchRequest fetchRequest = new RemoteFileSourcePluginFetchRequest();
        fetchRequest.setResultId(resultTicket);
        fetchRequest.setPluginName(PLUGIN_NAME);

        // Serialize the request to bytes
        Uint8Array innerRequestBytes = fetchRequest.serializeBinary();

        // Wrap in google.protobuf.Any with the proper typeUrl
        Uint8Array anyWrappedBytes = JsProtobufUtils.wrapInAny(
                "type.googleapis.com/io.deephaven.proto.backplane.grpc.RemoteFileSourcePluginFetchRequest",
                innerRequestBytes);

        // Create a FlightDescriptor with the command
        FlightDescriptor descriptor = new FlightDescriptor();
        descriptor.setType(FlightDescriptor.DescriptorType.getCMD());
        descriptor.setCmd(anyWrappedBytes);

        // Send the getFlightInfo request
        return Callbacks.grpcUnaryPromise(c ->
                connection.flightServiceClient().getFlightInfo(descriptor, connection.metadata(), c::apply));
    }

    /**
     * Fetches a RemoteFileSource plugin instance from the server and establishes a message stream connection.
     *
     * @param connection the worker connection to use for communication
     * @return a promise that resolves to a RemoteFileSourceService instance with an active message stream
     */
    @JsIgnore
    public static Promise<JsRemoteFileSourceService> fetchPlugin(@TsTypeRef(Object.class) WorkerConnection connection) {
        return fetchPluginFlightInfo(connection)
                .then(flightInfo -> {
                    // The first endpoint contains the ticket for the plugin instance.
                    // This is the standard Flight pattern: we passed resultTicket in the request,
                    // the server exported the service to that ticket, and returned a FlightInfo
                    // with an endpoint containing that same ticket for us to use.
                    if (flightInfo.getEndpointList().length > 0) {
                        // Get the Arrow Flight ticket from the endpoint
                        io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.Ticket flightTicket =
                                flightInfo.getEndpointList().getAt(0).getTicket();

                        // Convert the Arrow Flight ticket to a Deephaven ticket
                        Ticket dhTicket = new Ticket();
                        dhTicket.setTicket(flightTicket.getTicket_asU8());

                        // Create a TypedTicket for the plugin instance
                        // The type must match RemoteFileSourcePlugin.name()
                        TypedTicket typedTicket = new TypedTicket();
                        typedTicket.setTicket(dhTicket);
                        typedTicket.setType(PLUGIN_NAME);

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
    @JsIgnore
    private Promise<JsRemoteFileSourceService> connect() {
        widget.addEventListener("message", this::handleMessage);
        return widget.refetch().then(w -> Promise.resolve(this));
    }

    /**
     * Handles incoming messages from the server.
     *
     * @param event the message event from the server
     */
    @JsIgnore
    private void handleMessage(Event<WidgetMessageDetails> event) {
        Uint8Array payload = event.getDetail().getDataAsU8();

        RemoteFileSourceServerRequest message;
        try {
            message = RemoteFileSourceServerRequest.deserializeBinary(payload);
        } catch (Exception e) {
            // Failed to parse as proto, fire generic message event
            handleUnknownMessage(event);
            return;
        }

        // Route the parsed message to the appropriate handler
        if (message.hasMetaRequest()) {
            handleMetaRequest(message);
        } else if (message.hasSetExecutionContextResponse()) {
            handleSetExecutionContextResponse(message);
        } else {
            handleUnknownMessage(event);
        }
    }

    /**
     * Handles a meta request (resource request) from the server.
     *
     * @param message the server request message
     */
    @JsIgnore
    private void handleMetaRequest(RemoteFileSourceServerRequest message) {
        RemoteFileSourceMetaRequest request = message.getMetaRequest();
        DomGlobal.setTimeout(ignore -> fireEvent(EVENT_REQUEST_SOURCE,
                new ResourceRequestEvent(message.getRequestId(), request)), 0);
    }

    /**
     * Handles a set execution context response from the server.
     *
     * @param message the server request message
     */
    @JsIgnore
    private void handleSetExecutionContextResponse(RemoteFileSourceServerRequest message) {
        String requestId = message.getRequestId();
        Promise.PromiseExecutorCallbackFn.ResolveCallbackFn<Boolean> resolveCallback =
                pendingSetExecutionContextRequests.remove(requestId);
        if (resolveCallback != null) {
            SetExecutionContextResponse response = message.getSetExecutionContextResponse();
            resolveCallback.onInvoke(response.getSuccess());
        }
    }

    /**
     * Handles an unknown or unparseable message from the server.
     *
     * @param event the message event
     */
    @JsIgnore
    private void handleUnknownMessage(Event<WidgetMessageDetails> event) {
        DomGlobal.setTimeout(ignore -> fireEvent(EVENT_MESSAGE, event.getDetail()), 0);
    }

    /**
     * Sets the execution context on the server to identify this message stream as active
     * for script execution.
     *
     * @param resourcePaths array of resource paths to resolve from remote source
     *                      (e.g., ["com/example/Test.groovy", "org/mycompany/Utils.groovy"]),
     *                      or null/empty for no specific resources
     * @return a promise that resolves to true if the server successfully set the execution context, false otherwise
     */
    @JsMethod
    public Promise<Boolean> setExecutionContext(@JsOptional String[] resourcePaths) {
        return new Promise<>((resolve, reject) -> {
            // Generate a unique request ID
            String requestId = "setExecutionContext-" + (requestIdCounter++);

            // Store the resolve callback to call when we get the acknowledgment
            pendingSetExecutionContextRequests.put(requestId, resolve);

            // Set a timeout to reject the promise if no response is received
            DomGlobal.setTimeout(ignore -> {
                Promise.PromiseExecutorCallbackFn.ResolveCallbackFn<Boolean> callback =
                        pendingSetExecutionContextRequests.remove(requestId);
                if (callback != null) {
                    // Request timed out - reject the promise
                    reject.onInvoke("setExecutionContext request timed out after "
                            + SET_EXECUTION_CONTEXT_TIMEOUT_MS + "ms");
                }
            }, SET_EXECUTION_CONTEXT_TIMEOUT_MS);

            RemoteFileSourceClientRequest clientRequest = getSetExecutionContextRequest(resourcePaths, requestId);
            sendClientRequest(clientRequest);
        });
    }

    /**
     * Helper method to build a RemoteFileSourceClientRequest for setting execution context.
     *
     * @param resourcePaths array of resource paths to resolve
     * @param requestId unique request ID
     * @return the constructed RemoteFileSourceClientRequest
     */
    private static @NotNull RemoteFileSourceClientRequest getSetExecutionContextRequest(String[] resourcePaths, String requestId) {
        SetExecutionContextRequest setContextRequest = new SetExecutionContextRequest();

        if (resourcePaths != null) {
            for (String resourcePath : resourcePaths) {
                setContextRequest.addResourcePaths(resourcePath);
            }
        }

        RemoteFileSourceClientRequest clientRequest = new RemoteFileSourceClientRequest();
        clientRequest.setRequestId(requestId);
        clientRequest.setSetExecutionContext(setContextRequest);
        return clientRequest;
    }

    /**
     * Helper method to send a RemoteFileSourceClientRequest to the server.
     *
     * @param clientRequest the client request to send
     */
    @JsIgnore
    private void sendClientRequest(RemoteFileSourceClientRequest clientRequest) {
        // Serialize the protobuf message to bytes
        Uint8Array messageBytes = clientRequest.serializeBinary();

        // Uint8Array is an ArrayBufferView, which is one of the MessageUnion types
        // The unchecked cast is safe because MessageUnion accepts String | ArrayBuffer | ArrayBufferView
        widget.sendMessage(Js.uncheckedCast(messageBytes), null);
    }

    /**
     * Closes the message stream connection to the server.
     */
    @JsMethod
    public void close() {
        widget.close();
    }

    /**
     * Event details for a resource request from the server. Wraps the proto RemoteFileSourceMetaRequest and provides a
     * respond() method.
     */
    @TsInterface
    @TsName(namespace = "dh.remotefilesource", name = "ResourceRequestEvent")
    public class ResourceRequestEvent {
        private final String requestId;
        private final RemoteFileSourceMetaRequest protoRequest;

        @JsIgnore
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
         * @param content the resource content as a String, Uint8Array, or null to indicate
         *                the resource was not found. If a String is provided, it will be
         *                UTF-8 encoded before being sent to the server. Uint8Array content
         *                is sent as-is.
         * @throws IllegalArgumentException if content is not a String, Uint8Array, or null
         */
        @JsMethod
        public void respond(@JsNullable Object content) {
            // Build RemoteFileSourceMetaResponse proto
            RemoteFileSourceMetaResponse response = new RemoteFileSourceMetaResponse();

            if (content == null) {
                // Resource not found
                response.setFound(false);
                response.setContent(new Uint8Array(0));
            } else {
                response.setFound(true);

                // Convert content to bytes
                if (content instanceof String) {
                    TextEncoder textEncoder = new TextEncoder();
                    response.setContent(textEncoder.encode((String) content));
                } else if (content instanceof Uint8Array) {
                    response.setContent((Uint8Array) content);
                } else {
                    throw new IllegalArgumentException("Content must be a String, Uint8Array, or null");
                }
            }

            // Wrap in RemoteFileSourceClientRequest (client→server)
            RemoteFileSourceClientRequest clientRequest = new RemoteFileSourceClientRequest();
            clientRequest.setRequestId(requestId);
            clientRequest.setMetaResponse(response);

            sendClientRequest(clientRequest);
        }
    }
}

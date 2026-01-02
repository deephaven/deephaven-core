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
 * RemoteFileSourceServicePlugin via a message stream.
 */
@JsType(namespace = "dh.remotefilesource", name = "RemoteFileSourceService")
public class JsRemoteFileSourceService extends HasEventHandling {
    public static final String EVENT_MESSAGE = "message";
    public static final String EVENT_REQUEST_SOURCE = "requestsource";

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
     * @param pluginName the name of the plugin to fetch
     * @return a promise that resolves to the FlightInfo for the plugin fetch
     */
    @JsIgnore
    private static Promise<FlightInfo> fetchPluginFlightInfo(WorkerConnection connection, String pluginName) {
        // Create a new export ticket for the result
        Ticket resultTicket = connection.getTickets().newExportTicket();

        // Create the fetch request
        RemoteFileSourcePluginFetchRequest fetchRequest = new RemoteFileSourcePluginFetchRequest();
        fetchRequest.setResultId(resultTicket);
        fetchRequest.setPluginName(pluginName);

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
        return Callbacks.<FlightInfo, Object>grpcUnaryPromise(
                c -> connection.flightServiceClient().getFlightInfo(descriptor, connection.metadata(), c::apply));
    }

    /**
     * Fetches a RemoteFileSource plugin instance from the server and establishes a message stream connection.
     *
     * @param connection the worker connection to use for communication
     * @return a promise that resolves to a RemoteFileSourceService instance with an active message stream
     */
    @JsIgnore
    public static Promise<JsRemoteFileSourceService> fetchPlugin(@TsTypeRef(Object.class) WorkerConnection connection) {
        String pluginName = "DeephavenRemoteFileSourcePlugin";
        return fetchPluginFlightInfo(connection, pluginName)
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
                        typedTicket.setType(pluginName);

                        JsWidget widget = new JsWidget(connection, typedTicket);

                        JsRemoteFileSourceService service = new JsRemoteFileSourceService(widget);
                        return service.connect();
                    } else {
                        return Promise.reject("No endpoints returned from RemoteFileSource plugin fetch");
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
        widget.addEventListener("message", (Event<WidgetMessageDetails> event) -> {
            // Parse the message as RemoteFileSourceServerRequest proto (server→client)
            Uint8Array payload = event.getDetail().getDataAsU8();

            try {
                RemoteFileSourceServerRequest message =
                        RemoteFileSourceServerRequest.deserializeBinary(payload);

                if (message.hasMetaRequest()) {
                    // If server has requested a resource from the client, fire request event
                    RemoteFileSourceMetaRequest request = message.getMetaRequest();

                    DomGlobal.setTimeout(ignore -> fireEvent(EVENT_REQUEST_SOURCE,
                            new ResourceRequestEvent(message.getRequestId(), request)), 0);
                } else if (message.hasSetExecutionContextResponse()) {
                    // Server acknowledged execution context was set
                    String requestId = message.getRequestId();
                    Promise.PromiseExecutorCallbackFn.ResolveCallbackFn<Boolean> resolveCallback =
                            pendingSetExecutionContextRequests.remove(requestId);
                    if (resolveCallback != null) {
                        SetExecutionContextResponse response = message.getSetExecutionContextResponse();
                        resolveCallback.onInvoke(response.getSuccess());
                    }
                } else {
                    // Unknown message type
                    DomGlobal.setTimeout(ignore -> fireEvent(EVENT_MESSAGE, event.getDetail()), 0);
                }
            } catch (Exception e) {
                // Failed to parse as proto, fire generic message event
                DomGlobal.setTimeout(ignore -> fireEvent(EVENT_MESSAGE, event.getDetail()), 0);
            }
        });

        return widget.refetch().then(w -> Promise.resolve(this));
    }

    /**
     * Sets the execution context on the server to identify this message stream as active
     * for script execution.
     *
     * @param resourcePaths array of resource paths to resolve from remote source (e.g., ["com/example/Test.groovy", "org/mycompany/Utils.groovy"])
     * @return a promise that resolves to true if the server successfully set the execution context, false otherwise
     */
    @JsMethod
    public Promise<Boolean> setExecutionContext(@JsOptional String[] resourcePaths) {
        return new Promise<>((resolve, reject) -> {
            // Generate a unique request ID
            String requestId = "setExecutionContext-" + (requestIdCounter++);

            // Store the resolve callback to call when we get the acknowledgment
            pendingSetExecutionContextRequests.put(requestId, resolve);

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

        // Send as Uint8Array (which is an ArrayBufferView, compatible with MessageUnion)
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
         * @param content the resource content (string or Uint8Array), or null if not found
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

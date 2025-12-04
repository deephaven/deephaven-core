package io.deephaven.web.client.api.remotefilesource;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.Uint8Array;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightDescriptor;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightInfo;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb.ClientData;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb.ConnectRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb.StreamRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.object_pb.StreamResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceClientRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceMetaRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceMetaResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourcePluginFetchRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourceServerRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.event.HasEventHandling;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;

import java.util.function.Supplier;

/**
 * JavaScript client for the RemoteFileSource service. Provides bidirectional communication with the server-side
 * RemoteFileSourceServicePlugin via a message stream.
 */
@TsInterface
@TsName(namespace = "dh.remotefilesource", name = "RemoteFileSourceService")
public class JsRemoteFileSourceService extends HasEventHandling {
    @JsProperty(namespace = "dh.remotefilesource.RemoteFileSourceService")
    public static final String EVENT_MESSAGE = "message";
    @JsProperty(namespace = "dh.remotefilesource.RemoteFileSourceService")
    public static final String EVENT_CLOSE = "close";
    @JsProperty(namespace = "dh.remotefilesource.RemoteFileSourceService")
    public static final String EVENT_REQUEST = "request";

    private final TypedTicket typedTicket;

    private final Supplier<BiDiStream<StreamRequest, StreamResponse>> streamFactory;
    private BiDiStream<StreamRequest, StreamResponse> messageStream;

    private boolean hasFetched;

    @JsIgnore
    private JsRemoteFileSourceService(WorkerConnection connection, TypedTicket typedTicket) {
        this.typedTicket = typedTicket;
        this.hasFetched = false;

        // Set up the message stream factory
        BiDiStream.Factory<StreamRequest, StreamResponse> factory = connection.streamFactory();
        this.streamFactory = () -> factory.create(
                connection.objectServiceClient()::messageStream,
                (first, headers) -> connection.objectServiceClient().openMessageStream(first, headers),
                (next, headers, c) -> connection.objectServiceClient().nextMessageStream(next, headers, c::apply),
                new StreamRequest());
    }

    /**
     * Fetches a RemoteFileSource plugin instance from the server and establishes a message stream connection.
     *
     * @param connection the worker connection to use for communication
     * @param clientSessionId optional unique identifier for this client session
     * @return a promise that resolves to a RemoteFileSourceService instance with an active message stream
     */
    @JsMethod
    public static Promise<JsRemoteFileSourceService> fetchPlugin(WorkerConnection connection, String clientSessionId) {
        // Create a new export ticket for the result
        Ticket resultTicket = connection.getTickets().newExportTicket();

        // Create the fetch request
        RemoteFileSourcePluginFetchRequest fetchRequest = new RemoteFileSourcePluginFetchRequest();
        fetchRequest.setResultId(resultTicket);
        if (clientSessionId != null && !clientSessionId.isEmpty()) {
            fetchRequest.setClientSessionId(clientSessionId);
        }

        // Serialize the request to bytes
        Uint8Array innerRequestBytes = fetchRequest.serializeBinary();

        // Wrap in google.protobuf.Any with the proper typeUrl
        Uint8Array anyWrappedBytes = wrapInAny(
            "type.googleapis.com/io.deephaven.proto.backplane.grpc.RemoteFileSourcePluginFetchRequest",
            innerRequestBytes
        );

        // Create a FlightDescriptor with the command
        FlightDescriptor descriptor = new FlightDescriptor();
        descriptor.setType(FlightDescriptor.DescriptorType.getCMD());
        descriptor.setCmd(anyWrappedBytes);

        // Send the getFlightInfo request
        return Callbacks.<FlightInfo, Object>grpcUnaryPromise(c ->
            connection.flightServiceClient().getFlightInfo(descriptor, connection.metadata(), c::apply)
        ).then(flightInfo -> {
            // The first endpoint should contain the ticket for the plugin instance
            if (flightInfo.getEndpointList().length > 0) {
                // Get the Arrow Flight ticket from the endpoint
                io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.Ticket flightTicket =
                    flightInfo.getEndpointList().getAt(0).getTicket();

                // Convert the Arrow Flight ticket to a Deephaven ticket
                Ticket dhTicket = new Ticket();
                dhTicket.setTicket(flightTicket.getTicket_asU8());

                // Create a TypedTicket for the plugin instance
                TypedTicket typedTicket = new TypedTicket();
                typedTicket.setTicket(dhTicket);
                typedTicket.setType("RemoteFileSourceService");

                // Create a new service instance with the typed ticket and connect to it
                JsRemoteFileSourceService service = new JsRemoteFileSourceService(connection, typedTicket);
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
        if (messageStream != null) {
            messageStream.end();
        }

        return new Promise<>((resolve, reject) -> {
            messageStream = streamFactory.get();

            messageStream.onData(res -> {
                if (!hasFetched) {
                    hasFetched = true;
                    resolve.onInvoke(this);
                } else {
                    // Parse the message as RemoteFileSourceServerRequest proto (server→client)
                    Uint8Array payload = res.getData().getPayload_asU8();

                    try {
                        RemoteFileSourceServerRequest message = RemoteFileSourceServerRequest.deserializeBinary(payload);

                        // Check which message type it is
                        if (message.hasMetaRequest()) {
                            // Server is requesting a resource from the client
                            RemoteFileSourceMetaRequest request = message.getMetaRequest();

                            // Fire request event (include request_id from wrapper)
                            DomGlobal.setTimeout(ignore ->
                                fireEvent(EVENT_REQUEST, new ResourceRequestEvent(message.getRequestId(), request)), 0);
                        } else {
                            // Unknown message type
                            DomGlobal.setTimeout(ignore ->
                                fireEvent(EVENT_MESSAGE, res.getData()), 0);
                        }
                    } catch (Exception e) {
                        // Failed to parse as proto, fire generic message event
                        DomGlobal.setTimeout(ignore ->
                            fireEvent(EVENT_MESSAGE, res.getData()), 0);
                    }
                }
            });

            messageStream.onStatus(status -> {
                if (!status.isOk()) {
                    reject.onInvoke(status.getDetails());
                }
                DomGlobal.setTimeout(ignore ->
                    fireEvent(EVENT_CLOSE), 0);
                closeStream();
            });

            messageStream.onEnd(status ->
                closeStream());

            // First message establishes a connection w/ the plugin object instance we're talking to
            StreamRequest req = new StreamRequest();
            ConnectRequest data = new ConnectRequest();
            data.setSourceId(typedTicket);
            req.setConnect(data);
            messageStream.send(req);
        });
    }

    /**
     * Test method to verify bidirectional communication.
     * Sends a test command to the server, which will request a resource back from the client.
     *
     * @param resourceName the resource name to use for the test (e.g., "com/example/Test.java")
     */
    @JsMethod
    public void testBidirectionalCommunication(String resourceName) {
        RemoteFileSourceClientRequest clientRequest = new RemoteFileSourceClientRequest();
        clientRequest.setRequestId(""); // Empty request_id for test commands
        clientRequest.setTestCommand("TEST:" + resourceName);
        sendClientRequest(clientRequest);
    }

    /**
     * Helper method to send a RemoteFileSourceClientRequest to the server.
     *
     * @param clientRequest the client request to send
     */
    @JsIgnore
    private void sendClientRequest(RemoteFileSourceClientRequest clientRequest) {
        if (messageStream == null) {
            throw new IllegalStateException("Message stream not connected");
        }

        StreamRequest req = new StreamRequest();
        ClientData clientData = new ClientData();
        clientData.setPayload(clientRequest.serializeBinary());
        req.setData(clientData);
        messageStream.send(req);
    }

    /**
     * Closes the message stream connection to the server.
     */
    @JsMethod
    public void close() {
        closeStream();
    }

    @JsIgnore
    private void closeStream() {
        if (messageStream != null) {
            messageStream.end();
            messageStream = null;
        }
    }

    /**
     * Event details for a resource request from the server.
     * Wraps the proto RemoteFileSourceMetaRequest and provides a respond() method.
     */
    @TsInterface
    @TsName(namespace = "dh.remotefilesource", name = "ResourceRequest")
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
         * @param content the resource content (string, ArrayBuffer, or typed array), or null if not found
         */
        @JsMethod
        public void respond(Object content) {
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
                    response.setContent(stringToUtf8((String) content));
                } else if (content instanceof Uint8Array) {
                    response.setContent((Uint8Array) content);
                } else {
                    response.setContent(Js.uncheckedCast(content));
                }
            }

            // Wrap in RemoteFileSourceClientRequest (client→server)
            RemoteFileSourceClientRequest clientRequest = new RemoteFileSourceClientRequest();
            clientRequest.setRequestId(requestId);
            clientRequest.setMetaResponse(response);

            sendClientRequest(clientRequest);
        }
    }

    /**
     * Wraps a protobuf message in a google.protobuf.Any message.
     *
     * @param typeUrl the type URL for the message (e.g., "type.googleapis.com/package.MessageName")
     * @param messageBytes the serialized protobuf message bytes
     * @return the serialized Any message containing the wrapped message
     */
    private static Uint8Array wrapInAny(String typeUrl, Uint8Array messageBytes) {
        // Encode the type_url string to UTF-8 bytes
        Uint8Array typeUrlBytes = stringToUtf8(typeUrl);

        // Calculate sizes for protobuf encoding
        // Field 1 (type_url): tag + length + data
        int typeUrlTag = (1 << 3) | 2; // field 1, wire type 2 (length-delimited)
        int typeUrlFieldSize = sizeOfVarint(typeUrlTag) + sizeOfVarint(typeUrlBytes.length) + typeUrlBytes.length;

        // Field 2 (value): tag + length + data
        int valueTag = (2 << 3) | 2; // field 2, wire type 2 (length-delimited)
        int valueFieldSize = sizeOfVarint(valueTag) + sizeOfVarint(messageBytes.length) + messageBytes.length;

        int totalSize = typeUrlFieldSize + valueFieldSize;
        Uint8Array result = new Uint8Array(totalSize);
        int pos = 0;

        // Write type_url field
        pos = writeVarint(result, pos, typeUrlTag);
        pos = writeVarint(result, pos, typeUrlBytes.length);
        for (int i = 0; i < typeUrlBytes.length; i++) {
            result.setAt(pos++, typeUrlBytes.getAt(i));
        }

        // Write value field
        pos = writeVarint(result, pos, valueTag);
        pos = writeVarint(result, pos, messageBytes.length);
        for (int i = 0; i < messageBytes.length; i++) {
            result.setAt(pos++, messageBytes.getAt(i));
        }

        return result;
    }

    private static Uint8Array stringToUtf8(String str) {
        // Simple UTF-8 encoding for ASCII-compatible strings
        Uint8Array bytes = new Uint8Array(str.length());
        for (int i = 0; i < str.length(); i++) {
            bytes.setAt(i, (double) str.charAt(i));
        }
        return bytes;
    }

    private static int sizeOfVarint(int value) {
        if (value < 0) return 10;
        if (value < 128) return 1;
        if (value < 16384) return 2;
        if (value < 2097152) return 3;
        if (value < 268435456) return 4;
        return 5;
    }

    private static int writeVarint(Uint8Array buffer, int pos, int value) {
        while (value >= 128) {
            buffer.setAt(pos++, (double) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buffer.setAt(pos++, (double) value);
        return pos;
    }
}


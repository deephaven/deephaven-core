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
        Uint8Array anyWrappedBytes = wrapInAny(
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
        String pluginName = "DeephavenGroovyRemoteFileSourcePlugin";
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

    /**
     * Calculates the total size needed for a protobuf length-delimited field.
     * <p>
     * A length-delimited field consists of:
     * <ul>
     *   <li>Tag (field number + wire type) encoded as a varint</li>
     *   <li>Length of the data encoded as a varint</li>
     *   <li>The actual data bytes</li>
     * </ul>
     *
     * @param tag the protobuf field tag (field number << 3 | wire type)
     * @param dataLength the length of the data in bytes
     * @return the total number of bytes needed for this field
     */
    private static int calculateFieldSize(int tag, int dataLength) {
        return sizeOfVarint(tag) + sizeOfVarint(dataLength) + dataLength;
    }

    /**
     * Calculates how many bytes a varint encoding will require for the given value.
     * <p>
     * Protobuf uses varint encoding where each byte stores 7 bits of data (the 8th bit is
     * a continuation flag). This means:
     * <ul>
     *   <li>1 byte: 0 to 127 (2^7 - 1)</li>
     *   <li>2 bytes: 128 to 16,383 (2^14 - 1)</li>
     *   <li>3 bytes: 16,384 to 2,097,151 (2^21 - 1)</li>
     *   <li>4 bytes: 2,097,152 to 268,435,455 (2^28 - 1)</li>
     *   <li>5 bytes: 268,435,456 to 4,294,967,295 (2^35 - 1, max unsigned 32-bit)</li>
     *   <li>10 bytes: negative numbers (due to sign extension)</li>
     * </ul>
     *
     * @param value the integer value to encode
     * @return the number of bytes required to encode the value as a varint
     */
    private static int sizeOfVarint(int value) {
        if (value < 0)
            return 10;          // Negative numbers use sign extension, always 10 bytes
        if (value < 128)        // 2^7
            return 1;
        if (value < 16384)      // 2^14
            return 2;
        if (value < 2097152)    // 2^21
            return 3;
        if (value < 268435456)  // 2^28
            return 4;
        return 5;               // 2^35 (max for positive 32-bit int)
    }

    /**
     * Wraps a protobuf message in a google.protobuf.Any message.
     * <p>
     * The google.protobuf.Any message has two fields:
     * <ul>
     *   <li>Field 1: type_url (string) - identifies the type of message contained</li>
     *   <li>Field 2: value (bytes) - the actual serialized message</li>
     * </ul>
     * <p>
     * This method manually encodes the Any message in protobuf binary format since the client-side
     * JavaScript protobuf library doesn't provide Any.pack() like the server-side Java library does.
     *
     * @param typeUrl the type URL for the message (e.g., "type.googleapis.com/package.MessageName")
     * @param messageBytes the serialized protobuf message bytes
     * @return the serialized Any message containing the wrapped message
     */
    private static Uint8Array wrapInAny(String typeUrl, Uint8Array messageBytes) {
        // Protobuf tag constants for google.protobuf.Any message fields
        // Tag format: (field_number << 3) | wire_type
        // wire_type=2 means length-delimited (for strings/bytes)
        final int TYPE_URL_TAG = 10;  // (1 << 3) | 2 = field 1, wire type 2
        final int VALUE_TAG = 18;     // (2 << 3) | 2 = field 2, wire type 2

        // Encode the type_url string to UTF-8 bytes
        TextEncoder textEncoder = new TextEncoder();
        Uint8Array typeUrlBytes = textEncoder.encode(typeUrl);

        // Calculate sizes for protobuf binary encoding
        int typeUrlFieldSize = calculateFieldSize(TYPE_URL_TAG, typeUrlBytes.length);
        int valueFieldSize = calculateFieldSize(VALUE_TAG, messageBytes.length);

        // Allocate buffer for the complete Any message
        int totalSize = typeUrlFieldSize + valueFieldSize;
        Uint8Array result = new Uint8Array(totalSize);
        int pos = 0;

        // Write field 1 (type_url) in protobuf binary format
        pos = writeField(result, pos, TYPE_URL_TAG, typeUrlBytes);

        // Write field 2 (value) in protobuf binary format
        writeField(result, pos, VALUE_TAG, messageBytes);

        return result;
    }

    /**
     * Writes a complete protobuf length-delimited field to the buffer.
     * <p>
     * A length-delimited field consists of:
     * <ul>
     *   <li>Tag (field number + wire type) encoded as a varint</li>
     *   <li>Length of the data encoded as a varint</li>
     *   <li>The actual data bytes</li>
     * </ul>
     *
     * @param buffer the buffer to write to
     * @param pos the starting position in the buffer
     * @param tag the protobuf field tag
     * @param data the data bytes to write
     * @return the new position after writing the complete field
     */
    private static int writeField(Uint8Array buffer, int pos, int tag, Uint8Array data) {
        // Write tag and length
        pos = writeVarint(buffer, pos, tag);
        pos = writeVarint(buffer, pos, data.length);
        // Write data bytes
        for (int i = 0; i < data.length; i++) {
            buffer.setAt(pos++, data.getAt(i));
        }
        return pos;
    }


    /**
     * Writes a value to the buffer as a protobuf varint (variable-length integer).
     * <p>
     * Varint encoding works by:
     * <ol>
     *   <li>Taking the lowest 7 bits of the value</li>
     *   <li>Setting the 8th bit to 1 if more bytes follow (continuation flag)</li>
     *   <li>Writing the byte to the buffer</li>
     *   <li>Shifting the value right by 7 bits</li>
     *   <li>Repeating until the value is less than 128</li>
     *   <li>Writing the final byte without the continuation flag (8th bit = 0)</li>
     * </ol>
     * <p>
     * Example: encoding 300
     * <ul>
     *   <li>300 in binary: 100101100</li>
     *   <li>First byte: (300 & 0x7F) | 0x80 = 0b00101100 | 0b10000000 = 172 (0xAC)</li>
     *   <li>Shift: 300 >>> 7 = 2</li>
     *   <li>Second byte: 2 (no continuation flag)</li>
     *   <li>Result: [172, 2]</li>
     * </ul>
     *
     * @param buffer the buffer to write to
     * @param pos the starting position in the buffer
     * @param value the value to encode
     * @return the new position after writing
     */
    private static int writeVarint(Uint8Array buffer, int pos, int value) {
        while (value >= 128) {
            // Extract lowest 7 bits and set continuation flag (8th bit = 1)
            buffer.setAt(pos++, (double) ((value & 0x7F) | 0x80));
            // Shift right by 7 to process next chunk
            value >>>= 7;  // Unsigned right shift to handle large positive values
        }
        // Write final byte (no continuation flag, 8th bit = 0)
        buffer.setAt(pos++, (double) value);
        return pos;
    }
}

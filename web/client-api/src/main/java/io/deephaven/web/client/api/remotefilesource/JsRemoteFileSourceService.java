package io.deephaven.web.client.api.remotefilesource;

import elemental2.core.Uint8Array;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightDescriptor;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightInfo;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb.RemoteFileSourcePluginFetchRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.ServerObject;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.deephaven.web.client.api.widget.JsWidgetExportedObject;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsType;

/**
 * JavaScript client for the RemoteFileSource service.
 */
@JsType(namespace = "dh.remotefilesource", name = "RemoteFileSourceService")
public class JsRemoteFileSourceService extends HasEventHandling {
    private final WorkerConnection connection;

    @JsIgnore
    public JsRemoteFileSourceService(WorkerConnection connection) {
        this.connection = connection;
    }

    /**
     * Fetches a RemoteFileSource plugin instance from the server.
     *
     * @return a promise that resolves to a ServerObject representing the RemoteFileSource plugin instance
     */
    @JsMethod
    public Promise<ServerObject> fetchPlugin() {
        // Create a new export ticket for the result
        Ticket resultTicket = connection.getTickets().newExportTicket();

        // Create the fetch request
        RemoteFileSourcePluginFetchRequest fetchRequest = new RemoteFileSourcePluginFetchRequest();
        fetchRequest.setResultId(resultTicket);

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

                // Return a ServerObject wrapper
                return Promise.resolve(new JsWidgetExportedObject(connection, typedTicket));
            } else {
                return Promise.reject("No endpoints returned from RemoteFileSource plugin fetch");
            }
        });
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

//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.remotefilesource;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.PluginMarker;
import io.deephaven.proto.backplane.grpc.RemoteFileSourcePluginFetchRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.CommandResolver;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.server.session.WantsTicketRouter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class RemoteFileSourceCommandResolver implements CommandResolver, WantsTicketRouter {
    private static final Logger log = LoggerFactory.getLogger(RemoteFileSourceCommandResolver.class);

    private static final String FETCH_PLUGIN_TYPE_URL =
            "type.googleapis.com/" + RemoteFileSourcePluginFetchRequest.getDescriptor().getFullName();

    /**
     * Parses a RemoteFileSourcePluginFetchRequest from the given Any command.
     *
     * @param command the Any command containing the fetch request
     * @return the parsed RemoteFileSourcePluginFetchRequest
     * @throws IllegalArgumentException if the command type URL doesn't match the expected fetch plugin type
     * @throws UncheckedDeephavenException if the command cannot be parsed as a RemoteFileSourcePluginFetchRequest
     */
    private static RemoteFileSourcePluginFetchRequest parseFetchRequest(final Any command) {
        if (!FETCH_PLUGIN_TYPE_URL.equals(command.getTypeUrl())) {
            throw new IllegalArgumentException("Not a valid remotefilesource command: " + command.getTypeUrl());
        }

        try {
            return RemoteFileSourcePluginFetchRequest.parseFrom(command.getValue());
        } catch (InvalidProtocolBufferException e) {
            throw new UncheckedDeephavenException("Could not parse RemoteFileSourcePluginFetchRequest", e);
        }
    }

    /**
     * Attempts to parse ByteString data as a protobuf Any message.
     * Returns null if parsing fails rather than throwing an exception, allowing callers to handle
     * invalid data gracefully.
     *
     * @param data the ByteString data to parse
     * @return the parsed Any message, or null if parsing fails
     */
    private static Any parseOrNull(final ByteString data) {
        try {
            return Any.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            return null;
        }
    }

    /**
     * Exports a PluginMarker singleton based on the fetch request.
     * The marker object is exported to the session using the result ticket specified in the request,
     * and flight info is returned containing the endpoint for accessing it.
     *
     * Note: This exports PluginMarker.INSTANCE as a trusted marker. Plugin-specific routing
     * is handled by TypedTicket.type in the ConnectRequest phase, which is validated against
     * the plugin's name() method.
     *
     * @param session the session state for the current request
     * @param descriptor the flight descriptor containing the command
     * @param request the parsed RemoteFileSourcePluginFetchRequest containing the result ticket
     * @return a FlightInfo export object containing the plugin endpoint information
     * @throws StatusRuntimeException if the request doesn't contain a valid result ID ticket
     */
    private static SessionState.ExportObject<Flight.FlightInfo> fetchPlugin(@Nullable final SessionState session,
                                                                    final Flight.FlightDescriptor descriptor,
                                                                    final RemoteFileSourcePluginFetchRequest request) {
        final Ticket resultTicket = request.getResultId();
        final boolean hasResultId = !resultTicket.getTicket().isEmpty();
        if (!hasResultId) {
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT
                    .withDescription("RemoteFileSourcePluginFetchRequest must contain a valid result_id"));
        }

        final String pluginType = request.getPluginType();
        if (pluginType == null || pluginType.isEmpty()) {
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT
                    .withDescription("RemoteFileSourcePluginFetchRequest must contain a valid plugin_type"));
        }

        final SessionState.ExportBuilder<Object> markerExportBuilder =
                session.newExport(resultTicket, "RemoteFileSourcePluginFetchRequest.resultTicket");
//        markerExportBuilder.require();

        // Get singleton marker for this plugin type
        // This ensures isType() routing works correctly when multiple plugins use PluginMarker
        final SessionState.ExportObject<Object> markerExport =
                markerExportBuilder.submit(() -> PluginMarker.forPluginType(pluginType));

        final Flight.FlightInfo flightInfo = Flight.FlightInfo.newBuilder()
                .setFlightDescriptor(descriptor)
                .addEndpoint(Flight.FlightEndpoint.newBuilder()
                        .setTicket(Flight.Ticket.newBuilder()
                                .setTicket(
                                        resultTicket.getTicket()))
                        .build())
                .setTotalRecords(-1)
                .setTotalBytes(-1)
                .build();
        return SessionState.wrapAsExport(flightInfo);
    }

    /**
     * Resolves a flight descriptor to flight info for remote file source commands.
     * Handles RemoteFileSourcePluginFetchRequest commands by parsing the descriptor and delegating to the
     * appropriate handler method.
     *
     * @param session the session state for the current request
     * @param descriptor the flight descriptor containing the command
     * @param logId the log identifier for tracking
     * @return a FlightInfo export object for the requested command
     * @throws StatusRuntimeException if session is null (UNAUTHENTICATED), the command cannot be parsed,
     *                                or the command type URL is not recognized
     */
    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(@Nullable final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId) {
        if (session == null) {
            throw new StatusRuntimeException(Status.UNAUTHENTICATED);
        }

        final Any request = parseOrNull(descriptor.getCmd());
        if (request == null) {
            log.error().append("Could not parse remotefilesource command.").endl();
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Could not parse remotefilesource command Any.");
        }

        if (FETCH_PLUGIN_TYPE_URL.equals(request.getTypeUrl())) {
            return fetchPlugin(session, descriptor, parseFetchRequest(request));
        }

        log.error().append("Invalid pivot command typeUrl: " + request.getTypeUrl()).endl();
        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Invalid typeUrl: " + request.getTypeUrl());
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        // nothing to do
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        // no tickets
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean handlesCommand(final Flight.FlightDescriptor descriptor) {
        // If not CMD, there is an error with io.deephaven.server.session.TicketRouter.getPathResolver / handlesPath
        Assert.eq(descriptor.getType(), "descriptor.getType()", Flight.FlightDescriptor.DescriptorType.CMD, "CMD");

        // No good way to check if this is a valid command without parsing to Any first.
        final Any command = parseOrNull(descriptor.getCmd());
        if (command == null) {
            return false;
        }

        // Check if the command matches any types that this resolver handles.
        return FETCH_PLUGIN_TYPE_URL.equals(command.getTypeUrl());
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        // no publishing
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(final SessionState session,
            final Flight.FlightDescriptor descriptor, final String logId,
            @Nullable final Runnable onPublish) {
        // no publishing
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(@Nullable final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId) {
        // use flightInfoFor() instead of resolve() for descriptor handling
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(@Nullable final SessionState session,
            final ByteBuffer ticket,
            final String logId) {
        // no tickets
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTicketRouter(TicketRouter ticketRouter) {
        // not needed
    }

    @Override
    public byte ticketRoute() {
        return 0;
    }
}

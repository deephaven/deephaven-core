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
    private static Any parseAsAnyOrNull(final ByteString data) {
        try {
            return Any.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            return null;
        }
    }

    /**
     * Exports the PluginMarker singleton based on the fetch request.
     * The marker object is exported to the session using the result ticket specified in the request,
     * and flight info is returned containing the endpoint for accessing it.
     *
     * <p>Note: This exports a PluginMarker for the specified plugin name. Plugin-specific routing
     * is handled by TypedTicket.type in the ConnectRequest phase, which is validated against
     * the plugin's name() method.
     *
     * @param session the session state for the current request
     * @param descriptor the flight descriptor containing the command
     * @param request the parsed RemoteFileSourcePluginFetchRequest containing the result ticket
     * @return a FlightInfo export object containing the plugin endpoint information
     * @throws StatusRuntimeException if the request doesn't contain a valid result ID ticket or plugin name
     */
    private static SessionState.ExportObject<Flight.FlightInfo> fetchPlugin(@Nullable final SessionState session,
                                                                    final Flight.FlightDescriptor descriptor,
                                                                    final RemoteFileSourcePluginFetchRequest request) {
        final Ticket resultTicket = request.getResultId();
        final boolean hasResultId = !resultTicket.getTicket().isEmpty();
        if (!hasResultId) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "RemoteFileSourcePluginFetchRequest must contain a valid result_id");
        }

        final String pluginName = request.getPluginName();
        if (pluginName.isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "RemoteFileSourcePluginFetchRequest must contain a valid plugin_name");
        }

        // Export a plugin-specific PluginMarker. Plugins using PluginMarker should check
        // marker.getPluginName() in isType() to prevent conflicts when multiple plugins share PluginMarker.
        session.newExport(resultTicket, "RemoteFileSourcePluginFetchRequest.resultTicket")
                .submit(() -> PluginMarker.forPluginName(pluginName));

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
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED,
                    "Could not resolve '" + logId + "': no session available");
        }

        final Any request = parseAsAnyOrNull(descriptor.getCmd());
        if (request == null) {
            log.error().append("Could not parse remotefilesource command.").endl();
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Could not parse remotefilesource command Any.");
        }

        if (!FETCH_PLUGIN_TYPE_URL.equals(request.getTypeUrl())) {
            log.error().append("Invalid remotefilesource command typeUrl: " + request.getTypeUrl()).endl();
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Invalid typeUrl: " + request.getTypeUrl());
        }

        return fetchPlugin(session, descriptor, parseFetchRequest(request));
    }

    /**
     * Visits all flight info that this resolver exposes.
     *
     * <p><b>Not implemented:</b> This resolver does not expose any flight info via list flights.
     */
    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        // nothing to do
    }

    /**
     * Returns a log-friendly name for the given ticket.
     *
     * @throws UnsupportedOperationException always, as this resolver does not support ticket-based routing
     */
    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        // no ticket-based routing
        throw new UnsupportedOperationException();
    }

    /**
     * Determines if this resolver is responsible for handling the given command descriptor.
     * This resolver handles commands with type URL matching RemoteFileSourcePluginFetchRequest.
     *
     * @param descriptor the flight descriptor containing the command
     * @return true if this resolver handles the command, false otherwise
     */
    @Override
    public boolean handlesCommand(final Flight.FlightDescriptor descriptor) {
        Assert.eq(descriptor.getType(), "descriptor.getType()", Flight.FlightDescriptor.DescriptorType.CMD, "CMD");

        final Any command = parseAsAnyOrNull(descriptor.getCmd());
        if (command == null) {
            return false;
        }

        return FETCH_PLUGIN_TYPE_URL.equals(command.getTypeUrl());
    }

    /**
     * Publishes an export to the session using a ticket.
     *
     * @throws UnsupportedOperationException always, as this resolver does not support publishing
     */
    @Override
    public <T> SessionState.ExportBuilder<T> publish(final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        throw new UnsupportedOperationException();
    }

    /**
     * Publishes an export to the session using a flight descriptor.
     *
     * @throws UnsupportedOperationException always, as this resolver does not support publishing
     */
    @Override
    public <T> SessionState.ExportBuilder<T> publish(final SessionState session,
            final Flight.FlightDescriptor descriptor, final String logId,
            @Nullable final Runnable onPublish) {
        throw new UnsupportedOperationException();
    }

    /**
     * Resolves a flight descriptor to an export object.
     *
     * @throws UnsupportedOperationException always, use flightInfoFor() instead
     */
    @Override
    public <T> SessionState.ExportObject<T> resolve(@Nullable final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId) {
        throw new UnsupportedOperationException();
    }

    /**
     * Resolves a ticket to an export object.
     *
     * @throws UnsupportedOperationException always, as this resolver does not support ticket-based routing
     */
    @Override
    public <T> SessionState.ExportObject<T> resolve(@Nullable final SessionState session,
            final ByteBuffer ticket,
            final String logId) {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets the ticket router for this resolver.
     *
     * <p><b>Not implemented:</b> This resolver does not need access to the ticket router.
     */
    @Override
    public void setTicketRouter(TicketRouter ticketRouter) {
        // not needed
    }

    /**
     * Returns the ticket route byte for this resolver.
     * This resolver does not use ticket-based routing, so returns 0.
     *
     * @return 0, indicating no ticket routing
     */
    @Override
    public byte ticketRoute() {
        return 0;
    }
}

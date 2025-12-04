package io.deephaven.remotefilesource;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
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


    private static RemoteFileSourcePluginFetchRequest parseFetchRequest(final Any command) {
        if (!FETCH_PLUGIN_TYPE_URL.equals(command.getTypeUrl())) {
            throw new IllegalArgumentException("Not a valid remotefilesource command: " + command.getTypeUrl());
        }

        final ByteString bytes = command.getValue();
        final RemoteFileSourcePluginFetchRequest request;
        try {
            request = RemoteFileSourcePluginFetchRequest.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new UncheckedDeephavenException("Could not parse RemoteFileSourcePluginFetchRequest", e);
        }
        return request;
    }

    private static Any parseOrNull(final ByteString data) {
        try {
            return Any.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            return null;
        }
    }

    public SessionState.ExportObject<Flight.FlightInfo> fetchPlugin(@Nullable final SessionState session,
                                                                    final Flight.FlightDescriptor descriptor,
                                                                    final RemoteFileSourcePluginFetchRequest request) {
        final Ticket resultTicket = request.getResultId();
        final boolean hasResultId = !resultTicket.getTicket().isEmpty();
        if (!hasResultId) {
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
        }

        // Extract optional client session ID from the request (empty string means not provided)
        final String clientSessionId = request.getClientSessionId();

        final SessionState.ExportBuilder<RemoteFileSourceServicePlugin> pluginExportBuilder =
                session.newExport(resultTicket, "RemoteFileSourcePluginFetchRequest.resultTicket");
        pluginExportBuilder.require();

        final SessionState.ExportObject<RemoteFileSourceServicePlugin> pluginExport =
                pluginExportBuilder.submit(() -> new RemoteFileSourceServicePlugin(
                        clientSessionId.isEmpty() ? null : clientSessionId));

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
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Could not parse remotefilesource command Any.");
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
        return null;
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

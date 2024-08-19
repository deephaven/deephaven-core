//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import com.google.common.collect.MapMaker;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.flight.util.FlightExportTicketHelper;
import io.deephaven.proto.flight.util.TicketRouterHelper;
import io.deephaven.proto.util.ByteHelper;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.proto.util.SharedTicketHelper;
import io.deephaven.server.auth.AuthorizationProvider;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static io.deephaven.proto.util.SharedTicketHelper.FLIGHT_DESCRIPTOR_ROUTE;
import static io.deephaven.proto.util.SharedTicketHelper.TICKET_PREFIX;

@Singleton
public class SharedTicketResolver extends TicketResolverBase {

    private final ConcurrentMap<ByteString, SessionState.ExportObject<?>> sharedVariables = new MapMaker()
            .weakValues()
            .makeMap();

    @Inject
    public SharedTicketResolver(
            final AuthorizationProvider authProvider) {
        super(authProvider, (byte) TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
    }

    @Override
    public String getLogNameFor(ByteBuffer ticket, String logId) {
        final ByteString ticketId = idForTicket(ticket, logId);
        return String.format("%s/%s", FLIGHT_DESCRIPTOR_ROUTE, toHexString(ticketId));
    }

    private static @NotNull String toHexString(ByteString ticketId) {
        return ByteHelper.byteBufToHex(ticketId.asReadOnlyByteBuffer());
    }

    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, String.format(
                    "Could not resolve '%s': no session to handoff to", logId));
        }

        final ByteString sharedId = idForDescriptor(descriptor, logId);

        SessionState.ExportObject<?> export = sharedVariables.get(sharedId);
        if (export == null) {
            throw newNotFoundSRE(logId, toHexString(sharedId));
        }

        return session.<Flight.FlightInfo>nonExport()
                .require(export)
                .submit(() -> {
                    Object result = export.get();
                    if (result instanceof Table) {
                        result = authorization.transform(result);
                    }
                    if (result instanceof Table) {
                        return TicketRouter.getFlightInfo((Table) result, descriptor,
                                FlightExportTicketHelper.descriptorToFlightTicket(descriptor, logId));
                    }

                    throw newNotFoundSRE(logId, toHexString(sharedId));
                });
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        // shared tickets are otherwise private, so we don't need to do anything here
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteBuffer ticket, final String logId) {
        return resolve(session, idForTicket(ticket, logId), logId);
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        return resolve(session, idForDescriptor(descriptor, logId), logId);
    }

    private <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteString sharedId, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, String.format(
                    "Could not resolve '%s': no session to handoff to", logId));
        }

        // noinspection unchecked
        final SessionState.ExportObject<T> sharedVar = (SessionState.ExportObject<T>) sharedVariables.get(sharedId);
        if (sharedVar == null) {
            return SessionState.wrapAsFailedExport(newNotFoundSRE(logId, toHexString(sharedId)));
        }

        // we need to wrap this in a new export object to hand off to the new session and defer checking permissions
        return session.<T>nonExport()
                .require(sharedVar)
                .submit(() -> {
                    T result = sharedVar.get();
                    result = authorization.transform(result);
                    if (result == null) {
                        throw newNotFoundSRE(logId, toHexString(sharedId));
                    }
                    return result;
                });
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        return failDueToBadSource(logId, toHexString(idForTicket(ticket, logId)));
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId,
            @Nullable final Runnable onPublish) {
        return failDueToBadSource(logId, toHexString(idForDescriptor(descriptor, logId)));
    }

    @Override
    public <T> void publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish,
            final SessionState.ExportErrorHandler errorHandler,
            final SessionState.ExportObject<T> source) {
        if (source.isNonExport()) {
            failDueToBadSource(logId, toHexString(idForTicket(ticket, logId)));
            return;
        }
        final ByteString sharedId = idForTicket(ticket, logId);
        final SessionState.ExportObject<?> existing = sharedVariables.putIfAbsent(sharedId, source);
        if (existing != null) {
            final String ticketHex = toHexString(sharedId);
            errorHandler.onError(ExportNotification.State.FAILED, "",
                    Exceptions.statusRuntimeException(Code.ALREADY_EXISTS, String.format(
                            "Could not publish '%s' to shared ticket '%s' (hex): destination already exists",
                            logId, ticketHex)),
                    null);
        } else if (onPublish != null) {
            onPublish.run();
        }
    }

    private static <T> T failDueToBadSource(String logId, String ticketHex) {
        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, String.format(
                "Could not publish '%s' to shared ticket '%s' (hex): can only publish directly from a session"
                        + " export to a shared ticket",
                logId, ticketHex));
    }

    /**
     * Convenience method to convert from a shared identifier to Flight.Ticket
     *
     * @param identifier the shared identifier to convert
     * @return the flight ticket this descriptor represents
     */
    public static Flight.Ticket flightTicketForId(final byte[] identifier) {
        return Flight.Ticket.newBuilder()
                .setTicket(ByteString.copyFrom(identifier))
                .build();
    }

    /**
     * Convenience method to convert from a shared identifier to Ticket
     *
     * @param identifier the shared identifier to convert
     * @return the flight ticket this descriptor represents
     */
    public static io.deephaven.proto.backplane.grpc.Ticket ticketForId(final byte[] identifier) {
        return io.deephaven.proto.backplane.grpc.Ticket.newBuilder()
                .setTicket(ByteString.copyFrom(identifier))
                .build();
    }

    /**
     * Convenience method to convert from a shared identifier to Flight.FlightDescriptor
     *
     * @param identifier the shared identifier to convert
     * @return the flight descriptor this descriptor represents
     */
    public static Flight.FlightDescriptor descriptorForId(final byte[] identifier) {
        return Flight.FlightDescriptor.newBuilder()
                .setType(Flight.FlightDescriptor.DescriptorType.PATH)
                .addAllPath(SharedTicketHelper.idToPath(identifier))
                .build();
    }

    /**
     * Convenience method to convert from a Flight.Ticket (as ByteBuffer) to shared identifier
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the query scope name this ticket represents
     */
    private static ByteString idForTicket(final ByteBuffer ticket, final String logId) {
        if (ticket == null || ticket.remaining() == 0) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    String.format("Could not resolve '%s': no ticket supplied", logId));
        }
        if (ticket.remaining() < 2 || ticket.get(ticket.position()) != SharedTicketHelper.TICKET_PREFIX) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    String.format("Could not resolve '%s': found 0x%s (hex)", logId, ByteHelper.byteBufToHex(ticket)));
        }

        final int initialPosition = ticket.position();
        try {
            ticket.position(initialPosition + 1);
            final byte[] dst = new byte[ticket.remaining()];
            ticket.get(dst);
            return ByteStringAccess.wrap(dst);
        } finally {
            ticket.position(initialPosition);
        }
    }

    /**
     * Convenience method to convert from a Flight.FlightDescriptor to shared identifier
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the query scope name this descriptor represents
     */
    private static ByteString idForDescriptor(final Flight.FlightDescriptor descriptor, final String logId) {
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, String.format(
                    "Could not resolve descriptor '%s': only paths are supported", logId));
        }
        if (descriptor.getPathCount() != 2) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, String.format(
                    "Could not resolve descriptor '%s': unexpected path length (found: %s, expected: 2)",
                    logId, TicketRouterHelper.getLogNameFor(descriptor)));
        }
        if (!descriptor.getPath(0).equals(FLIGHT_DESCRIPTOR_ROUTE)) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, String.format(
                    "Could not resolve descriptor '%s': unexpected path (found: %s, expected: %s)",
                    logId, TicketRouterHelper.getLogNameFor(descriptor), FLIGHT_DESCRIPTOR_ROUTE));
        }
        return ByteString.fromHex(descriptor.getPath(1));
    }

    /**
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight descriptor that represents the ticket
     */
    public static Flight.FlightDescriptor ticketToDescriptor(final Flight.Ticket ticket, final String logId) {
        return descriptorForId(idForTicket(ticket.getTicket().asReadOnlyByteBuffer(), logId).toByteArray());
    }

    /**
     * Convenience method to convert from a Flight.Descriptor to a Flight.Ticket.
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight ticket that represents the descriptor
     */
    public static Flight.Ticket descriptorToTicket(final Flight.FlightDescriptor descriptor, final String logId) {
        return flightTicketForId(idForDescriptor(descriptor, logId).toByteArray());
    }

    private static @NotNull StatusRuntimeException newNotFoundSRE(String logId, String sharedId) {
        return Exceptions.statusRuntimeException(Code.NOT_FOUND, String.format(
                "Could not resolve '%s': ticket '%s' not found", logId, sharedId));
    }
}

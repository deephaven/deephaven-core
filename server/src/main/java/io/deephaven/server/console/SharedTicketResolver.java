//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console;

import com.google.common.collect.MapMaker;
import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.base.string.EncodingInfo;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.flight.util.FlightExportTicketHelper;
import io.deephaven.proto.flight.util.TicketRouterHelper;
import io.deephaven.proto.util.ByteHelper;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.proto.util.SharedTicketHelper;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketResolverBase;
import io.deephaven.server.session.TicketRouter;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.Map;
import java.util.function.Consumer;

import static io.deephaven.proto.util.SharedTicketHelper.FLIGHT_DESCRIPTOR_ROUTE;
import static io.deephaven.proto.util.SharedTicketHelper.TICKET_PREFIX;

@Singleton
public class SharedTicketResolver extends TicketResolverBase {

    final Map<String, SessionState.ExportObject<?>> sharedVariables = new MapMaker()
            .weakValues()
            .makeMap();

    @Inject
    public SharedTicketResolver(
            final AuthorizationProvider authProvider) {
        super(authProvider, (byte) TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
    }

    @Override
    public String getLogNameFor(ByteBuffer ticket, String logId) {
        return FLIGHT_DESCRIPTOR_ROUTE + "/" + idForTicket(ticket, logId);
    }

    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED,
                    "Could not resolve '" + logId + "': no session to handoff to");
        }

        final String sharedId = idForDescriptor(descriptor, logId);

        SessionState.ExportObject<?> export = sharedVariables.get(sharedId);
        if (export == null) {
            throw Exceptions.statusRuntimeException(Code.NOT_FOUND,
                    "Could not resolve '" + logId + ": no shared ticket exists with id '" + sharedId + "'");
        }

        return session.<Flight.FlightInfo>nonExport()
                .require(export)
                .submit(() -> {
                    if (export.get() instanceof Table) {
                        final Table table = (Table) authorization.transform(export.get());
                        return TicketRouter.getFlightInfo(table, descriptor,
                                FlightExportTicketHelper.descriptorToFlightTicket(descriptor, logId));
                    }

                    throw Exceptions.statusRuntimeException(Code.NOT_FOUND,
                            "Could not resolve '" + logId + "': flight '" + descriptor + " does not exist");
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
            @Nullable final SessionState session, final String sharedId, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED,
                    "Could not resolve '" + logId + "': no session to handoff to");
        }

        // noinspection unchecked
        final SessionState.ExportObject<T> sharedVar = (SessionState.ExportObject<T>) sharedVariables.get(sharedId);
        if (sharedVar == null) {
            return SessionState.wrapAsFailedExport(Exceptions.statusRuntimeException(Code.NOT_FOUND,
                    "Could not resolve '" + logId + "': no shared ticket exists with id '" + sharedId + "'"));
        }

        // we need to wrap this in a new export object to hand off to the new session and defer checking permissions
        return session.<T>nonExport()
                .require(sharedVar)
                .submit(() -> {
                    final T result = sharedVar.get();
                    return authorization.transform(result);
                });
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        return publish(session, idForTicket(ticket, logId), logId, onPublish);
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId,
            @Nullable final Runnable onPublish) {
        return publish(session, idForDescriptor(descriptor, logId), logId, onPublish);
    }

    private <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final String sharedId,
            final String logId,
            @Nullable final Runnable onPublish) {
        // We publish to the query scope after the client finishes publishing their result. We accomplish this by
        // directly depending on the result of this export builder.
        final SessionState.ExportBuilder<T> resultBuilder = session.nonExport();
        final SessionState.ExportObject<T> resultExport = resultBuilder.getExport();
        final SessionState.ExportBuilder<T> publishTask = session.nonExport();

        sharedVariables.put(sharedId, resultExport);

        if (onPublish != null) {
            publishTask
                    .requiresSerialQueue()
                    .require(resultExport)
                    .submit(onPublish);
        }

        return resultBuilder;
    }

    @Override
    public <T> void publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable Runnable onPublish,
            final SessionState.ExportErrorHandler errorHandler,
            final SessionState.ExportObject<T> source) {
        sharedVariables.put(idForTicket(ticket, logId), source);
        if (onPublish != null) {
            onPublish.run();
        }
    }

    /**
     * Convenience method to convert from a shared variable identifier to Flight.Ticket
     *
     * @param identifier the shared variable identifier to convert
     * @return the flight ticket this descriptor represents
     */
    public static Flight.Ticket flightTicketForId(final String identifier) {
        return Flight.Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(SharedTicketHelper.nameToBytes(identifier)))
                .build();
    }

    /**
     * Convenience method to convert from a shared variable identifier to Ticket
     *
     * @param identifier the shared variable identifier to convert
     * @return the flight ticket this descriptor represents
     */
    public static io.deephaven.proto.backplane.grpc.Ticket ticketForId(final String identifier) {
        return io.deephaven.proto.backplane.grpc.Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(SharedTicketHelper.nameToBytes(identifier)))
                .build();
    }

    /**
     * Convenience method to convert from a shared variable identifier to Flight.FlightDescriptor
     *
     * @param identifier the shared variable identifier to convert
     * @return the flight descriptor this descriptor represents
     */
    public static Flight.FlightDescriptor descriptorForId(final String identifier) {
        return Flight.FlightDescriptor.newBuilder()
                .setType(Flight.FlightDescriptor.DescriptorType.PATH)
                .addAllPath(SharedTicketHelper.nameToPath(identifier))
                .build();
    }

    /**
     * Convenience method to convert from a Flight.Ticket (as ByteBuffer) to shared variable identifier
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the query scope name this ticket represents
     */
    public static String idForTicket(final ByteBuffer ticket, final String logId) {
        if (ticket == null || ticket.remaining() == 0) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': no ticket supplied");
        }
        if (ticket.remaining() < 3 || ticket.get(ticket.position()) != SharedTicketHelper.TICKET_PREFIX
                || ticket.get(ticket.position() + 1) != '/') {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': found 0x" + ByteHelper.byteBufToHex(ticket) + "' (hex)");
        }

        final int initialLimit = ticket.limit();
        final int initialPosition = ticket.position();
        final CharsetDecoder decoder = EncodingInfo.UTF_8.getDecoder().reset();
        try {
            ticket.position(initialPosition + 2);
            return decoder.decode(ticket).toString();
        } catch (CharacterCodingException e) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': failed to decode: " + e.getMessage());
        } finally {
            ticket.position(initialPosition);
            ticket.limit(initialLimit);
        }
    }

    /**
     * Convenience method to convert from a Flight.FlightDescriptor to shared variable identifier
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the query scope name this descriptor represents
     */
    public static String idForDescriptor(final Flight.FlightDescriptor descriptor, final String logId) {
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': only paths are supported");
        }
        if (descriptor.getPathCount() != 2) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': unexpected path length (found: "
                            + TicketRouterHelper.getLogNameFor(descriptor) + ", expected: 2)");
        }

        return descriptor.getPath(1);
    }

    /**
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight descriptor that represents the ticket
     */
    public static Flight.FlightDescriptor ticketToDescriptor(final Flight.Ticket ticket, final String logId) {
        return descriptorForId(idForTicket(ticket.getTicket().asReadOnlyByteBuffer(), logId));
    }

    /**
     * Convenience method to convert from a Flight.Descriptor to a Flight.Ticket.
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight ticket that represents the descriptor
     */
    public static Flight.Ticket descriptorToTicket(final Flight.FlightDescriptor descriptor, final String logId) {
        return flightTicketForId(idForDescriptor(descriptor, logId));
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.console;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.base.string.EncodingInfo;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.util.ScriptSession;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.session.TicketResolverBase;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.grpc_api.util.TicketRouterHelper;
import io.deephaven.proto.backplane.grpc.Ticket;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

@Singleton
public class ScopeTicketResolver extends TicketResolverBase {
    private static final char TICKET_PREFIX = 's';
    private static final String FLIGHT_DESCRIPTOR_ROUTE = "scope";

    private final GlobalSessionProvider globalSessionProvider;

    @Inject
    public ScopeTicketResolver(final GlobalSessionProvider globalSessionProvider) {
        super((byte) TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
        this.globalSessionProvider = globalSessionProvider;
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        return FLIGHT_DESCRIPTOR_ROUTE + "/" + nameForTicket(ticket, logId);
    }

    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        // there is no mechanism to wait for a scope variable to resolve; require that the scope variable exists now
        final String scopeName = nameForDescriptor(descriptor, logId);

        final Flight.FlightInfo flightInfo = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> {
            final ScriptSession gss = globalSessionProvider.getGlobalSession();
            Object scopeVar = gss.getVariable(scopeName, null);
            if (scopeVar == null) {
                throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                        "Could not resolve '" + logId + ": no variable exists with name '" + scopeName + "'");
            }
            if (scopeVar instanceof Table) {
                return TicketRouter.getFlightInfo((Table) scopeVar, descriptor, flightTicketForName(scopeName));
            }

            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                    "Could not resolve '" + logId + "': no variable exists with name '" + scopeName + "'");
        });

        return SessionState.wrapAsExport(flightInfo);
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        globalSessionProvider.getGlobalSession().getVariables().forEach((varName, varObj) -> {
            if (varObj instanceof Table) {
                visitor.accept(TicketRouter.getFlightInfo((Table) varObj, descriptorForName(varName),
                        flightTicketForName(varName)));
            }
        });
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteBuffer ticket, final String logId) {
        return resolve(session, nameForTicket(ticket, logId), logId);
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        return resolve(session, nameForDescriptor(descriptor, logId), logId);
    }

    private <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final String scopeName, final String logId) {
        // if we are not attached to a session, check the scope for a variable right now
        final T export = LiveTableMonitor.DEFAULT.sharedLock().computeLocked(() -> {
            final ScriptSession gss = globalSessionProvider.getGlobalSession();
            // noinspection unchecked
            T scopeVar = (T) gss.unwrapObject(gss.getVariable(scopeName));
            if (scopeVar == null) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                        "Could not resolve '" + logId + "': no variable exists with name '" + scopeName + "'");
            }
            return scopeVar;
        });

        if (export == null) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': no variable exists with name '" + scopeName + "'");
        }
        return SessionState.wrapAsExport(export);
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session, final ByteBuffer ticket, final String logId) {
        return publish(session, nameForTicket(ticket, logId), logId);
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        return publish(session, nameForDescriptor(descriptor, logId), logId);
    }

    private <T> SessionState.ExportBuilder<T> publish(
            final SessionState session, final String varName, final String logId) {
        // We publish to the query scope after the client finishes publishing their result. We accomplish this by
        // directly depending on the result of this export builder.
        final SessionState.ExportBuilder<T> resultBuilder = session.nonExport();
        final SessionState.ExportObject<T> resultExport = resultBuilder.getExport();
        final SessionState.ExportBuilder<T> publishTask = session.nonExport();

        publishTask
                .requiresSerialQueue()
                .require(resultExport)
                .submit(() -> {
                    final ScriptSession gss = globalSessionProvider.getGlobalSession();
                    gss.setVariable(varName, resultExport.get());
                });

        return resultBuilder;
    }

    /**
     * Convenience method to convert from a scoped variable name to Flight.Ticket
     *
     * @param name the scoped variable name to convert
     * @return the flight ticket this descriptor represents
     */
    public static Flight.Ticket flightTicketForName(final String name) {
        final byte[] ticket = (TICKET_PREFIX + "/" + name).getBytes(StandardCharsets.UTF_8);
        return Flight.Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(ticket))
                .build();
    }

    /**
     * Convenience method to convert from a scoped variable name to Ticket
     *
     * @param name the scoped variable name to convert
     * @return the flight ticket this descriptor represents
     */
    public static Ticket ticketForName(final String name) {
        final byte[] ticket = (TICKET_PREFIX + "/" + name).getBytes(StandardCharsets.UTF_8);
        return Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(ticket))
                .build();
    }

    /**
     * Convenience method to convert from a scoped variable name to Flight.FlightDescriptor
     *
     * @param name the scoped variable name to convert
     * @return the flight descriptor this descriptor represents
     */
    public static Flight.FlightDescriptor descriptorForName(final String name) {
        return Flight.FlightDescriptor.newBuilder()
                .setType(Flight.FlightDescriptor.DescriptorType.PATH)
                .addPath(FLIGHT_DESCRIPTOR_ROUTE)
                .addPath(name)
                .build();
    }

    /**
     * Convenience method to convert from a Flight.Ticket (as ByteBuffer) to scope variable name
     *
     * @param ticket the ticket to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the query scope name this ticket represents
     */
    public static String nameForTicket(final ByteBuffer ticket, final String logId) {
        if (ticket == null || ticket.remaining() == 0) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': no ticket supplied");
        }
        if (ticket.remaining() < 3 || ticket.get(ticket.position()) != TICKET_PREFIX
                || ticket.get(ticket.position() + 1) != '/') {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': found 0x" + byteBufToHex(ticket) + "' (hex)");
        }

        final int initialLimit = ticket.limit();
        final int initialPosition = ticket.position();
        final CharsetDecoder decoder = EncodingInfo.UTF_8.getDecoder().reset();
        try {
            ticket.position(initialPosition + 2);
            return decoder.decode(ticket).toString();
        } catch (CharacterCodingException e) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': failed to decode: " + e.getMessage());
        } finally {
            ticket.position(initialPosition);
            ticket.limit(initialLimit);
        }
    }

    /**
     * Convenience method to convert from a Flight.FlightDescriptor to scoped Variable Name
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return the query scope name this descriptor represents
     */
    public static String nameForDescriptor(final Flight.FlightDescriptor descriptor, final String logId) {
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': only paths are supported");
        }
        if (descriptor.getPathCount() != 2) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
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
        return descriptorForName(nameForTicket(ticket.getTicket().asReadOnlyByteBuffer(), logId));
    }

    /**
     * Convenience method to convert from a Flight.Descriptor to a Flight.Ticket.
     *
     * @param descriptor the descriptor to convert
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a flight ticket that represents the descriptor
     */
    public static Flight.Ticket descriptorToTicket(final Flight.FlightDescriptor descriptor, final String logId) {
        return flightTicketForName(nameForDescriptor(descriptor, logId));
    }
}

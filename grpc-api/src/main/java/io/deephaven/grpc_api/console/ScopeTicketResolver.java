/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.console;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.base.string.EncodingInfo;
import io.deephaven.db.tables.Table;
import io.deephaven.db.util.ScriptSession;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.session.TicketResolverBase;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.grpc_api.util.TicketRouterHelper;
import org.apache.arrow.flight.impl.Flight;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

public class ScopeTicketResolver extends TicketResolverBase {
    private static final char TICKET_PREFIX = 's';
    private static final String FLIGHT_DESCRIPTOR_ROUTE = "scope";

    private final GlobalSessionProvider globalSessionProvider;

    @Inject
    public ScopeTicketResolver(final GlobalSessionProvider globalSessionProvider) {
        super((byte)TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
        this.globalSessionProvider = globalSessionProvider;
    }

    @Override
    public String getLogNameFor(ByteBuffer ticket) {
        return FLIGHT_DESCRIPTOR_ROUTE + "/" + nameForTicket(ticket);
    }

    @Override
    public Flight.FlightInfo flightInfoFor(final Flight.FlightDescriptor descriptor) {
        final String varName = nameForDescriptor(descriptor);
        final Object varObj = globalSessionProvider.getGlobalSession().getVariable(varName);
        if (varObj instanceof Table) {
            return TicketRouter.getFlightInfo((Table) varObj, descriptor, ticketForName(varName));
        } else {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Could not compute flight info: variable '" + varName + "' is not a flight");
        }
    }

    @Override
    public void forAllFlightInfo(final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        globalSessionProvider.getGlobalSession().getVariables().forEach((varName, varObj) -> {
            if (varObj instanceof Table) {
                visitor.accept(TicketRouter.getFlightInfo((Table) varObj, descriptorForName(varName), ticketForName(varName)));
            }
        });
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(final SessionState session, final ByteBuffer ticket) {
        return resolve(session, nameForTicket(ticket));
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(final SessionState session, final Flight.FlightDescriptor descriptor) {
        return resolve(session, nameForDescriptor(descriptor));
    }

    private <T> SessionState.ExportObject<T> resolve(final SessionState session, final String scopeName) {
        return session.<T>nonExport()
                .requiresSerialQueue()
                .submit(() -> {
                    final ScriptSession gss = globalSessionProvider.getGlobalSession();
                    //noinspection unchecked
                    T scopeVar = (T) gss.getVariable(scopeName);
                    if (scopeVar == null) {
                        throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Could not resolve: no variable exists with name '" + scopeVar + "'");
                    }
                    return scopeVar;
                });
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(final SessionState session, final ByteBuffer ticket) {
        return publish(session, nameForTicket(ticket));
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(final SessionState session, final Flight.FlightDescriptor descriptor) {
        return publish(session, nameForDescriptor(descriptor));
    }

    private <T> SessionState.ExportBuilder<T> publish(final SessionState session, final String varName) {
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
     * Convenience method to convert from a scoped variable name to Flight.ticket
     *
     * @param name the scoped variable name to convert
     * @return the flight ticket this descriptor represents
     */
    public static Flight.Ticket ticketForName(final String name) {
        final byte[] ticket = (TICKET_PREFIX + '/' + name).getBytes(StandardCharsets.UTF_8);
        return Flight.Ticket.newBuilder()
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
     * @return the query scope name this ticket represents
     */
    public static String nameForTicket(final ByteBuffer ticket) {
        if (ticket == null) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Ticket not supplied");
        }
        if (ticket.remaining() < 3 || ticket.get(0) != TICKET_PREFIX || ticket.get(1) != '/') {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Cannot parse ticket: found 0x" + byteBufToHex(ticket) + "' (hex)");
        }

        final int initialLimit = ticket.limit();
        final int initialPosition = ticket.position();
        final CharsetDecoder decoder = EncodingInfo.UTF_8.getDecoder().reset();
        try {
            ticket.position(initialPosition + 2);
            return decoder.decode(ticket).toString();
        } catch (CharacterCodingException e) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Cannot parse ticket: failed to decode: " + e.getMessage());
        } finally {
            ticket.position(initialPosition);
            ticket.limit(initialLimit);
        }
    }

    /**
     * Convenience method to convert from a Flight.FlightDescriptor to scoped Variable Name
     *
     * @param descriptor the descriptor to convert
     * @return the query scope name this descriptor represents
     */
    public static String nameForDescriptor(final Flight.FlightDescriptor descriptor) {
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Cannot parse descriptor: not a path");
        }
        if (descriptor.getPathCount() != 2) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Cannot parse descriptor: unexpected path length (found: " + TicketRouterHelper.getLogNameFor(descriptor) + ", expected: 2)");
        }

        return descriptor.getPath(1);
    }

    /**
     * Convenience method to convert from a Flight.Ticket to a Flight.FlightDescriptor.
     *
     * @param ticket the ticket to convert
     * @return a flight descriptor that represents the ticket
     */
    public static Flight.FlightDescriptor ticketToDescriptor(final Flight.Ticket ticket) {
        return descriptorForName(nameForTicket(ticket.getTicket().asReadOnlyByteBuffer()));
    }

    /**
     * Convenience method to convert from a Flight.Descriptor to a Flight.Ticket.
     *
     * @param descriptor the descriptor to convert
     * @return a flight ticket that represents the descriptor
     */
    public static Flight.Ticket descriptorToTicket(final Flight.FlightDescriptor descriptor) {
        return ticketForName(nameForDescriptor(descriptor));
    }
}

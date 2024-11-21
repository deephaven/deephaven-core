//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console;

import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.base.string.EncodingInfo;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.Table;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.flight.util.TicketRouterHelper;
import io.deephaven.proto.util.ByteHelper;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.proto.util.ScopeTicketHelper;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketResolverBase;
import io.deephaven.server.session.TicketRouter;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.function.Consumer;

import static io.deephaven.proto.util.ScopeTicketHelper.FLIGHT_DESCRIPTOR_ROUTE;
import static io.deephaven.proto.util.ScopeTicketHelper.TICKET_PREFIX;

@Singleton
public class ScopeTicketResolver extends TicketResolverBase {

    @Inject
    public ScopeTicketResolver(
            final AuthorizationProvider authProvider) {
        super(authProvider, (byte) TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
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

        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        final Object scopeVar = queryScope.unwrapObject(queryScope.readParamValue(scopeName, null));
        if (scopeVar == null) {
            throw newNotFoundSRE(logId, scopeName);
        }
        if (!(scopeVar instanceof Table)) {
            throw newNotFoundSRE(logId, scopeName);
        }

        final Table transformed = authorization.transform((Table) scopeVar);
        if (transformed == null) {
            throw newNotFoundSRE(logId, scopeName);
        }
        final Flight.FlightInfo flightInfo =
                TicketRouter.getFlightInfo(transformed, descriptor, flightTicketForName(scopeName));

        return SessionState.wrapAsExport(flightInfo);
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        if (session == null) {
            return;
        }
        final QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
        queryScope.toMap(queryScope::unwrapObject, (n, t) -> t instanceof Table).forEach((name, table) -> {
            final Table transformedTable = authorization.transform((Table) table);
            if (transformedTable != null) {
                visitor.accept(TicketRouter.getFlightInfo(
                        transformedTable, descriptorForName(name), flightTicketForName(name)));
            }
        });
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteBuffer ticket, final String logId) {
        return resolve(nameForTicket(ticket, logId), logId);
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        return resolve(nameForDescriptor(descriptor, logId), logId);
    }

    private <T> SessionState.ExportObject<T> resolve(final String scopeName, final String logId) {
        // fetch the variable from the scope right now
        T export = null;
        try {
            QueryScope queryScope = ExecutionContext.getContext().getQueryScope();
            // noinspection unchecked
            export = (T) queryScope.unwrapObject(queryScope.readParamValue(scopeName));
        } catch (QueryScope.MissingVariableException ignored) {
        }

        export = authorization.transform(export);

        if (export == null) {
            return SessionState.wrapAsFailedExport(newNotFoundSRE(logId, scopeName));
        }

        return SessionState.wrapAsExport(export);
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        return publish(session, nameForTicket(ticket, logId), logId, onPublish);
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId,
            @Nullable final Runnable onPublish) {
        return publish(session, nameForDescriptor(descriptor, logId), logId, onPublish);
    }

    private <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final String varName,
            final String logId,
            @Nullable final Runnable onPublish) {
        // We publish to the query scope after the client finishes publishing their result. We accomplish this by
        // directly depending on the result of this export builder.
        final SessionState.ExportBuilder<T> resultBuilder = session.nonExport();
        final SessionState.ExportObject<T> resultExport = resultBuilder.getExport();
        final SessionState.ExportBuilder<T> publishTask = session.nonExport();

        publishTask
                .requiresSerialQueue()
                .require(resultExport)
                .submit(() -> {
                    T value = resultExport.get();
                    ExecutionContext.getContext().getQueryScope().putParam(varName, value);

                    if (onPublish != null) {
                        onPublish.run();
                    }
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
        return Flight.Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(ScopeTicketHelper.nameToBytes(name)))
                .build();
    }

    /**
     * Convenience method to convert from a scoped variable name to Ticket
     *
     * @param name the scoped variable name to convert
     * @return the flight ticket this descriptor represents
     */
    public static Ticket ticketForName(final String name) {
        return Ticket.newBuilder()
                .setTicket(ByteStringAccess.wrap(ScopeTicketHelper.nameToBytes(name)))
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
                .addAllPath(ScopeTicketHelper.nameToPath(name))
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
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': no ticket supplied");
        }
        if (ticket.remaining() < 3 || ticket.get(ticket.position()) != TICKET_PREFIX
                || ticket.get(ticket.position() + 1) != '/') {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': found 0x" + ByteHelper.byteBufToHex(ticket) + "' (hex)");
        }

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
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': only paths are supported");
        }
        if (descriptor.getPathCount() != 2) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': unexpected path length (found: "
                            + TicketRouterHelper.getLogNameFor(descriptor) + ", expected: 2)");
        }
        if (!descriptor.getPath(0).equals(FLIGHT_DESCRIPTOR_ROUTE)) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve descriptor '" + logId + "': unexpected path (found: "
                            + TicketRouterHelper.getLogNameFor(descriptor) + ", expected: " + FLIGHT_DESCRIPTOR_ROUTE
                            + ")");
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

    private static @NotNull StatusRuntimeException newNotFoundSRE(String logId, String scopeName) {
        return Exceptions.statusRuntimeException(Code.NOT_FOUND,
                "Could not resolve '" + logId + ": variable '" + scopeName + "' not found");
    }
}

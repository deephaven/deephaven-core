/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.session;

import com.google.rpc.Code;
import io.deephaven.engine.table.Table;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.hash.KeyedIntObjectHashMap;
import io.deephaven.hash.KeyedIntObjectKey;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.auth.AuthorizationProvider;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.Consumer;

@Singleton
public class TicketRouter {
    private final KeyedIntObjectHashMap<TicketResolver> byteResolverMap =
            new KeyedIntObjectHashMap<>(RESOLVER_OBJECT_TICKET_ID);
    private final KeyedObjectHashMap<String, TicketResolver> descriptorResolverMap =
            new KeyedObjectHashMap<>(RESOLVER_OBJECT_DESCRIPTOR_ID);

    private final TicketResolver.Authorization authorization;

    @Inject
    public TicketRouter(
            final AuthorizationProvider authorizationProvider,
            final Set<TicketResolver> resolvers) {
        this.authorization = authorizationProvider.getTicketResolverAuthorization();
        resolvers.forEach(resolver -> {
            if (!byteResolverMap.add(resolver)) {
                throw new IllegalArgumentException("Duplicate ticket resolver for ticket route "
                        + resolver.ticketRoute());
            }
            if (!descriptorResolverMap.add(resolver)) {
                throw new IllegalArgumentException("Duplicate ticket resolver for descriptor route "
                        + resolver.flightDescriptorRoute());
            }
        });
    }

    /**
     * Resolve a flight ticket (as ByteBuffer) to an export object future.
     *
     * @param session the user session context
     * @param ticket the ticket to resolve
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param <T> the expected return type of the ticket; this is not validated
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session,
            final ByteBuffer ticket,
            final String logId) {
        if (ticket.remaining() == 0) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "could not resolve '" + logId + "' it's an empty ticket");
        }
        return getResolver(ticket.get(ticket.position()), logId).resolve(session, ticket, logId);
    }

    /**
     * Resolve a flight ticket to an export object future.
     *
     * @param session the user session context
     * @param ticket the ticket to resolve
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param <T> the expected return type of the ticket; this is not validated
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session,
            final Flight.Ticket ticket,
            final String logId) {
        return resolve(session, ticket.getTicket().asReadOnlyByteBuffer(), logId);
    }

    /**
     * Resolve a flight ticket to an export object future.
     *
     * @param session the user session context
     * @param ticket the ticket to resolve
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param <T> the expected return type of the ticket; this is not validated
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session,
            final Ticket ticket,
            final String logId) {
        return resolve(session, ticket.getTicket().asReadOnlyByteBuffer(), logId);
    }

    /**
     * Resolve a flight descriptor to an export object future.
     *
     * @param session the user session context
     * @param descriptor the descriptor to resolve
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param <T> the expected return type of the ticket; this is not validated
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId) {
        return getResolver(descriptor, logId).resolve(session, descriptor, logId);
    }

    /**
     * Publish a new result as a flight ticket to an export object future.
     *
     * <p>
     * The user must call {@link SessionState.ExportBuilder#submit} to publish the result value.
     *
     * @param session the user session context
     * @param ticket (as ByteByffer) the ticket to publish to
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param onPublish an optional callback to invoke when the result is published
     * @param <T> the type of the result the export will publish
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        final TicketResolver resolver = getResolver(ticket.get(ticket.position()), logId);
        authorization.authorizePublishRequest(resolver, ticket);
        return resolver.publish(session, ticket, logId, onPublish);
    }

    /**
     * Publish a new result as a flight ticket to an export object future.
     *
     * <p>
     * The user must call {@link SessionState.ExportBuilder#submit} to publish the result value.
     *
     * @param session the user session context
     * @param ticket (as Flight.Ticket) the ticket to publish to
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param onPublish an optional callback to invoke when the result is published
     * @param <T> the type of the result the export will publish
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.Ticket ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        // note this impl is an internal delegation; defer the authorization check, too
        return publish(session, ticket.getTicket().asReadOnlyByteBuffer(), logId, onPublish);
    }

    /**
     * Publish a new result as a flight ticket to an export object future.
     *
     * <p>
     * The user must call {@link SessionState.ExportBuilder#submit} to publish the result value.
     *
     * @param session the user session context
     * @param ticket the ticket to publish to
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param onPublish an optional callback to invoke when the result is published
     * @param <T> the type of the result the export will publish
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Ticket ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        // note this impl is an internal delegation; defer the authorization check, too
        return publish(session, ticket.getTicket().asReadOnlyByteBuffer(), logId, onPublish);
    }

    /**
     * Publish a new result as a flight descriptor to an export object future.
     *
     * <p>
     * The user must call {@link SessionState.ExportBuilder#submit} to publish the result value.
     *
     * @param session the user session context
     * @param descriptor (as Flight.Descriptor) the descriptor to publish to
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param onPublish an optional callback to invoke when the result is published
     * @param <T> the type of the result the export will publish
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId,
            @Nullable final Runnable onPublish) {
        final TicketResolver resolver = getResolver(descriptor, logId);
        authorization.authorizePublishRequest(resolver, descriptor);
        return resolver.publish(session, descriptor, logId, onPublish);
    }

    /**
     * Resolve a flight descriptor and retrieve flight info for the flight.
     *
     * @param session the user session context; ticket resolvers may expose flights that do not require a session (such
     *        as via DoGet)
     * @param descriptor the flight descriptor
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return an export object that will resolve to the flight descriptor; see {@link SessionState} for lifecycle
     *         propagation details
     */
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(
            @Nullable final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId) {
        return getResolver(descriptor, logId).flightInfoFor(session, descriptor, logId);
    }

    /**
     * Create a human readable string to identify this ticket.
     *
     * @param ticket the ticket to parse
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a string that is good for log/error messages
     */
    public String getLogNameFor(final Ticket ticket, final String logId) {
        return getLogNameFor(ticket.getTicket().asReadOnlyByteBuffer(), logId);
    }

    /**
     * Create a human readable string to identify this Flight.Ticket.
     *
     * @param ticket the ticket to parse
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a string that is good for log/error messages
     */
    public String getLogNameFor(final Flight.Ticket ticket, final String logId) {
        return getLogNameFor(ticket.getTicket().asReadOnlyByteBuffer(), logId);
    }

    /**
     * Create a human readable string to identify this ticket.
     *
     * @param ticket the ticket to parse
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a string that is good for log/error messages
     */
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        return getResolver(ticket.get(ticket.position()), logId).getLogNameFor(ticket, logId);
    }

    /**
     * This invokes the provided visitor for each valid flight descriptor this ticket resolver exposes via flight.
     *
     * @param session optional session that the resolver can use to filter which flights a visitor sees
     * @param visitor the callback to invoke per descriptor path
     */
    public void visitFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        byteResolverMap.iterator().forEachRemaining(resolver -> resolver.forAllFlightInfo(session, visitor));
    }

    public static Flight.FlightInfo getFlightInfo(final Table table,
            final Flight.FlightDescriptor descriptor,
            final Flight.Ticket ticket) {
        return Flight.FlightInfo.newBuilder()
                .setSchema(BarrageUtil.schemaBytesFromTable(table))
                .setFlightDescriptor(descriptor)
                .addEndpoint(Flight.FlightEndpoint.newBuilder()
                        .setTicket(ticket)
                        .build())
                .setTotalRecords(table.isRefreshing() ? -1 : table.size())
                .setTotalBytes(-1)
                .build();
    }

    private TicketResolver getResolver(final byte route, final String logId) {
        final TicketResolver resolver = byteResolverMap.get(route);
        if (resolver == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': no resolver for route '" + route + "' (byte)");
        }
        return resolver;
    }

    private TicketResolver getResolver(final Flight.FlightDescriptor descriptor, final String logId) {
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': flight descriptor is not a path");
        }
        if (descriptor.getPathCount() <= 0) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': flight descriptor does not have route path");
        }

        final String route = descriptor.getPath(0);
        final TicketResolver resolver = descriptorResolverMap.get(route);
        if (resolver == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': no resolver for route '" + route + "'");
        }

        return resolver;
    }

    private static final KeyedIntObjectKey<TicketResolver> RESOLVER_OBJECT_TICKET_ID =
            new KeyedIntObjectKey.BasicStrict<TicketResolver>() {
                @Override
                public int getIntKey(final TicketResolver ticketResolver) {
                    return ticketResolver.ticketRoute();
                }
            };

    private static final KeyedObjectKey<String, TicketResolver> RESOLVER_OBJECT_DESCRIPTOR_ID =
            new KeyedObjectKey.Basic<String, TicketResolver>() {
                @Override
                public String getKey(TicketResolver ticketResolver) {
                    return ticketResolver.flightDescriptorRoute();
                }
            };
}

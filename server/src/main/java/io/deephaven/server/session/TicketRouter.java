//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import com.google.rpc.Code;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.hash.KeyedIntObjectHashMap;
import io.deephaven.hash.KeyedIntObjectKey;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.util.SafeCloseable;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor.DescriptorType;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Singleton
public class TicketRouter {
    private final KeyedIntObjectHashMap<TicketResolver> byteResolverMap =
            new KeyedIntObjectHashMap<>(RESOLVER_OBJECT_TICKET_ID);
    private final KeyedObjectHashMap<String, TicketResolver> descriptorResolverMap =
            new KeyedObjectHashMap<>(RESOLVER_OBJECT_DESCRIPTOR_ID);

    private final TicketResolver.Authorization authorization;
    private final Set<CommandResolver> commandResolvers;

    @Inject
    public TicketRouter(
            final AuthorizationProvider authorizationProvider,
            final Set<TicketResolver> resolvers) {
        this.authorization = authorizationProvider.getTicketResolverAuthorization();
        this.commandResolvers = resolvers.stream()
                .filter(CommandResolver.class::isInstance)
                .map(CommandResolver.class::cast)
                .collect(Collectors.toSet());
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
        final String ticketName = getLogNameFor(ticket, logId);
        try (final SafeCloseable ignored = QueryPerformanceRecorder.getInstance().getNugget(
                "resolveTicket:" + ticketName)) {
            return getResolver(ticket.get(ticket.position()), logId).resolve(session, ticket, logId);
        } catch (RuntimeException e) {
            throw e;
            // return SessionState.wrapAsFailedExport(e);
        }
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
        try (final SafeCloseable ignored = QueryPerformanceRecorder.getInstance().getNugget(
                "resolveDescriptor:" + descriptor)) {
            return getResolver(descriptor, logId).resolve(session, descriptor, logId);
        } catch (RuntimeException e) {
            throw e;
            // return SessionState.wrapAsFailedExport(e);
        }
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
        final String ticketName = getLogNameFor(ticket, logId);
        try (final SafeCloseable ignored = QueryPerformanceRecorder.getInstance().getNugget(
                "publishTicket:" + ticketName)) {
            final TicketResolver resolver = getResolver(ticket.get(ticket.position()), logId);
            authorization.authorizePublishRequest(resolver, ticket);
            return resolver.publish(session, ticket, logId, onPublish);
        }
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
        try (final SafeCloseable ignored = QueryPerformanceRecorder.getInstance().getNugget(
                "publishDescriptor:" + descriptor)) {
            final TicketResolver resolver = getResolver(descriptor, logId);
            authorization.authorizePublishRequest(resolver, descriptor);
            return resolver.publish(session, descriptor, logId, onPublish);
        }
    }

    /**
     * Publish a new result as a flight ticket as to-be defined by the supplied source.
     *
     * @param session the user session context
     * @param ticket the ticket to publish to
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param onPublish an optional callback to invoke when the result is accessible to callers
     * @param errorHandler an error handler to invoke if the source fails to produce a result
     * @param source the source object to publish
     * @param <T> the type of the result the export will publish
     */
    public <T> void publish(
            final SessionState session,
            final Ticket ticket,
            final String logId,
            @Nullable final Runnable onPublish,
            final SessionState.ExportErrorHandler errorHandler,
            final SessionState.ExportObject<T> source) {
        final ByteBuffer ticketBuffer = ticket.getTicket().asReadOnlyByteBuffer();
        final TicketResolver resolver = getResolver(ticketBuffer.get(ticketBuffer.position()), logId);
        authorization.authorizePublishRequest(resolver, ticketBuffer);
        resolver.publish(session, ticketBuffer, logId, onPublish, errorHandler, source);
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
        try (final SafeCloseable ignored = QueryPerformanceRecorder.getInstance().getNugget(
                "flightInfoForDescriptor:" + descriptor)) {
            return getResolver(descriptor, logId).flightInfoFor(session, descriptor, logId);
        } catch (RuntimeException e) {
            throw e;
            // return SessionState.wrapAsFailedExport(e);
        }
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
        if (descriptor.getType() == Flight.FlightDescriptor.DescriptorType.PATH) {
            return getPathResolver(descriptor, logId);
        }
        if (descriptor.getType() == DescriptorType.CMD) {
            return getCommandResolver(descriptor, logId);
        }
        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                "Could not resolve '" + logId + "': unexpected type");
    }

    private TicketResolver getPathResolver(FlightDescriptor descriptor, String logId) {
        if (descriptor.getType() != DescriptorType.PATH) {
            throw new IllegalStateException("descriptor is not a path");
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

    private CommandResolver getCommandResolver(FlightDescriptor descriptor, String logId) {
        if (descriptor.getType() != DescriptorType.CMD) {
            throw new IllegalStateException("descriptor is not a command");
        }
        // This is the most "naive" resolution logic; it scales linearly with the number of command resolvers, but it is
        // the most general and may be the best we can do for certain types of command protocols built on top of Flight.
        // If we find the number of command resolvers scaling up, we could devise a more efficient strategy in some
        // cases either based on a prefix model and/or a fixed set model (which could be communicated either through new
        // method(s) on CommandResolver, or through subclasses).
        //
        // Regardless, even with a moderate amount of command resolvers, the linear nature of this should not be a
        // bottleneck.
        CommandResolver commandResolver = null;
        for (CommandResolver resolver : commandResolvers) {
            if (!resolver.handlesCommand(descriptor)) {
                continue;
            }
            if (commandResolver != null) {
                // Is there any good way to give a friendly string for unknown command bytes? Probably not.
                throw Exceptions.statusRuntimeException(Code.INTERNAL,
                        "Could not resolve '" + logId + "': multiple resolvers for command");
            }
            commandResolver = resolver;
        }
        if (commandResolver == null) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Could not resolve '" + logId + "': no resolver for command");
        }
        return commandResolver;
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

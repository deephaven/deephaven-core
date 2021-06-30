/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.session;

import com.google.rpc.Code;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.hash.KeyedIntObjectHashMap;
import io.deephaven.hash.KeyedIntObjectKey;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.function.Consumer;

@Singleton
public class TicketRouter {

    private final KeyedIntObjectHashMap<TicketResolver> byteResolverMap = new KeyedIntObjectHashMap<>(RESOLVER_OBJECT_TICKET_ID);
    private final KeyedObjectHashMap<String, TicketResolver> descriptorResolverMap = new KeyedObjectHashMap<>(RESOLVER_OBJECT_DESCRIPTOR_ID);

    @Inject
    public TicketRouter(final Set<TicketResolver> resolvers) {
        resolvers.forEach(resolver -> {
            byteResolverMap.add(resolver);
            descriptorResolverMap.add(resolver);
        });
    }

    /**
     * Resolve a flight ticket (as ByteBuffer) to an export object future.
     *
     * @param session the user session context
     * @param ticket the ticket to resolve
     * @param <T> the expected return type of the ticket; this is not validated
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportObject<T> resolve(
            final SessionState session,
            final ByteBuffer ticket) {
        return getResolver(ticket.get(0)).resolve(session, ticket);
    }

    /**
     * Resolve a flight ticket to an export object future.
     *
     * @param session the user session context
     * @param ticket the ticket to resolve
     * @param <T> the expected return type of the ticket; this is not validated
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportObject<T> resolve(
            final SessionState session,
            final Flight.Ticket ticket) {
        return resolve(session, ticket.getTicket().asReadOnlyByteBuffer());
    }

    /**
     * Resolve a flight descriptor to an export object future.
     *
     * @param session the user session context
     * @param descriptor the descriptor to resolve
     * @param <T> the expected return type of the ticket; this is not validated
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportObject<T> resolve(
            final SessionState session,
            final Flight.FlightDescriptor descriptor) {
        return getResolver(descriptor).resolve(session, descriptor);
    }

    /**
     * Publish a new result as a flight ticket to an export object future.
     *
     * The user must call {@link SessionState.ExportBuilder#submit} to publish the result value.
     *
     * @param session the user session context
     * @param ticket (as ByteByffer) the ticket to publish to
     * @param <T> the type of the result the export will publish
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final ByteBuffer ticket) {
        return getResolver(ticket.get(0)).publish(session, ticket);
    }

    /**
     * Publish a new result as a flight ticket to an export object future.
     *
     * The user must call {@link SessionState.ExportBuilder#submit} to publish the result value.
     *
     * @param session the user session context
     * @param ticket (as Flight.Ticket) the ticket to publish to
     * @param <T> the type of the result the export will publish
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.Ticket ticket) {
        return publish(session, ticket.getTicket().asReadOnlyByteBuffer());
    }

    /**
     * Publish a new result as a flight descriptor to an export object future.
     *
     * The user must call {@link SessionState.ExportBuilder#submit} to publish the result value.
     *
     * @param session the user session context
     * @param descriptor (as Flight.Descriptor) the descriptor to publish to
     * @param <T> the type of the result the export will publish
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.FlightDescriptor descriptor) {
        return getResolver(descriptor).publish(session, descriptor);
    }

    /**
     * Resolve a flight descriptor and retrieve flight info for the flight.
     *
     * @param descriptor the flight descriptor
     * @return flight info for the particular descriptor
     */
    public Flight.FlightInfo flightInfoFor(final Flight.FlightDescriptor descriptor) {
        return getResolver(descriptor).flightInfoFor(descriptor);
    }

    /**
     * Create a human readable string to identify this ticket.
     *
     * @param ticket the ticket to parse
     * @return a string that is good for log/error messages
     */
    public String getLogNameFor(final Flight.Ticket ticket) {
        return getLogNameFor(ticket.getTicket().asReadOnlyByteBuffer());
    }

    /**
     * Create a human readable string to identify this ticket.
     *
     * @param ticket the ticket to parse
     * @return a string that is good for log/error messages
     */
    public String getLogNameFor(final ByteBuffer ticket) {
        return getResolver(ticket.get(0)).getLogNameFor(ticket);
    }

    /**
     * This invokes the provided visitor for each valid flight descriptor this ticket resolver exposes via flight.
     *
     * @param session optional session that the resolver can use to filter which flights a visitor sees
     * @param visitor the callback to invoke per descriptor path
     */
    public void visitFlightInfo(@Nullable SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        byteResolverMap.iterator().forEachRemaining(resolver ->
                resolver.forAllFlightInfo(session, visitor)
        );
    }

    private TicketResolver getResolver(final byte route) {
        final TicketResolver resolver = byteResolverMap.get(route);
        if (resolver == null) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Cannot find resolver for route '" + route + "' (byte)");
        }
        return resolver;
    }

    private TicketResolver getResolver(final Flight.FlightDescriptor descriptor) {
        if (descriptor.getType() != Flight.FlightDescriptor.DescriptorType.PATH) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Cannot find resolver: flight descriptor is not a path");
        }
        if (descriptor.getPathCount() <= 0) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Cannot find resolver: flight descriptor does not have route path");
        }

        final String route = descriptor.getPath(0);
        final TicketResolver resolver = descriptorResolverMap.get(route);
        if (resolver == null) {
            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Cannot find resolver for route '" + route + "'");
        }

        return resolver;
    }

    private static final KeyedIntObjectKey<TicketResolver> RESOLVER_OBJECT_TICKET_ID = new KeyedIntObjectKey.BasicStrict<TicketResolver>() {
        @Override
        public int getIntKey(final TicketResolver ticketResolver) {
            return ticketResolver.ticketRoute();
        }
    };

    private static final KeyedObjectKey<String, TicketResolver> RESOLVER_OBJECT_DESCRIPTOR_ID = new KeyedObjectKey.Basic<String, TicketResolver>() {
        @Override
        public String getKey(TicketResolver ticketResolver) {
            return ticketResolver.flightDescriptorRoute();
        }
    };
}

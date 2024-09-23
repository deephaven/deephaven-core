//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.Action;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.flight.impl.Flight.Result;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public interface TicketResolver {
    interface Authorization {
        /**
         * Implementations must type check the provided source as any type of object can be stored in an export.
         * <p>
         *
         * @apiNote Types typically encountered are {@link Table} and {@link PartitionedTable}. Perform an identity
         *          mapping for any types that you do not wish to transform. This method should not error.
         *          Implementations may wish to query {@link ExecutionContext#getAuthContext()} to apply user-specific
         *          transformations to requested resources.
         *
         * @param source the object to transform (such as by applying ACLs)
         * @return an object that has been sanitized to be used by the current user; may return null if user does not
         *         have access to the resource
         */
        <T> T transform(T source);

        /**
         * Implementations must validate that the provided ticket is authorized for the current user.
         * <p>
         *
         * @apiNote Implementations may wish to query {@link ExecutionContext#getAuthContext()} to apply user-specific
         *          transformations to requested resources.
         *
         * @param ticketResolver the ticket resolver
         * @param ticket the ticket to publish to as a byte buffer; note that the first byte is the route
         * @throws io.grpc.StatusRuntimeException if the user is not authorized
         */
        void authorizePublishRequest(TicketResolver ticketResolver, ByteBuffer ticket);

        /**
         * Implementations must validate that the provided ticket is authorized for the current user.
         * <p>
         *
         * @apiNote Implementations may wish to query {@link ExecutionContext#getAuthContext()} to apply user-specific
         *          transformations to requested resources.
         *
         * @param ticketResolver the ticket resolver
         * @param descriptor the flight descriptor to publish to; note that the first path element is the route
         * @throws io.grpc.StatusRuntimeException if the user is not authorized
         */
        void authorizePublishRequest(TicketResolver ticketResolver, Flight.FlightDescriptor descriptor);
    }

    /**
     * @return the single byte prefix used as a route on the ticket
     */
    byte ticketRoute();

    /**
     * The first path entry on a route indicates which resolver to use. The remaining path elements are used to resolve
     * the descriptor.
     *
     * @return the string that will route from flight descriptor to this resolver
     */
    String flightDescriptorRoute();

    /**
     * Resolve a flight ticket to an export object future.
     *
     * @param session the user session context
     * @param ticket (as ByteByffer) the ticket to resolve
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param <T> the expected return type of the ticket; this is not validated
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    <T> SessionState.ExportObject<T> resolve(@Nullable SessionState session, ByteBuffer ticket, String logId);

    /**
     * Resolve a flight descriptor to an export object future.
     *
     * @param session the user session context
     * @param descriptor the descriptor to resolve
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param <T> the expected return type of the ticket; this is not validated
     * @return an export object; see {@link SessionState} for lifecycle propagation details
     */
    <T> SessionState.ExportObject<T> resolve(@Nullable SessionState session, Flight.FlightDescriptor descriptor,
            String logId);

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
    <T> SessionState.ExportBuilder<T> publish(
            SessionState session, ByteBuffer ticket, String logId, @Nullable Runnable onPublish);

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
    <T> SessionState.ExportBuilder<T> publish(
            SessionState session, Flight.FlightDescriptor descriptor, String logId, @Nullable Runnable onPublish);

    /**
     * Publish the result of the source object as the result represented by the destination ticket.
     *
     * @param session the user session context
     * @param ticket the ticket to publish to
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param onPublish an optional callback to invoke when the result is published
     * @param errorHandler the error handler to invoke if the source object fails to export
     * @param source the source object to export
     * @param <T> the type of the result the export will publish
     */
    default <T> void publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish,
            final SessionState.ExportErrorHandler errorHandler,
            final SessionState.ExportObject<T> source) {
        publish(session, ticket, logId, onPublish)
                .onError(errorHandler)
                .require(source)
                .submit(source::get);
    }

    /**
     * Retrieve a FlightInfo for a given FlightDescriptor.
     *
     * @param descriptor the flight descriptor to retrieve a ticket for
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a FlightInfo describing this flight
     */
    SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(@Nullable SessionState session,
            Flight.FlightDescriptor descriptor, String logId);

    /**
     * Create a human readable string to identify this ticket.
     *
     * @param ticket the ticket to parse
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a string that is good for log/error messages
     * @apiNote There is not a {@link Flight.FlightDescriptor} equivalent as the path must already be displayable.
     */
    String getLogNameFor(ByteBuffer ticket, String logId);

    /**
     * This invokes the provided visitor for each valid flight descriptor this ticket resolver exposes via flight.
     *
     * @param session optional session that the resolver can use to filter which flights a visitor sees
     * @param visitor the callback to invoke per descriptor path
     */
    void forAllFlightInfo(@Nullable SessionState session, Consumer<Flight.FlightInfo> visitor);

    default boolean supportsCommand(FlightDescriptor descriptor) {
        // Unfortunately, there is no universal way to know whether a command belongs to a given Flight protocol or not;
        // at best, we can assume (or mandate) that all of the supportable command bytes are sufficiently unique such
        // that there is no potential for overlap amongst the installed Flight protocols and it's a "non-issue".
        //
        // For example there could be command protocols built on top of Flight that simply use integer ordinals as their
        // command serialization format. In such a case, only one such protocol could safely be installed; otherwise,
        // there would be no reliable way of differentiating between them from the command bytes. (It's possible that
        // other means of differentiating could be established, like header values.)
        //
        // If we are ever in a position to create a protocol that uses Flight commands, or advise on their creation, it
        // would probably be wise to use a command serialization format that has a "unique" magic value as its prefix.
        //
        // The FlightSQL approach is to use the protobuf message Any to wrap up the respective protobuf FlightSQL
        // command message. While this approach is very likely to produce a sufficiently unique selection criteria, it
        // requires non-trivial parsing to determine whether the command is supported or not.
        return false;
    }
}

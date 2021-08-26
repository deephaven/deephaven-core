package org.apache.arrow.flight;

import io.grpc.ManagedChannel;
import org.apache.arrow.flight.FlightClientMiddleware.Factory;
import org.apache.arrow.flight.FlightGrpcUtils.NonClosingProxyManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import java.util.List;

/**
 * Extension that exposes middleware for construction of {@link FlightClient}.
 *
 * @see FlightGrpcUtils
 */
public class FlightGrpcUtilsExtension {

    /**
     * Creates a Flight client.
     * 
     * @param incomingAllocator Memory allocator
     * @param channel provides a connection to a gRPC server.
     * @param middleware the middleware
     */
    public static FlightClient createFlightClient(BufferAllocator incomingAllocator,
            ManagedChannel channel, List<Factory> middleware) {
        return new FlightClient(incomingAllocator, channel, middleware);
    }

    /**
     * Creates a Flight client.
     * 
     * @param incomingAllocator Memory allocator
     * @param channel provides a connection to a gRPC server. Will not be closed on closure of the returned
     *        FlightClient.
     * @param middleware the middleware
     */
    public static FlightClient createFlightClientWithSharedChannel(
            BufferAllocator incomingAllocator, ManagedChannel channel, List<Factory> middleware) {
        return new FlightClient(incomingAllocator, new NonClosingProxyManagedChannel(channel),
                middleware);
    }
}
